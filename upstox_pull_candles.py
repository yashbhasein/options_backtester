"""
upstox_pull_candles.py
-----------------------
Phase 2 of the Upstox historical backfill.

Reads the state DB built by upstox_enumerate_contracts.py and pulls
1-min OHLCV candles from Upstox's Expired Historical Candle API for
every contract still marked 'pending'.

RESUMABLE BY DESIGN:
  Each contract is marked 'done' in the DB immediately after a successful
  pull and write. If the script crashes or is interrupted, re-running it
  resumes from the next pending contract -- no already-fetched contract
  is pulled twice.

OUTPUT:
  Parquet files partitioned by underlying/year, same schema as the NSE
  bhavcopy pipeline. Both track A (NSE daily) and track B (Upstox intraday)
  write to different directories but identical schemas, so the backtester
  can union them trivially.

RATE LIMITING:
  Upstox hasn't published a hard limit for the historical data endpoint,
  but community observations suggest ~10 req/sec is safe. We default to
  ~6-7 req/sec (0.15s sleep) for headroom. If you see 429 responses,
  increase UPSTOX_SLEEP_BETWEEN_CALLS in config.py.

Usage:
    # Pull everything pending
    python upstox_pull_candles.py --underlyings NIFTY,BANKNIFTY

    # Pull only a specific expiry (useful for spot-checks)
    python upstox_pull_candles.py --underlyings NIFTY --expiry 2024-01-25

    # Retry contracts that previously errored
    python upstox_pull_candles.py --underlyings NIFTY --retry-errors
"""
import argparse
import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import upstox_client

from confignew import (
    UPSTOX_INTRADAY_DIR, STATE_DB_PATH, LOG_DIR,
    CANONICAL_COLUMNS, UPSTOX_SLEEP_BETWEEN_CALLS,
)
from upstox_auth import get_upstox_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "upstox_pull_candles.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def fetch_candles(api: upstox_client.ExpiredInstrumentApi,
                  expired_instrument_key: str,
                  expiry_date: str,
                  interval: str = "1minute") -> list:
    """
    Fetches 1-min candles for a single expired contract.
    from_date = UPSTOX_INTRADAY_START (Jan 2022), to_date = expiry_date.
    Returns list of raw candle rows: [timestamp, o, h, l, c, volume, oi].
    """
    try:
        resp = api.get_expired_historical_candle_data(
            expired_instrument_key=expired_instrument_key,
            interval=interval,
            to_date=expiry_date,
            from_date="2022-01-01",
        )
        if resp and resp.data and resp.data.candles:
            return resp.data.candles
        return []
    except Exception as e:
        raise RuntimeError(f"API call failed: {e}") from e


def candles_to_df(candles: list, underlying: str, expiry: str,
                  strike: float, option_type: str) -> pd.DataFrame:
    """Convert raw candle list to canonical schema DataFrame."""
    rows = []
    for c in candles:
        # Upstox candle format: [timestamp_str, open, high, low, close, volume, oi]
        ts = pd.to_datetime(c[0])
        rows.append({
            "date": ts.date().isoformat(),
            "timestamp": ts,
            "underlying": underlying,
            "expiry": expiry,
            "strike": float(strike),
            "option_type": option_type,
            "exercise_style": "european",  # NSE index options: always European
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": int(c[5]) if c[5] is not None else 0,
            "oi": int(c[6]) if len(c) > 6 and c[6] is not None else None,
            "settle_price": None,
            "source": "upstox_expired",
            "granularity": "1min",
        })
    df = pd.DataFrame(rows)
    return df.reindex(columns=CANONICAL_COLUMNS)


def write_to_parquet(df: pd.DataFrame):
    """Append rows to the appropriate underlying/year parquet files."""
    df["_year"] = pd.to_datetime(df["date"]).dt.year

    for (underlying, year), group in df.groupby(["underlying", "_year"]):
        out_dir = UPSTOX_INTRADAY_DIR / underlying
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{underlying}_{year}.parquet"
        group = group.drop(columns=["_year"])

        if out_path.exists():
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["timestamp", "underlying", "expiry", "strike", "option_type"],
                keep="last",
            )
        else:
            combined = group

        combined.to_parquet(out_path, index=False)


def run(underlyings: list, expiry_filter: str = None,
        retry_errors: bool = False, interval: str = "1minute"):
    conn = sqlite3.connect(STATE_DB_PATH)
    api_client = get_upstox_client()
    expired_api = upstox_client.ExpiredInstrumentApi(api_client)

    status_filter = "pending"
    if retry_errors:
        status_filter = "error"

    placeholders = ",".join("?" for _ in underlyings)
    query = f"""
        SELECT id, underlying, expiry, strike, option_type, expired_instrument_key
        FROM contracts
        WHERE underlying IN ({placeholders})
          AND status = ?
    """
    params = underlyings + [status_filter]

    if expiry_filter:
        query += " AND expiry = ?"
        params.append(expiry_filter)

    query += " ORDER BY underlying, expiry, strike, option_type"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    total = len(rows)
    log.info(f"{total} contracts to pull (status={status_filter})")
    if total == 0:
        log.info("Nothing to do. Run upstox_enumerate_contracts.py first if "
                 "this is unexpected.")
        return

    done = 0
    errors = 0
    empty = 0

    for i, (row_id, underlying, expiry, strike, opt_type, exp_key) in enumerate(rows):
        try:
            candles = fetch_candles(expired_api, exp_key, expiry, interval)
            time.sleep(UPSTOX_SLEEP_BETWEEN_CALLS)

            if not candles:
                # Contract existed but no data (illiquid / never traded)
                conn = sqlite3.connect(STATE_DB_PATH)
                conn.execute(
                    "UPDATE contracts SET status='empty', rows_fetched=0, "
                    "updated_at=CURRENT_TIMESTAMP WHERE id=?", (row_id,)
                )
                conn.commit()
                conn.close()
                empty += 1
                continue

            df = candles_to_df(candles, underlying, expiry, strike, opt_type)
            write_to_parquet(df)

            conn = sqlite3.connect(STATE_DB_PATH)
            conn.execute(
                "UPDATE contracts SET status='done', rows_fetched=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (len(df), row_id)
            )
            conn.commit()
            conn.close()
            done += 1

        except Exception as e:
            log.error(f"[{i+1}/{total}] {underlying} {expiry} {strike} "
                      f"{opt_type} ERROR: {e}")
            conn = sqlite3.connect(STATE_DB_PATH)
            conn.execute(
                "UPDATE contracts SET status='error', last_error=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (str(e)[:500], row_id)
            )
            conn.commit()
            conn.close()
            errors += 1
            time.sleep(1.0)  # back off on error
            continue

        if (i + 1) % 100 == 0 or (i + 1) == total:
            pct = (i + 1) / total * 100
            remaining = (total - i - 1) * UPSTOX_SLEEP_BETWEEN_CALLS / 3600
            log.info(f"Progress: {i+1}/{total} ({pct:.1f}%) — "
                     f"done={done} empty={empty} errors={errors} — "
                     f"~{remaining:.1f}h remaining")

    log.info(f"Finished. done={done}, empty={empty}, errors={errors}")
    if errors > 0:
        log.info("Re-run with --retry-errors to retry failed contracts.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Pull 1-min candles for Upstox expired option contracts"
    )
    ap.add_argument("--underlyings", required=True,
                     help="e.g. NIFTY,BANKNIFTY")
    ap.add_argument("--expiry", default=None,
                     help="Pull only this expiry (YYYY-MM-DD), useful for testing")
    ap.add_argument("--retry-errors", action="store_true",
                     help="Retry contracts previously marked 'error'")
    ap.add_argument("--interval", default="1minute",
                     choices=["1minute", "3minute", "5minute", "15minute",
                               "30minute", "day"],
                     help="Candle interval (default: 1minute)")
    args = ap.parse_args()

    run(
        underlyings=args.underlyings.split(","),
        expiry_filter=args.expiry,
        retry_errors=args.retry_errors,
        interval=args.interval,
    )
