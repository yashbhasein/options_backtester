"""
kotak_live_collector.py
------------------------
Track C: collect LIVE market data (options + futures) via Kotak Neo.

Two modes:

1. SNAPSHOT mode (default, for EOD data collection):
   Uses REST quotes API to get a point-in-time OHLC+LTP snapshot of all
   active option contracts. Run once at market close (15:30 IST).
   Writes to kotak_live/ partitioned parquet in the canonical schema.

2. WEBSOCKET mode (--stream):
   Opens a persistent WebSocket subscription for real-time tick-by-tick
   updates. Keeps a running in-memory OHLCV state (building 1-min candles
   from ticks) and writes completed candles to parquet on the fly.
   Suitable for building your own intraday OHLCV archive from today onward.

IMPORTANT:
  Kotak Neo WebSocket allows max 30 symbols per subscribe() call.
  For a full option chain (100+ strikes × 2 sides), you'll need to batch
  subscriptions. This script handles that automatically.

  Instrument tokens for F&O come from the Kotak scrip master CSV.
  The script auto-downloads the nse_fo scrip master on first run and
  caches it locally.

Usage:
    # EOD snapshot (run after 15:30 IST)
    python kotak_live_collector.py --underlyings NIFTY,BANKNIFTY

    # Real-time streaming
    python kotak_live_collector.py --underlyings NIFTY --stream

    # Snapshot for a specific expiry only
    python kotak_live_collector.py --underlyings NIFTY --expiry 2026-07-03
"""
import argparse
import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from threading import Event, Thread

import pandas as pd

from confignew import (
    KOTAK_LIVE_DIR, LOG_DIR, CANONICAL_COLUMNS, KOTAK_WS_MAX_SYMBOLS,
)
from kotak_auth import get_kotak_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "kotak_live_collector.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

SCRIP_MASTER_CACHE = Path("./kotak_scrip_master_nse_fo.csv")


# ─────────────────────────────────────────────
# Scrip master helpers
# ─────────────────────────────────────────────

def load_scrip_master(client) -> pd.DataFrame:
    """
    Downloads and caches the NSE F&O scrip master CSV from Kotak Neo.
    Returns a DataFrame indexed for fast symbol lookups.
    """
    if SCRIP_MASTER_CACHE.exists():
        log.info(f"Loading cached scrip master from {SCRIP_MASTER_CACHE}")
        return pd.read_csv(SCRIP_MASTER_CACHE)

    log.info("Downloading NSE F&O scrip master from Kotak Neo...")
    resp = client.scrip_master(exchange_segment="nse_fo")
    # resp is a CSV string or file path depending on SDK version
    if isinstance(resp, str):
        with open(SCRIP_MASTER_CACHE, "w") as f:
            f.write(resp)
        df = pd.read_csv(SCRIP_MASTER_CACHE)
    else:
        # Some SDK versions save to disk and return a path
        import shutil
        shutil.copy(str(resp), str(SCRIP_MASTER_CACHE))
        df = pd.read_csv(SCRIP_MASTER_CACHE)
    log.info(f"Scrip master: {len(df)} rows")
    return df


def find_option_tokens(scrip_df: pd.DataFrame,
                       underlying: str,
                       expiry: str = None,
                       max_strikes_from_atm: int = 20) -> list:
    """
    Returns list of dicts: {instrument_token, exchange_segment, strike,
                             option_type, expiry, trading_symbol}
    for all active option contracts of the given underlying.

    Filters CE + PE only, optionally for a specific expiry.
    Column names are standardised from the Kotak scrip master format.
    """
    # Kotak scrip master columns (nse_fo segment):
    # pSymbol, pGroup, pExpiryDate, pOptionType, pStrikePrice, pTrdSymbol,
    # pScripRefKey (instrument_token), etc.
    # The exact column names vary slightly between SDK versions -- we
    # detect them defensively.
    df = scrip_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Find the column that holds instrument tokens
    token_col = next(
        (c for c in df.columns if "token" in c or "refkey" in c.lower()), None
    )
    symbol_col = next(
        (c for c in df.columns if "symbol" in c and "trd" in c.lower()), None
    ) or next((c for c in df.columns if "symbol" in c), None)
    expiry_col = next(
        (c for c in df.columns if "expiry" in c or "expdt" in c), None
    )
    opt_col = next(
        (c for c in df.columns if "optiontype" in c.replace(" ", "").lower()
         or c == "poptiontype" or "optype" in c), None
    )
    strike_col = next(
        (c for c in df.columns if "strike" in c), None
    )
    name_col = next(
        (c for c in df.columns if "psymbol" == c or "name" in c), None
    )

    if not all([token_col, expiry_col, opt_col, strike_col]):
        log.error(f"Could not identify required scrip master columns. "
                  f"Columns found: {list(df.columns)}")
        return []

    # Filter to CE/PE for this underlying
    mask = (
        df[opt_col].isin(["CE", "PE"]) &
        df.apply(lambda r: underlying in str(r.get(name_col or symbol_col, "")),
                 axis=1)
    )
    df = df[mask].copy()

    if expiry:
        df = df[df[expiry_col].astype(str).str.contains(expiry.replace("-", ""))]

    tokens = []
    for _, row in df.iterrows():
        tokens.append({
            "instrument_token": str(row[token_col]),
            "exchange_segment": "nse_fo",
            "strike": float(row[strike_col]),
            "option_type": str(row[opt_col]),
            "expiry": str(row[expiry_col]),
            "trading_symbol": str(row.get(symbol_col, "")),
        })
    return tokens


def write_rows_to_parquet(rows: list, underlying: str):
    if not rows:
        return
    df = pd.DataFrame(rows).reindex(columns=CANONICAL_COLUMNS)
    df["date"] = pd.to_datetime(df["date"])
    year = df["date"].dt.year.iloc[0]
    out_dir = KOTAK_LIVE_DIR / underlying
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{underlying}_{year}.parquet"
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["timestamp", "underlying", "expiry", "strike", "option_type"],
            keep="last",
        )
    else:
        combined = df
    combined.to_parquet(out_path, index=False)
    log.info(f"Wrote {len(df)} rows -> {out_path}")


# ─────────────────────────────────────────────
# Snapshot mode
# ─────────────────────────────────────────────

def run_snapshot(client, underlyings: list, expiry: str = None):
    """REST quote snapshot — run once at/after 15:30 IST."""
    scrip_df = load_scrip_master(client)
    today = date.today().isoformat()

    for underlying in underlyings:
        all_tokens = find_option_tokens(scrip_df, underlying, expiry)
        if not all_tokens:
            log.warning(f"{underlying}: no active contracts found in scrip master")
            continue
        log.info(f"{underlying}: {len(all_tokens)} contracts to snapshot")

        rows = []
        # Batch into groups of 30 (Kotak limit per quotes call)
        for i in range(0, len(all_tokens), KOTAK_WS_MAX_SYMBOLS):
            batch = all_tokens[i:i + KOTAK_WS_MAX_SYMBOLS]
            instrument_tokens = [
                {"instrument_token": t["instrument_token"],
                 "exchange_segment": "nse_fo"}
                for t in batch
            ]
            try:
                resp = client.quotes(
                    instrument_tokens=instrument_tokens,
                    quote_type="ohlc"
                )
                # resp is a list of quote dicts or a JSON string
                quotes = resp if isinstance(resp, list) else (
                    json.loads(resp) if isinstance(resp, str) else []
                )
            except Exception as e:
                log.warning(f"{underlying} batch {i//KOTAK_WS_MAX_SYMBOLS}: {e}")
                continue

            # Build a token→meta lookup for this batch
            meta = {t["instrument_token"]: t for t in batch}
            for q in (quotes if isinstance(quotes, list) else [quotes]):
                token = str(q.get("instrument_token") or q.get("tk", ""))
                m = meta.get(token, {})
                rows.append({
                    "date": today,
                    "timestamp": datetime.combine(
                        date.fromisoformat(today),
                        datetime.strptime("15:30:00", "%H:%M:%S").time()
                    ),
                    "underlying": underlying,
                    "expiry": m.get("expiry", ""),
                    "strike": m.get("strike", 0.0),
                    "option_type": m.get("option_type", ""),
                    "exercise_style": "european",
                    "open": float(q.get("open", 0) or 0),
                    "high": float(q.get("high", 0) or 0),
                    "low": float(q.get("low", 0) or 0),
                    "close": float(q.get("ltp", q.get("close", 0)) or 0),
                    "volume": int(q.get("volume", 0) or 0),
                    "oi": int(q.get("oi", 0) or 0),
                    "settle_price": None,
                    "source": "kotak_live",
                    "granularity": "1d",
                })
            time.sleep(0.1)

        write_rows_to_parquet(rows, underlying)


# ─────────────────────────────────────────────
# WebSocket streaming mode
# ─────────────────────────────────────────────

class CandleBuilder:
    """Builds 1-min OHLCV candles from individual tick messages."""

    def __init__(self):
        self.candles = defaultdict(dict)  # key → current open candle

    def on_tick(self, token: str, ltp: float, volume: int,
                oi: int, ts: datetime) -> dict | None:
        """
        Process a tick. Returns a completed candle dict when the minute
        rolls over, otherwise None.
        """
        minute_key = ts.replace(second=0, microsecond=0)
        key = (token, minute_key)
        prev_minute = self.candles.get((token, None))

        # Find the previous minute's candle if this is a new minute
        prev_keys = [k for k in self.candles if k[0] == token and k[1] != minute_key]
        completed = None
        for pk in prev_keys:
            completed = self.candles.pop(pk)

        if key not in self.candles:
            self.candles[key] = {
                "ts": minute_key, "open": ltp, "high": ltp,
                "low": ltp, "close": ltp, "volume": 0, "oi": oi,
            }
        else:
            c = self.candles[key]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp
            c["volume"] += volume
            c["oi"] = oi

        return completed


def run_stream(client, underlyings: list, expiry: str = None):
    """WebSocket streaming mode — builds 1-min candles from ticks."""
    scrip_df = load_scrip_master(client)
    stop_event = Event()
    candle_builder = CandleBuilder()
    pending_rows = defaultdict(list)  # underlying → list of completed candle rows

    # Build token→metadata lookup
    token_meta = {}
    all_batches = []
    for underlying in underlyings:
        tokens = find_option_tokens(scrip_df, underlying, expiry)
        for t in tokens:
            token_meta[t["instrument_token"]] = {**t, "underlying": underlying}
        # Build batches of 30
        for i in range(0, len(tokens), KOTAK_WS_MAX_SYMBOLS):
            all_batches.append((underlying, tokens[i:i + KOTAK_WS_MAX_SYMBOLS]))

    log.info(f"Total contracts to stream: {len(token_meta)} across "
             f"{len(all_batches)} batches")

    def on_message(msg):
        try:
            data = json.loads(msg) if isinstance(msg, str) else msg
            if not isinstance(data, list):
                data = [data]
            for tick in data:
                token = str(tick.get("tk") or tick.get("instrument_token", ""))
                ltp = float(tick.get("lp") or tick.get("ltp") or 0)
                volume = int(tick.get("v") or tick.get("volume") or 0)
                oi = int(tick.get("oi") or 0)
                ts = datetime.now()
                meta = token_meta.get(token)
                if not meta or not ltp:
                    return
                completed = candle_builder.on_tick(token, ltp, volume, oi, ts)
                if completed:
                    underlying = meta["underlying"]
                    pending_rows[underlying].append({
                        "date": completed["ts"].date().isoformat(),
                        "timestamp": completed["ts"],
                        "underlying": underlying,
                        "expiry": meta["expiry"],
                        "strike": meta["strike"],
                        "option_type": meta["option_type"],
                        "exercise_style": "european",
                        "open": completed["open"],
                        "high": completed["high"],
                        "low": completed["low"],
                        "close": completed["close"],
                        "volume": completed["volume"],
                        "oi": completed["oi"],
                        "settle_price": None,
                        "source": "kotak_live",
                        "granularity": "1min",
                    })
                    # Flush every 500 completed candles per underlying
                    if len(pending_rows[underlying]) >= 500:
                        write_rows_to_parquet(pending_rows[underlying], underlying)
                        pending_rows[underlying] = []
        except Exception as e:
            log.debug(f"Tick parse error: {e}")

    def on_error(msg):
        log.error(f"WebSocket error: {msg}")

    def on_close(msg):
        log.info(f"WebSocket closed: {msg}")
        stop_event.set()

    def on_open(msg):
        log.info("WebSocket connected")
        # Subscribe in batches of 30
        for underlying, batch in all_batches:
            inst_tokens = [
                {"instrument_token": t["instrument_token"],
                 "exchange_segment": "nse_fo"}
                for t in batch
            ]
            client.subscribe(instrument_tokens=inst_tokens,
                             isIndex=False, isDepth=False)
            time.sleep(0.1)
        log.info(f"Subscribed to {len(token_meta)} contracts")

    client.on_message = on_message
    client.on_error = on_error
    client.on_close = on_close
    client.on_open = on_open

    log.info("Starting WebSocket stream. Press Ctrl+C to stop and flush remaining candles.")
    try:
        # Subscribe to first batch to open the connection
        first_batch = all_batches[0][1] if all_batches else []
        client.subscribe(
            instrument_tokens=[
                {"instrument_token": t["instrument_token"],
                 "exchange_segment": "nse_fo"}
                for t in first_batch
            ],
            isIndex=False, isDepth=False,
        )
        stop_event.wait()  # block until WebSocket closes
    except KeyboardInterrupt:
        log.info("Interrupted by user, flushing remaining candles...")
    finally:
        for underlying, rows in pending_rows.items():
            if rows:
                write_rows_to_parquet(rows, underlying)
        try:
            for underlying in underlyings:
                tokens = find_option_tokens(scrip_df, underlying, expiry)
                client.un_subscribe(
                    instrument_tokens=[
                        {"instrument_token": t["instrument_token"],
                         "exchange_segment": "nse_fo"}
                        for t in tokens
                    ]
                )
        except Exception:
            pass


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Collect live options data from Kotak Neo"
    )
    ap.add_argument("--underlyings", required=True,
                     help="e.g. NIFTY,BANKNIFTY")
    ap.add_argument("--expiry", default=None,
                     help="Filter to specific expiry YYYY-MM-DD")
    ap.add_argument("--stream", action="store_true",
                     help="WebSocket streaming mode (default: EOD snapshot)")
    args = ap.parse_args()

    client = get_kotak_client()
    underlyings = args.underlyings.split(",")

    if args.stream:
        run_stream(client, underlyings, args.expiry)
    else:
        run_snapshot(client, underlyings, args.expiry)
