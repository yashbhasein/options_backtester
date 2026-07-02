"""
upstox_enumerate_contracts.py
------------------------------
Phase 1 of the Upstox historical backfill.

Queries Upstox's Get Expiries + Get Expired Option Contracts APIs to build
a complete list of every (underlying, expiry, strike, CE/PE) contract that
existed from Jan 2022 onward, and writes them into a SQLite state DB.

This step MUST run before upstox_pull_candles.py.
It's fast (~minutes), doesn't pull any OHLC data, and is completely safe
to re-run -- it upserts rather than duplicating.

The state DB also tracks which contracts have been fully pulled in Phase 2,
enabling crash-safe resumption.

Usage:
    python upstox_enumerate_contracts.py --underlyings NIFTY,BANKNIFTY
    python upstox_enumerate_contracts.py --underlyings NIFTY --from-date 2023-01-01
"""
import argparse
import logging
import sqlite3
import time
from datetime import date

import upstox_client

from confignew import (
    STATE_DB_PATH, UPSTOX_UNDERLYING_MAP, UPSTOX_INTRADAY_START,
    LOG_DIR, UPSTOX_SLEEP_BETWEEN_CALLS,
)
from upstox_auth import get_upstox_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "upstox_enumerate.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            underlying            TEXT NOT NULL,
            expiry                TEXT NOT NULL,
            strike                REAL NOT NULL,
            option_type           TEXT NOT NULL,
            expired_instrument_key TEXT NOT NULL UNIQUE,
            status                TEXT NOT NULL DEFAULT 'pending',
            -- 'pending' | 'done' | 'empty' (returned no data) | 'error'
            last_error            TEXT,
            rows_fetched          INTEGER DEFAULT 0,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_status ON contracts(status)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_underlying_expiry
            ON contracts(underlying, expiry)
    """)
    conn.commit()


def get_expiries(api: upstox_client.ExpiredInstrumentApi,
                 instrument_key: str) -> list:
    """Returns list of expiry date strings for a given underlying."""
    try:
        resp = api.get_expiries(instrument_key)
        return resp.data if resp and resp.data else []
    except Exception as e:
        log.warning(f"get_expiries failed for {instrument_key}: {e}")
        return []


def get_contracts_for_expiry(api: upstox_client.ExpiredInstrumentApi,
                              instrument_key: str,
                              expiry_date: str) -> list:
    """
    Returns list of expired option contract objects for one (underlying, expiry).
    Each has: expired_instrument_key, strike_price, instrument_type (CE/PE),
              trading_symbol, etc.
    """
    try:
        resp = api.get_expired_option_contracts(
            instrument_key=instrument_key,
            expiry_date=expiry_date,
        )
        return resp.data if resp and resp.data else []
    except Exception as e:
        log.warning(f"get_expired_option_contracts failed for "
                    f"{instrument_key} expiry={expiry_date}: {e}")
        return []


def run(underlyings: list, from_date: date):
    conn = sqlite3.connect(STATE_DB_PATH)
    init_db(conn)

    api_client = get_upstox_client()
    expired_api = upstox_client.ExpiredInstrumentApi(api_client)

    total_inserted = 0
    total_skipped = 0

    for name in underlyings:
        instrument_key = UPSTOX_UNDERLYING_MAP.get(name)
        if not instrument_key:
            log.warning(f"No Upstox instrument key for '{name}'. "
                        f"Add it to UPSTOX_UNDERLYING_MAP in config.py.")
            continue

        log.info(f"{name}: fetching expiries...")
        expiries = get_expiries(expired_api, instrument_key)
        time.sleep(UPSTOX_SLEEP_BETWEEN_CALLS)

        # Filter to from_date and later only
        expiries_in_range = [
            e for e in expiries
            if date.fromisoformat(str(e)) >= from_date
        ]
        log.info(f"{name}: {len(expiries_in_range)} expiries >= {from_date} "
                 f"(out of {len(expiries)} total)")

        for expiry in expiries_in_range:
            expiry_str = str(expiry)
            contracts = get_contracts_for_expiry(
                expired_api, instrument_key, expiry_str
            )
            time.sleep(UPSTOX_SLEEP_BETWEEN_CALLS)

            inserted = 0
            skipped = 0
            for c in contracts:
                opt_type = getattr(c, "instrument_type", None)
                if opt_type not in ("CE", "PE"):
                    continue  # skip futures rows if any sneak through
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO contracts
                            (underlying, expiry, strike, option_type,
                             expired_instrument_key)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        name,
                        expiry_str,
                        float(getattr(c, "strike_price", 0)),
                        opt_type,
                        str(c.expired_instrument_key),
                    ))
                    if conn.total_changes > 0:
                        inserted += 1
                    else:
                        skipped += 1
                except sqlite3.IntegrityError:
                    skipped += 1

            conn.commit()
            total_inserted += inserted
            total_skipped += skipped
            log.info(f"  {name} {expiry_str}: "
                     f"{len(contracts)} contracts, "
                     f"{inserted} new, {skipped} already exist")

    conn.close()
    log.info(f"Done. {total_inserted} contracts inserted, "
             f"{total_skipped} already existed.")
    log.info(f"State DB: {STATE_DB_PATH}")

    # Print a summary of pending work
    conn = sqlite3.connect(STATE_DB_PATH)
    pending = conn.execute(
        "SELECT COUNT(*) FROM contracts WHERE status='pending'"
    ).fetchone()[0]
    done = conn.execute(
        "SELECT COUNT(*) FROM contracts WHERE status='done'"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    conn.close()
    log.info(f"State DB summary: {total} total contracts, "
             f"{pending} pending pull, {done} already done.")
    if pending > 0:
        hrs = pending * UPSTOX_SLEEP_BETWEEN_CALLS / 3600
        log.info(f"Estimated pull time at current rate limit: ~{hrs:.1f} hours. "
                 f"Run upstox_pull_candles.py to start pulling.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Enumerate Upstox expired option contracts into state DB"
    )
    ap.add_argument("--underlyings", required=True,
                     help="e.g. NIFTY,BANKNIFTY")
    ap.add_argument("--from-date", default=str(UPSTOX_INTRADAY_START),
                     help=f"Earliest expiry to enumerate "
                          f"(default: {UPSTOX_INTRADAY_START})")
    args = ap.parse_args()
    run(
        underlyings=args.underlyings.split(","),
        from_date=date.fromisoformat(args.from_date),
    )
