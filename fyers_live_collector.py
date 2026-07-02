"""
fyers_live_collector.py
-------------------------
Track B: build YOUR OWN intraday options archive going forward, since
Fyers cannot retroactively give you historical/expired option candles.

Run this once per trading day, AFTER market close (e.g. 16:00 IST), so the
full day's 5-min candles are available. Each run:
  1. For each underlying, calls Fyers optionchain() to discover which
     expiries currently exist and which strikes are active.
  2. For each (underlying, expiry, strike, CE/PE) contract, calls
     Fyers history() for today's 5-min candles.
  3. Writes results into the canonical schema, partitioned same as Track A
     (by underlying + year), so your backtester can query across both
     tracks without caring which source a given date came from.

IMPORTANT LIMITATIONS (by design, not bugs):
  - Only ACTIVE (non-expired) contracts return data. This script will
    silently get empty candles for anything Fyers has already delisted
    from history -- that's expected, not an error to chase.
  - Fyers history() does NOT return open interest. The 'oi' column will be
    NaN for all Track B rows. If OI matters to your strategies, you'll need
    it from Track A's daily NSE bhavcopy (OI is EOD-only anyway industry-
    wide, so this isn't actually a loss vs what's achievable elsewhere).
  - Strike universe is capped by `strikecount` -- deep ITM/OTM strikes
    outside that window won't be collected. Raise STRIKE_COUNT below if
    your strategies trade far-OTM wings.
  - Rate limits: Fyers' history endpoint allows an unlimited NUMBER of
    symbols/day but each call still counts toward the 10k/day API call
    rate limit. A full chain pull (multiple expiries x ~30 strikes x 2
    CE/PE) can run into the hundreds of calls per underlying per day --
    budget accordingly if you're tracking many underlyings.

Usage:
    python fyers_live_collector.py --underlyings NIFTY,BANKNIFTY
    python fyers_live_collector.py --underlyings NIFTY --strikecount 40 --max-expiries 2
"""

import argparse
import logging
import time
from datetime import date, datetime

import pandas as pd

from confignew import (
    FYERS_INTRADAY_DIR, FYERS_INDEX_SYMBOL_MAP, LOG_DIR, CANONICAL_COLUMNS,
)
from fyers_auth import get_fyers_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "fyers_live_collector.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DEFAULT_STRIKE_COUNT = 30  # -> up to 30 ITM + 30 OTM + ATM per expiry, per side
RATE_LIMIT_SLEEP_S = 0.25   # be polite -- avoid tripping Fyers' rate limiter


def get_option_chain(fyers, underlying_symbol: str, strikecount: int, timestamp: str = ""):
    """Wraps fyers.optionchain(); returns the raw response dict."""
    resp = fyers.optionchain(data={
        "symbol": underlying_symbol,
        "timestamp": timestamp,
        "strikecount": strikecount,
    })
    if resp.get("s") != "ok":
        log.warning(f"optionchain call failed for {underlying_symbol} "
                    f"(timestamp={timestamp!r}): {resp}")
        return None
    return resp


def discover_expiries(fyers, underlying_symbol: str, max_expiries: int) -> list:
    """
    Calls optionchain with empty timestamp to get the list of available
    expiries from expiryData, then returns up to max_expiries of them
    (nearest first).
    """
    resp = get_option_chain(fyers, underlying_symbol, strikecount=1, timestamp="")
    if resp is None:
        return []
    expiry_data = resp.get("data", {}).get("expiryData", [])
    # expiryData items look like {"date": "29-08-2024", "expiry": "1724913000"}
    expiries = [(e["expiry"], e["date"]) for e in expiry_data]
    return expiries[:max_expiries]


def fetch_chain_for_expiry(fyers, underlying_symbol: str, expiry_timestamp: str,
                            strikecount: int) -> pd.DataFrame:
    """
    Returns a DataFrame of contract symbols + strikes + option_type for one
    expiry, derived from optionchain's optionsChain list. This gives us the
    exact Fyers symbol strings we need to feed into history().
    """
    resp = get_option_chain(fyers, underlying_symbol, strikecount, expiry_timestamp)
    if resp is None:
        return pd.DataFrame()

    rows = []
    for item in resp.get("data", {}).get("optionsChain", []):
        # optionsChain entries include the underlying's own row (option_type
        # blank/"") interleaved with CE/PE rows -- skip non-option rows.
        opt_type = item.get("option_type")
        if opt_type not in ("CE", "PE"):
            continue
        rows.append({
            "fyers_symbol": item.get("symbol"),
            "strike": item.get("strike_price"),
            "option_type": opt_type,
        })
    return pd.DataFrame(rows)


def fetch_intraday_candles(fyers, fyers_symbol: str, day: date, resolution: str = "5") -> pd.DataFrame:
    """Pulls one day's 5-min candles for a single contract symbol."""
    resp = fyers.history(data={
        "symbol": fyers_symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": day.isoformat(),
        "range_to": day.isoformat(),
        "cont_flag": "0",
    })
    if resp.get("s") != "ok" or not resp.get("candles"):
        # Expected for illiquid/just-listed/about-to-expire contracts -- not
        # necessarily an error worth alarming on.
        return pd.DataFrame()

    df = pd.DataFrame(resp["candles"],
                       columns=["epoch", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["epoch"], unit="s", utc=True).dt.tz_convert(
        "Asia/Kolkata"
    )
    return df


def parse_fyers_option_symbol(fyers_symbol: str):
    """
    Fyers option symbols look like: NSE:NIFTY24AUG24500CE or
    NSE:NIFTY2482924500CE (weekly). Extracting underlying cleanly from this
    string is fragile across format variants, so we DON'T rely on parsing
    it -- strike/option_type/underlying are instead carried through
    explicitly from fetch_chain_for_expiry's structured fields. This
    function is kept only for logging/debugging legibility.
    """
    return fyers_symbol.replace("NSE:", "").replace("BSE:", "")


def run(underlyings: list, strikecount: int, max_expiries: int, day: date = None):
    day = day or date.today()
    fyers = get_fyers_client()

    all_rows = []

    for underlying in underlyings:
        fyers_symbol = FYERS_INDEX_SYMBOL_MAP.get(underlying)
        if not fyers_symbol:
            log.warning(f"No Fyers symbol mapping for {underlying} -- "
                        f"add it to FYERS_INDEX_SYMBOL_MAP in config.py "
                        f"(stock options need 'NSE:<SYMBOL>-EQ' style "
                        f"underlying lookups, not yet wired up here).")
            continue

        expiries = discover_expiries(fyers, fyers_symbol, max_expiries)
        if not expiries:
            log.warning(f"{underlying}: no expiries discovered, skipping")
            continue
        log.info(f"{underlying}: found {len(expiries)} expiries to pull "
                f"-> {[e[1] for e in expiries]}")

        for expiry_ts, expiry_date_str in expiries:
            chain_df = fetch_chain_for_expiry(fyers, fyers_symbol, expiry_ts, strikecount)
            if chain_df.empty:
                log.warning(f"{underlying} {expiry_date_str}: empty chain, skipping")
                continue

            expiry_iso = datetime.strptime(expiry_date_str, "%d-%m-%Y").date().isoformat()
            log.info(f"{underlying} {expiry_iso}: pulling {len(chain_df)} contracts")

            for _, row in chain_df.iterrows():
                candles = fetch_intraday_candles(fyers, row["fyers_symbol"], day)
                time.sleep(RATE_LIMIT_SLEEP_S)
                if candles.empty:
                    continue

                candles["date"] = day.isoformat()
                candles["underlying"] = underlying
                candles["expiry"] = expiry_iso
                candles["strike"] = row["strike"]
                candles["option_type"] = row["option_type"]
                candles["oi"] = pd.NA          # Fyers history() doesn't return OI
                candles["settle_price"] = pd.NA  # only meaningful EOD
                candles["source"] = "fyers_history"
                candles["granularity"] = "5min"
                all_rows.append(candles.reindex(columns=CANONICAL_COLUMNS))

    if not all_rows:
        log.warning("No data collected this run. Check token validity "
                    "(generate_fyers_token.py) and that markets were open "
                    f"on {day.isoformat()}.")
        return

    full = pd.concat(all_rows, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])
    full["year"] = full["date"].dt.year

    for (underlying, year), group in full.groupby(["underlying", "year"]):
        out_dir = FYERS_INTRADAY_DIR / underlying
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{underlying}_{year}.parquet"
        group = group.drop(columns=["year"])

        if out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["date"] = pd.to_datetime(existing["date"])
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["timestamp", "underlying", "expiry", "strike", "option_type"],
                keep="last",
            )
        else:
            combined = group

        combined.to_parquet(out_path, index=False)
        log.info(f"Wrote {len(combined)} total rows -> {out_path} "
                f"({len(group)} new this run)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Collect today's intraday option candles from Fyers")
    ap.add_argument("--underlyings", required=True,
                     help="Comma-separated, e.g. NIFTY,BANKNIFTY (must exist in FYERS_INDEX_SYMBOL_MAP)")
    ap.add_argument("--strikecount", type=int, default=DEFAULT_STRIKE_COUNT)
    ap.add_argument("--max-expiries", type=int, default=2,
                     help="How many upcoming expiries to pull per underlying")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD, defaults to today")
    args = ap.parse_args()

    underlyings = args.underlyings.split(",")
    day = date.fromisoformat(args.date) if args.date else date.today()
    run(underlyings, args.strikecount, args.max_expiries, day)
