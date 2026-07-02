# NSE Options Data Pipeline — Historical + Live

## Overview

This pipeline collects and manages NSE options data from multiple sources to give you a complete picture for backtesting: EOD data for deep history, intraday data for recent history, and live collection for forward data.

| | Track A (EOD History) | Track B (Historical Intraday) | Track C (Live Forward) |
|---|---|---|---|
| **Source** | NSE F&O Bhavcopy / UDiFF | Upstox Expired Historical API | Fyers & Kotak Neo APIs |
| **Coverage** | Years back | Jan 2022 onward | Today onward |
| **Granularity**| **Daily** (OHLC, OI, settle) | **1-min intraday** (OHLCV) | **1-min intraday / Snapshot** |
| **Cost** | Free | Free (requires Upstox account) | Free (requires Fyers/Kotak account) |

## Files

- `confignew.py` — shared paths, UDiFF format-switch date, underlying symbol maps, canonical output schema
- `download_nse_bhavcopy.py` — Track A step 1: downloads raw daily F&O bhavcopy zips
- `parse_nse_bhavcopy.py` — Track A step 2: parses legacy + UDiFF formats into canonical Parquet
- `upstox_auth.py` / `upstox_enumerate_contracts.py` / `upstox_pull_candles.py` — Track B: backfills 1-min historical candles for expired options
- `generate_fyers_token.py` / `fyers_auth.py` / `fyers_live_collector.py` — Track C: discovers active contracts, pulls today's candles from Fyers
- `kotak_auth.py` / `kotak_live_collector.py` — Track C: collects live market data via Kotak Neo (EOD Snapshot or WebSocket streaming)
- `diagnose_missing_strikes.py` — checks whether missing far ITM/OTM strikes on a given day are a real pipeline gap or genuinely untraded contracts

## Track B: Upstox Historical Intraday Backfill

Upstox provides an invaluable (and rare) free API for accessing 1-minute historical data for **expired** option contracts.
The backfill process is split into two resumable phases:

1. **Enumerate**: Queries Upstox to build a complete list of every (underlying, expiry, strike, CE/PE) contract that existed. Stores this in a local SQLite state database.
2. **Pull Candles**: Iterates through the state DB and fetches the 1-min OHLCV candles for every pending contract, saving them into partitioned Parquet files.

## Track C: Live Collection (Kotak Neo & Fyers)

Both Fyers and Kotak Neo can be used to collect data going forward. 
Kotak Neo supports two modes:
- **Snapshot mode**: Run after market close to get an EOD quote snapshot for all active contracts.
- **WebSocket mode**: Run during market hours to stream ticks and build 1-min candles on the fly.

## Run order

### 1. EOD Historical Backfill (Track A)
```bash
python download_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27
python parse_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27 --underlyings NIFTY,BANKNIFTY
```

### 2. Intraday Historical Backfill (Track B)
```bash
# Set your UPSTOX_ACCESS_TOKEN first!
python upstox_enumerate_contracts.py --underlyings NIFTY,BANKNIFTY
python upstox_pull_candles.py --underlyings NIFTY,BANKNIFTY
```

### 3. Daily Live Collection (Track C)
```bash
# E.g. via cron job after market close for Fyers:
python generate_fyers_token.py
python fyers_live_collector.py --underlyings NIFTY,BANKNIFTY --strikecount 30

# Or using Kotak Neo for an EOD Snapshot:
python kotak_live_collector.py --underlyings NIFTY,BANKNIFTY
```

## Exercise Style (European vs. American)

NIFTY and BANKNIFTY index options were always **European-style** (`CE`/`PE`). However, individual stock options (`OPTSTK`) traded on NSE were **American-style** (`CA`/`PA`) until NSE transitioned them to European-style in 2010-2011.

Because American vs. European exercise style is real financial information that changes fair value assumptions (early-exercise premium), the pipeline parses this faithfully:
- The `option_type` column retains the raw NSE code (`CE`/`PE`/`CA`/`PA`).
- An explicit `exercise_style` column is added, mapping to `"european"` or `"american"`.

## Joining Tracks in your backtester

All tracks write to the same 16-column canonical schema (see `CANONICAL_COLUMNS` in `confignew.py`), partitioned identically by `underlying/underlying_year.parquet`. This makes unioning them trivial. Use daily data for OI-dependent EOD backtests, and intraday data for high-resolution backtests.