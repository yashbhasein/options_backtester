# Options Data Pipeline — Revised Architecture
## Upstox Plus (Historical) + Kotak Neo (Live)

---

## Data Source Map

| Layer | Source | Granularity | Coverage | Notes |
|---|---|---|---|---|
| Deep historical (EOD) | NSE Bhavcopy (existing pipeline) | Daily | 2001–present | Keep as-is, untouched |
| Intraday historical | Upstox Plus API | 1min | Jan 2022–present | Expired contracts via `ExpiredInstrumentApi` |
| Live / forward | Kotak Neo API | WebSocket tick + REST | Today onward | Replaces Fyers |

## Critical Scale Constraint (Upstox backfill)

At 1-min resolution, NIFTY + BANKNIFTY from Jan 2022 means roughly:
- ~1,100 trading days × ~100 strikes × ~3 expiries × 2 sides = ~660,000 calls for NIFTY
- Rate limit: ~10 req/sec (undocumented but community-observed)
- Estimated runtime: 37–75 hours of continuous pulling

This MUST be a resumable, crash-safe pipeline. Design:
1. Phase 1: Enumerate ALL expired contracts into a SQLite state DB
2. Phase 2: Pull candles for each contract, marking each as 'done' atomically
3. Any crash/restart resumes from the last incomplete contract

## File Layout

```
new_pipeline/
├── PLAN.md                          # this file
├── config.py                        # shared config, canonical schema, paths
├── upstox_auth.py                   # Upstox OAuth flow
├── upstox_enumerate_contracts.py    # Phase 1: build contract universe into SQLite
├── upstox_pull_candles.py           # Phase 2: pull 1-min candles per contract
├── kotak_auth.py                    # Kotak Neo TOTP login
├── kotak_live_collector.py          # WebSocket live data collection
└── requirements.txt
```

## Canonical Output Schema (same as existing NSE pipeline, 2 columns added)

| Column | Type | Notes |
|---|---|---|
| date | date | Trading date |
| timestamp | datetime (IST) | Candle start time; for daily rows = date@15:30 |
| underlying | str | NIFTY, BANKNIFTY, etc. |
| expiry | date str | Contract expiry date |
| strike | float | Strike price |
| option_type | str | CE / PE / CA / PA |
| exercise_style | str | european / american |
| open / high / low / close | float | |
| volume | int | |
| oi | int | NaN for Upstox intraday (not returned) |
| settle_price | float | NaN for intraday |
| source | str | upstox_expired / kotak_live |
| granularity | str | 1min / 5min / 1d |

## Output Partitioning

Parquet, partitioned by: `underlying / underlying_year.parquet`
Same scheme as existing NSE bhavcopy pipeline — backtester reads both
without needing to know which source any given row came from.
