# NSE Options Data Pipeline — Historical + Live

## Read this first

**Fyers cannot give you historical (expired) options data.** This is a
confirmed, long-standing limitation — repeatedly reported on Fyers' own
community forums, with no fix as of the most recent thread (Feb 2026), and
no roadmap. Active/non-expired contracts work fine at intraday resolution.
Expired contracts return empty results, full stop.

So this pipeline is two tracks that meet in one schema:

| | Track A (historical) | Track B (forward) |
|---|---|---|
| Source | NSE F&O Bhavcopy / UDiFF archives | Fyers `optionchain` + `history` |
| Coverage | Years back, free | Today onward only |
| Granularity | **Daily** (OHLC, OI, settle) | **5-min intraday** |
| Cost | Free | Free (your existing Fyers account) |

There is currently no free source for **intraday** history on **expired**
contracts. If your strategies need 5-min granularity on old expiries, the
only paths are paid vendors (TrueData, Global Datafeeds) or your broker's
own tick archive if they happen to retain one for your account. This
pipeline gets you everything free; bolt on a paid vendor later if intraday
historical depth turns out to matter for your specific strategies.

## Files

- `config.py` — shared paths, the UDiFF format-switch date, underlying symbol maps, canonical output schema
- `download_nse_bhavcopy.py` — Track A step 1: downloads raw daily F&O bhavcopy zips
- `parse_nse_bhavcopy.py` — Track A step 2: parses legacy + UDiFF formats into canonical Parquet
- `generate_fyers_token.py` — run once per day before the collector; Fyers tokens expire daily
- `fyers_auth.py` — loads the saved token into a Fyers client
- `fyers_live_collector.py` — Track B: discovers active contracts, pulls today's 5-min candles
- `diagnose_missing_strikes.py` — checks whether missing far ITM/OTM strikes on a given day are a real pipeline gap or genuinely untraded contracts (see below)

## Why far ITM/OTM strikes are often "missing" — and how to tell if that's a bug

NSE's bhavcopy is a **trade/settlement report, not a contract master**. A
strike with zero trades that day has no row to report — this isn't
`jugaad-data` or this pipeline dropping anything. On top of that, NSE
periodically **prunes illiquid strikes** with zero open interest from the
listed contract ladder entirely, and the strike step (interval between
strikes) is set per-underlying based on volatility, so how far the ladder
extends isn't fixed over time.

So before assuming a missing far-wing strike is a pipeline bug, check:

```bash
python diagnose_missing_strikes.py --underlying NIFTY --date 2024-08-01 \
    --expiry 2024-08-29 --spot 24550 --range-pct 15
```

This reconstructs the full theoretical strike ladder around spot, diffs it
against what's actually in your parsed data, and analyzes the **upside
(calls) and downside (puts) independently** — calls and puts trade as
separate books with their own liquidity tapers, and it is completely
normal for one side to extend further than the other (e.g. puts trading
out to 10% from spot while calls thin out by 7%). An earlier version of
this script compared missing/present strikes across both sides pooled
together, which produced false-positive "gap" verdicts on perfectly
ordinary asymmetric chains — caught via a real run against actual NIFTY
data. The current version also explicitly checks for **interior gaps**:
a missing strike that sits closer to spot than another present, liquid
strike on the same side cannot be explained by liquidity decay, and gets
flagged as a real gap by name, regardless of where the overall taper
point falls.

If the verdict says strikes are genuinely untraded, there is no
historical price for them anywhere, on any vendor, free or paid, because
no trade happened. The only honest path for a backtester that still needs
a price there is **synthetic Black-Scholes pricing** off the implied vol
surface built from strikes that did trade that day. If the verdict flags
a real gap, it names the exact sandwiched strikes — re-check that date's
raw download and parser log for those specific strikes.

## Run order

### One-time historical backfill
```bash
# 1. Download raw files (this WILL take a while for years of data — be
#    patient, it sleeps between requests to avoid getting blocked by NSE)
python download_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27

# 2. Parse into canonical Parquet, filtered to underlyings you care about
python parse_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27 \
    --underlyings NIFTY,BANKNIFTY,FINNIFTY
```

### Daily, going forward (e.g. a cron job at 16:00 IST after market close)
```bash
# 1. Refresh today's access token (Fyers tokens expire overnight)
python generate_fyers_token.py

# 2. Collect today's intraday option candles
python fyers_live_collector.py --underlyings NIFTY,BANKNIFTY --strikecount 30
```

## Exercise Style (European vs. American)

NIFTY and BANKNIFTY index options were always **European-style** (`CE`/`PE`). However, individual stock options (`OPTSTK`) traded on NSE were **American-style** (`CA`/`PA`) until NSE transitioned them to European-style in 2010-2011.

Because American vs. European exercise style is real financial information that changes fair value assumptions (early-exercise premium), the pipeline parses this faithfully:
- The `option_type` column retains the raw NSE code (`CE`/`PE`/`CA`/`PA`).
- An explicit `exercise_style` column is added, mapping to `"european"` or `"american"`.

**Warning:** If you are querying pre-2011 data and filter by `option_type == "CE"` alone, you will silently miss all American-style calls. Filter on `exercise_style` if you want to be agnostic to exercise style.

## Known rough edges, by design

1. **NSE holiday calendar isn't hardcoded.** The downloader treats a 404 as
   "holiday or no trading" rather than maintaining its own calendar. Cross-
   check final coverage against NSE's published holiday list if you need
   certainty on specific gaps.
2. **NSE may shift URL/column formats again** (it already has once — old
   bhavcopy → UDiFF, July 2024). The parser detects option rows by
   `OptnTp`/`STRIKE_PR` being populated rather than trusting exact
   instrument-type codes, which is somewhat more robust, but a future NSE
   format change will still need a parser update. If `parse_nse_bhavcopy.py`
   starts logging `KeyError` for dates that used to work, that's the signal.
3. **Fyers `history()` returns no open interest.** OI is daily-only
   everywhere in this market anyway (no vendor publishes intraday OI), so
   for OI you always fall back to Track A's daily NSE data regardless of
   how the rest of your project evolves.
4. **Strike coverage in Track B is capped by `strikecount`.** Deep
   ITM/OTM wings outside that window are not collected. Raise it if a
   strategy trades far-dated wings.
5. **Fyers API call budget**: each `history()` call counts toward the
   10k/day limit. A full chain (2 expiries × ~60 strikes × 2 sides ≈ 240
   calls) per underlying per day adds up fast across many underlyings —
   budget your `--underlyings` list and `--max-expiries` accordingly.
6. **Token refresh isn't automated.** Fyers has no silent refresh token in
   the v3 SDK — `generate_fyers_token.py` requires a one-time browser login
   each trading day. If you want this to run as an unattended overnight
   cron job long-term, you'll need to script the login redirect (e.g. with
   Selenium/Playwright) — not included here since it's a meaningfully
   different, more fragile piece of automation, and worth doing only once
   you're confident in the rest of the pipeline.

## Joining Track A + Track B in your backtester

Both tracks write to the same 15-column canonical schema (see
`CANONICAL_COLUMNS` in `config.py`), partitioned identically by
`underlying/underlying_year.parquet`. For dates before today, you'll only
have Track A (daily). For today onward, you'll have both — Track B gives
you intraday, Track A gives you the EOD anchor/OI. A simple backtester
read path:

```python
import pandas as pd
from pathlib import Path

def load_underlying(underlying: str, daily_dir: Path, intraday_dir: Path):
    daily_files = sorted((daily_dir / underlying).glob("*.parquet"))
    intraday_files = sorted((intraday_dir / underlying).glob("*.parquet"))
    daily = pd.concat([pd.read_parquet(f) for f in daily_files], ignore_index=True) if daily_files else pd.DataFrame()
    intraday = pd.concat([pd.read_parquet(f) for f in intraday_files], ignore_index=True) if intraday_files else pd.DataFrame()
    return daily, intraday
```

Use `daily` for OI-dependent or EOD-resolution strategies across your full
history; use `intraday` for 5-min-resolution backtests, which will only be
as deep as how long you've been running the daily collector.