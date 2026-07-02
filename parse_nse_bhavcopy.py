"""
parse_nse_bhavcopy.py
-----------------------
Track A, step 2: parse raw NSE F&O bhavcopy CSVs (downloaded by
download_nse_bhavcopy.py) into the canonical schema, filtered to OPTIONS
rows only (OPTIDX + OPTSTK), and write out as partitioned Parquet.

Two source schemas, handled separately:

LEGACY (pre 2024-07-08), columns include:
    INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP,
    OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH,
    OPEN_INT, CHG_IN_OI, TIMESTAMP
  INSTRUMENT values: FUTIDX, FUTSTK, OPTIDX, OPTSTK

UDIFF (>= 2024-07-08), columns include:
    TradDt, BizDt, Sgmt, Src, FinInstrmTp, FinInstrmId, ISIN, TckrSymb,
    SctySrs, XpryDt, FininstrmActlXpryDt, StrkPric, OptnTp, FinInstrmNm,
    OpnPric, HghPric, LwPric, ClsPric, LastPric, PrvsClsgPric,
    UndrlygPric, SttlmPric, OpnIntrst, ChngInOpnIntrst, TtlTradgVol,
    TtlTrfVal, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks, Rsvd01..04
  FinInstrmTp values: IDO (index options), STO (stock options),
                       IDF (index futures), STF (stock futures)
  NOTE: NSE has been known to shuffle exact FinInstrmTp codes between
  rollouts -- this script detects options rows via OptnTp/StrkPric being
  non-null instead of hardcoding FinInstrmTp, which is more robust.

Usage:
    python parse_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27
    python parse_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27 --underlyings NIFTY,BANKNIFTY
"""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from confignew import RAW_NSE_DIR, PARSED_HIST_DIR, UDIFF_START_DATE, LOG_DIR, CANONICAL_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "parse_nse_bhavcopy.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def _raw_path_for(d: date) -> Path:
    return RAW_NSE_DIR / str(d.year) / f"fo_bhav_{d.isoformat()}.csv"


def parse_legacy(text: str, d: date) -> pd.DataFrame:
    from io import StringIO
    import re
    
    # Fix missing newlines in legacy records (NSE generator bug in 2002/2003)
    text = re.sub(
        r'(\d{2}-[A-Za-z]{3}-\d{4},)(OPTSTK|OPTIDX|FUTSTK|FUTIDX)', 
        r'\1\n\2', 
        text
    )
    df = pd.read_csv(StringIO(text))
    df.columns = [c.strip() for c in df.columns]

    is_option = df["INSTRUMENT"].isin(["OPTIDX", "OPTSTK"])
    df = df.loc[is_option].copy()
    if df.empty:
        return df

    out = pd.DataFrame({
        "date": d.isoformat(),
        "timestamp": d.isoformat(),
        "underlying": df["SYMBOL"].str.strip(),
        "expiry": pd.to_datetime(df["EXPIRY_DT"], format="%d-%b-%Y", errors="coerce").dt.date.astype(str),
        "strike": pd.to_numeric(df["STRIKE_PR"], errors="coerce"),
        "option_type": df["OPTION_TYP"].str.strip() if "OPTION_TYP" in df.columns else df["OPTIONTYPE"].str.strip(),
        "exercise_style": (df["OPTION_TYP"].str.strip() if "OPTION_TYP" in df.columns else df["OPTIONTYPE"].str.strip()).map({"CE": "european", "PE": "european", "CA": "american", "PA": "american"}),
        "open": pd.to_numeric(df["OPEN"], errors="coerce"),
        "high": pd.to_numeric(df["HIGH"], errors="coerce"),
        "low": pd.to_numeric(df["LOW"], errors="coerce"),
        "close": pd.to_numeric(df["CLOSE"], errors="coerce"),
        "volume": pd.to_numeric(df["CONTRACTS"], errors="coerce"),
        "oi": pd.to_numeric(df["OPEN_INT"], errors="coerce"),
        "settle_price": pd.to_numeric(df["SETTLE_PR"], errors="coerce"),
        "source": "nse_bhavcopy_legacy",
        "granularity": "1d",
    })
    return out


def parse_udiff(text: str, d: date) -> pd.DataFrame:
    from io import StringIO
    df = pd.read_csv(StringIO(text))
    df.columns = [c.strip() for c in df.columns]

    # Options rows: OptnTp is "CE"/"PE" for options, NaN/blank for futures.
    # More robust than trusting the exact FinInstrmTp code, which NSE has
    # changed before.
    is_option = df["OptnTp"].isin(["CE", "PE"])
    df = df.loc[is_option].copy()
    if df.empty:
        return df

    out = pd.DataFrame({
        "date": d.isoformat(),
        "timestamp": d.isoformat(),
        "underlying": df["TckrSymb"].str.strip(),
        "expiry": pd.to_datetime(df["XpryDt"], errors="coerce").dt.date.astype(str),
        "strike": pd.to_numeric(df["StrkPric"], errors="coerce"),
        "option_type": df["OptnTp"].str.strip(),
        "exercise_style": "european",
        "open": pd.to_numeric(df["OpnPric"], errors="coerce"),
        "high": pd.to_numeric(df["HghPric"], errors="coerce"),
        "low": pd.to_numeric(df["LwPric"], errors="coerce"),
        "close": pd.to_numeric(df["ClsPric"], errors="coerce"),
        "volume": pd.to_numeric(df["TtlTradgVol"], errors="coerce"),
        "oi": pd.to_numeric(df["OpnIntrst"], errors="coerce"),
        "settle_price": pd.to_numeric(df["SttlmPric"], errors="coerce"),
        "source": "nse_bhavcopy_udiff",
        "granularity": "1d",
    })
    return out


def parse_one_day(d: date) -> pd.DataFrame:
    raw_path = _raw_path_for(d)
    if not raw_path.exists():
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    text = raw_path.read_text(encoding="utf-8", errors="replace")
    try:
        if d >= UDIFF_START_DATE:
            df = parse_udiff(text, d)
        else:
            df = parse_legacy(text, d)
    except KeyError as e:
        log.error(f"{d.isoformat()}: missing expected column {e} -- "
                  f"NSE may have changed schema again. File: {raw_path}")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return df.reindex(columns=CANONICAL_COLUMNS)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def run(start: date, end: date, underlyings=None):
    all_rows = []
    n_days_parsed = 0
    n_days_missing_raw = 0

    for d in daterange(start, end):
        df = parse_one_day(d)
        if df.empty:
            n_days_missing_raw += 1
            continue
        if underlyings:
            df = df[df["underlying"].isin(underlyings)]
        all_rows.append(df)
        n_days_parsed += 1

    if not all_rows:
        log.warning("No data parsed for the given range. Did you run "
                    "download_nse_bhavcopy.py first?")
        return

    full = pd.concat(all_rows, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])
    full["year"] = full["date"].dt.year

    # Partition by underlying + year -- matches the partitioning pattern
    # your 5-min equity data and intraday Fyers archive should also use,
    # so backtester joins stay consistent.
    for (underlying, year), group in full.groupby(["underlying", "year"]):
        out_dir = PARSED_HIST_DIR / underlying
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{underlying}_{year}.parquet"
        group = group.drop(columns=["year"])

        if out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["date"] = pd.to_datetime(existing["date"])
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["date", "underlying", "expiry", "strike", "option_type"],
                keep="last",
            )
        else:
            combined = group

        combined.to_parquet(out_path, index=False)
        log.info(f"Wrote {len(combined)} rows -> {out_path}")

    log.info(f"Parsed {n_days_parsed} trading days, "
            f"{n_days_missing_raw} days had no raw file on disk "
            f"(run the downloader for those, or they're holidays).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Parse raw NSE F&O bhavcopy into canonical schema")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--underlyings", default=None,
                     help="Comma-separated list to filter, e.g. NIFTY,BANKNIFTY. "
                          "Omit to keep everything (large -- thousands of stock options too).")
    args = ap.parse_args()

    start_d = date.fromisoformat(args.start)
    end_d = date.fromisoformat(args.end)
    underlyings = args.underlyings.split(",") if args.underlyings else None
    run(start_d, end_d, underlyings=underlyings)
