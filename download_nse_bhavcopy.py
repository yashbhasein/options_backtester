"""
download_nse_bhavcopy.py
-------------------------
Track A, step 1: download RAW NSE F&O bhavcopy files for a date range.

Handles the July 8, 2024 format switch:
  - dates <  UDIFF_START_DATE -> legacy "fo<dd><MON><yyyy>bhav.csv.zip"
  - dates >= UDIFF_START_DATE -> "BhavCopy_NSE_FO_0_0_0_<yyyymmdd>_F_0000.csv.zip"

This step only downloads and unzips into RAW_NSE_DIR. It does NOT parse
columns -- that happens in parse_nse_bhavcopy.py, deliberately split out
so a parsing bug never forces you to re-download anything.

Usage:
    python download_nse_bhavcopy.py --start 2018-01-01 --end 2026-06-27
    python download_nse_bhavcopy.py --start 2024-07-01 --end 2024-07-31 --retry-failed
"""

import argparse
import io
import logging
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests

from confignew import RAW_NSE_DIR, UDIFF_START_DATE, LOG_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "download_nse_bhavcopy.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {
    # NSE blocks requests without a browser-like UA / referer. This mirrors
    # what jugaad-data and other NSE scrapers use successfully.
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "referer": "https://www.nseindia.com/all-reports-derivatives",
}

LEGACY_URL_FMT = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
    "{yyyy}/{mon}/fo{dd}{mon}{yyyy}bhav.csv.zip"
)
UDIFF_URL_FMT = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

NSE_HOLIDAYS_NOTE = (
    "This script does not hardcode an NSE trading-day calendar. It simply "
    "tries every calendar day Mon-Fri and treats a 404/empty response as "
    "'no trading that day' (holiday) rather than an error. Cross-check your "
    "final parsed output's date coverage against NSE's published holiday "
    "list if you need certainty on which gaps are holidays vs failed pulls."
)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:  # skip Sat/Sun outright; NSE has no weekend sessions
            yield d
        d += timedelta(days=1)


def build_url(d: date) -> str:
    if d >= UDIFF_START_DATE:
        return UDIFF_URL_FMT.format(yyyymmdd=d.strftime("%Y%m%d"))
    return LEGACY_URL_FMT.format(
        yyyy=d.year, mon=d.strftime("%b").upper(), dd=d.strftime("%d")
    )


def dest_path_for(d: date) -> Path:
    # Keep raw files partitioned by year so the directory stays browsable.
    year_dir = RAW_NSE_DIR / str(d.year)
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / f"fo_bhav_{d.isoformat()}.csv"


def download_one(session: requests.Session, d: date, skip_if_present: bool = True) -> str:
    """
    Returns one of: 'ok', 'skipped', 'holiday_or_missing', 'error'
    """
    dest = dest_path_for(d)
    if skip_if_present and dest.exists():
        return "skipped"

    url = build_url(d)
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
    except requests.RequestException as e:
        log.warning(f"{d.isoformat()}: request failed ({e})")
        return "error"

    if resp.status_code == 404:
        # Most often a trading holiday. Could also mean NSE moved the path
        # again -- if you see this for a date you KNOW was a trading day,
        # the URL pattern likely needs updating.
        log.info(f"{d.isoformat()}: 404 (holiday or path changed)")
        return "holiday_or_missing"

    if resp.status_code != 200 or len(resp.content) < 200:
        log.warning(f"{d.isoformat()}: unexpected response ({resp.status_code}, "
                    f"{len(resp.content)} bytes)")
        return "error"

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = zf.namelist()
            if not names:
                log.warning(f"{d.isoformat()}: zip had no files")
                return "error"
            with zf.open(names[0]) as f:
                text = f.read().decode("utf-8", errors="replace")
    except zipfile.BadZipFile:
        log.warning(f"{d.isoformat()}: response was not a valid zip "
                    f"(NSE may have served an HTML error page)")
        return "error"

    dest.write_text(text, encoding="utf-8")
    log.info(f"{d.isoformat()}: saved -> {dest}")
    return "ok"


def run(start: date, end: date, skip_if_present: bool = True, sleep_s: float = 0.8):
    session = requests.Session()
    # Touch the homepage first to pick up cookies NSE expects on subsequent
    # archive requests -- many NSE scrapers report archive requests get
    # rejected without this warm-up.
    try:
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
    except requests.RequestException:
        log.warning("Could not warm up session with nseindia.com homepage; continuing anyway")

    counts = {"ok": 0, "skipped": 0, "holiday_or_missing": 0, "error": 0}
    errors = []

    for d in daterange(start, end):
        status = download_one(session, d, skip_if_present=skip_if_present)
        counts[status] += 1
        if status == "error":
            errors.append(d.isoformat())
        time.sleep(sleep_s)  # be polite -- NSE rate-limits/blocks aggressive scraping

    log.info(f"Done. {counts}")
    if errors:
        log.warning(f"{len(errors)} dates failed with errors: {errors[:20]}"
                    f"{' ...' if len(errors) > 20 else ''}")
        log.warning("Re-run with --retry-failed to retry only these, or just "
                    "re-run the same command -- skip_if_present means "
                    "successfully-downloaded days won't be re-fetched.")
    log.info(NSE_HOLIDAYS_NOTE)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Download raw NSE F&O bhavcopy files")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--no-skip", action="store_true",
                     help="Re-download even if file already exists")
    ap.add_argument("--sleep", type=float, default=0.8,
                     help="Seconds to sleep between requests (be polite to NSE)")
    args = ap.parse_args()

    start_d = date.fromisoformat(args.start)
    end_d = date.fromisoformat(args.end)
    run(start_d, end_d, skip_if_present=not args.no_skip, sleep_s=args.sleep)
