"""
config.py — shared config for Upstox + Kotak Neo pipeline
"""
import os
from pathlib import Path
from datetime import date

BASE_DIR = Path(os.environ.get("OPTIONS_DATA_DIR", "./options_data")).resolve()

# Upstox intraday historical (Track B)
UPSTOX_INTRADAY_DIR = BASE_DIR / "parsed" / "upstox_intraday"
# Kotak live (Track C)
KOTAK_LIVE_DIR = BASE_DIR / "parsed" / "kotak_live"
# SQLite state DB for resumable Upstox backfill
STATE_DB_PATH = BASE_DIR / "upstox_backfill_state.db"
LOG_DIR = BASE_DIR / "logs"

for d in (UPSTOX_INTRADAY_DIR, KOTAK_LIVE_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Upstox: Jan 2022 is the confirmed floor for 1-min intraday data
UPSTOX_INTRADAY_START = date(2022, 1, 1)

# Underlying symbol maps
UPSTOX_UNDERLYING_MAP = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
}

CANONICAL_COLUMNS = [
    "date", "timestamp", "underlying", "expiry", "strike", "option_type",
    "exercise_style", "open", "high", "low", "close", "volume", "oi",
    "settle_price", "source", "granularity",
]

# Rate limiting
UPSTOX_SLEEP_BETWEEN_CALLS = 0.15   # ~6-7 req/sec, comfortably under the limit
KOTAK_WS_MAX_SYMBOLS = 30            # Kotak Neo WebSocket limit per request
