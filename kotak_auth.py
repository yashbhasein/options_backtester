"""
kotak_auth.py
--------------
Kotak Neo TOTP-based authentication.

Prerequisite (one-time setup):
  1. Login to Kotak Neo web/app → Invest → Trade API → API Dashboard
  2. Generate an application → copy the Consumer Key (this is your token)
  3. Register for TOTP at https://www.kotaksecurities.com/platform/kotak-neo-trade-api/
     Scan the QR code with Google/Microsoft Authenticator
     This gives you 6-digit TOTP codes and a TOTP secret key

Required env vars:
    KOTAK_CONSUMER_KEY   — from Trade API dashboard
    KOTAK_MOBILE         — registered mobile number with country code (e.g. +919876543210)
    KOTAK_UCC            — Unique Client Code (from profile section)
    KOTAK_MPIN           — 6-digit MPIN
    KOTAK_TOTP           — current 6-digit TOTP code (changes every 30s)
                           OR set KOTAK_TOTP_SECRET for automated generation

Usage:
    from kotak_auth import get_kotak_client
    client = get_kotak_client()
"""
import os
import sys

try:
    from neo_api_client import NeoAPI
except ImportError:
    print(
        "neo_api_client not installed. Install with:\n"
        "pip install 'git+https://github.com/Kotak-Neo/Kotak-neo-api-v2.git"
        "@v2.0.1#egg=neo_api_client'"
    )
    sys.exit(1)


def get_totp_code() -> str:
    """
    Returns the current 6-digit TOTP code.
    Uses KOTAK_TOTP env var if set (manual mode).
    Uses KOTAK_TOTP_SECRET + pyotp for automated generation if available.
    """
    manual = os.environ.get("KOTAK_TOTP")
    if manual:
        return manual

    secret = os.environ.get("KOTAK_TOTP_SECRET")
    if secret:
        try:
            import pyotp
            return pyotp.TOTP(secret).now()
        except ImportError:
            raise ImportError(
                "Set KOTAK_TOTP env var with the current code, or "
                "install pyotp (`pip install pyotp`) and set "
                "KOTAK_TOTP_SECRET for automated TOTP generation."
            )

    raise ValueError(
        "Set either KOTAK_TOTP (current 6-digit code) or "
        "KOTAK_TOTP_SECRET (your TOTP secret key for automated generation)."
    )


def get_kotak_client() -> NeoAPI:
    """
    Returns an authenticated Kotak Neo client.
    Performs full TOTP login + MPIN validation.
    Call this once per session (tokens are session-scoped).
    """
    consumer_key = os.environ.get("KOTAK_CONSUMER_KEY")
    if not consumer_key:
        raise ValueError("Set KOTAK_CONSUMER_KEY env var.")

    client = NeoAPI(
        environment="prod",
        access_token=None,
        neo_fin_key=None,
        consumer_key=consumer_key,
    )

    mobile = os.environ.get("KOTAK_MOBILE")
    ucc = os.environ.get("KOTAK_UCC")
    mpin = os.environ.get("KOTAK_MPIN")

    if not all([mobile, ucc, mpin]):
        raise ValueError(
            "Set KOTAK_MOBILE, KOTAK_UCC, KOTAK_MPIN env vars."
        )

    totp = get_totp_code()

    # Step 1: TOTP login (generates view token + session ID)
    resp1 = client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
    if isinstance(resp1, dict) and resp1.get("data", {}).get("token") is None:
        raise RuntimeError(f"TOTP login failed: {resp1}")

    # Step 2: MPIN validation (generates trade token)
    resp2 = client.totp_validate(mpin=mpin)
    if isinstance(resp2, dict) and "error" in str(resp2).lower():
        raise RuntimeError(f"MPIN validation failed: {resp2}")

    return client
