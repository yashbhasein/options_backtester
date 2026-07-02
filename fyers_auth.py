"""
fyers_auth.py
--------------
Thin wrapper around fyers_apiv3 auth so other scripts just do:

    from fyers_auth import get_fyers_client
    fyers = get_fyers_client()

Fyers uses an OAuth-like flow: you need a one-time browser login to get an
auth_code, exchange it for an access_token, and that access_token is valid
for the trading day (Fyers invalidates it daily, typically early morning).

For a script that runs unattended every trading day (this collector), you
have two options:
  1. Re-run the manual browser login each morning before market open and
     paste the fresh token (fine while you're hands-on and testing).
  2. Automate token refresh (Fyers' v3 SDK does NOT support silent refresh
     without a fresh login -- there is no long-lived refresh token in the
     way some other brokers offer it). Most people solve this by scripting
     the login redirect with Selenium/Playwright once a day, or by manually
     refreshing each morning if running this only occasionally.

This module assumes you already have a valid access_token saved in a local
file (fyers_token.txt) or environment variable FYERS_ACCESS_TOKEN, and just
constructs the client. It does NOT do the browser login flow -- run
generate_fyers_token.py once (separately) to get the token.
"""

import os
from pathlib import Path

from fyers_apiv3 import fyersModel

TOKEN_FILE = Path(os.environ.get("FYERS_TOKEN_FILE", "./fyers_token.txt"))


def get_fyers_client(client_id: str = None) -> fyersModel.FyersModel:
    client_id = client_id or os.environ.get("FYERS_CLIENT_ID")
    if not client_id:
        raise ValueError(
            "Set FYERS_CLIENT_ID env var (your Fyers app_id, e.g. 'XXXXX-100')."
        )

    token = os.environ.get("FYERS_ACCESS_TOKEN")
    if not token and TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text().strip()

    if not token:
        raise ValueError(
            f"No Fyers access token found. Set FYERS_ACCESS_TOKEN env var, "
            f"or write it to {TOKEN_FILE}. Run generate_fyers_token.py first "
            f"if you haven't completed the login flow today -- Fyers tokens "
            f"expire daily."
        )

    return fyersModel.FyersModel(client_id=client_id, token=token, is_async=False)
