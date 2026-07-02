"""
generate_fyers_token.py
-------------------------
Run this ONCE per trading day (Fyers access tokens expire daily, usually
overnight) before running fyers_live_collector.py.

This performs the standard Fyers v3 OAuth flow:
  1. Opens the Fyers login URL in your browser
  2. You log in, Fyers redirects to your configured redirect_uri with an
     `auth_code` query param
  3. Paste that full redirected URL (or just the auth_code) back here
  4. Script exchanges it for an access_token and saves it to fyers_token.txt

You need FYERS_CLIENT_ID, FYERS_SECRET_KEY, FYERS_REDIRECT_URI set as env
vars, matching what you registered at https://myapi.fyers.in/dashboard/.

Usage:
    export FYERS_CLIENT_ID="XXXXX-100"
    export FYERS_SECRET_KEY="your_secret"
    export FYERS_REDIRECT_URI="https://your-redirect-uri.com"
    python generate_fyers_token.py
"""

import os
import webbrowser
from pathlib import Path

from fyers_apiv3 import fyersModel

TOKEN_FILE = Path(os.environ.get("FYERS_TOKEN_FILE", "./fyers_token.txt"))


def main():
    client_id = os.environ["FYERS_CLIENT_ID"]
    secret_key = os.environ["FYERS_SECRET_KEY"]
    redirect_uri = os.environ["FYERS_REDIRECT_URI"]

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code",
    )

    auth_url = session.generate_authcode()
    print(f"Opening login URL:\n{auth_url}\n")
    webbrowser.open(auth_url, new=1)

    pasted = input(
        "After logging in, paste the FULL redirected URL (or just the "
        "auth_code value if you can isolate it):\n> "
    ).strip()

    if "auth_code=" in pasted:
        auth_code = pasted.split("auth_code=")[1].split("&")[0]
    else:
        auth_code = pasted  # assume they pasted the raw code

    session.set_token(auth_code)
    response = session.generate_token()

    if "access_token" not in response:
        print(f"Token exchange failed. Full response: {response}")
        return

    access_token = response["access_token"]
    TOKEN_FILE.write_text(access_token)
    print(f"Saved access token to {TOKEN_FILE} (valid for today's session).")


if __name__ == "__main__":
    main()
