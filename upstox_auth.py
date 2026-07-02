"""
upstox_auth.py
--------------
Upstox OAuth2 authentication.

Two modes:

1. MANUAL (default, for first-time setup and debugging):
   - Generates the login URL, opens browser
   - You log in, copy the redirected URL, paste back
   - Saves access token to ./upstox_token.txt

2. ANALYTICS TOKEN (recommended for backtesting):
   Upstox offers a long-lived "Analytics Token" with 1-year validity,
   specifically for read-only market data access, NO daily re-login needed.
   See: https://upstox.com/developer/api-documentation/announcements/
   This is the right token type for an unattended historical backfill.
   Pass it via UPSTOX_ACCESS_TOKEN env var or upstox_token.txt.

Usage:
    # First time: run this script to generate a token
    python upstox_auth.py

    # Subsequent use (in other scripts):
    from upstox_auth import get_upstox_client
    api_client = get_upstox_client()
"""
import os
import webbrowser
from pathlib import Path
import upstox_client

TOKEN_FILE = Path(os.environ.get("UPSTOX_TOKEN_FILE", "./upstox_token.txt"))

UPSTOX_AUTH_URL = "https://api.upstox.com/v2/login/authorization/dialog"
UPSTOX_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"


def get_upstox_client() -> upstox_client.ApiClient:
    """
    Returns a configured ApiClient using the saved access token.
    Works with both daily OAuth tokens and long-lived Analytics tokens.
    """
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token and TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text().strip()
    if not token:
        raise ValueError(
            f"No Upstox access token found. Set UPSTOX_ACCESS_TOKEN env var "
            f"or run upstox_auth.py to generate one. "
            f"For unattended backfills, use the Analytics Token (1-year validity) "
            f"from your Upstox developer dashboard."
        )
    config = upstox_client.Configuration()
    config.access_token = token
    return upstox_client.ApiClient(config)


def generate_token_interactive():
    """
    Full manual OAuth flow. Run once to get a token.
    For long-running backfills, get an Analytics Token instead.
    """
    client_id = os.environ.get("UPSTOX_CLIENT_ID")
    client_secret = os.environ.get("UPSTOX_CLIENT_SECRET")
    redirect_uri = os.environ.get("UPSTOX_REDIRECT_URI", "https://google.com/")

    if not client_id or not client_secret:
        raise ValueError(
            "Set UPSTOX_CLIENT_ID and UPSTOX_CLIENT_SECRET env vars. "
            "Get these from https://account.upstox.com/developer/apps"
        )

    auth_url = (
        f"{UPSTOX_AUTH_URL}"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
    )
    print(f"Opening:\n{auth_url}\n")
    webbrowser.open(auth_url, new=1)

    pasted = input(
        "After logging in, paste the FULL redirected URL:\n> "
    ).strip()

    if "code=" in pasted:
        auth_code = pasted.split("code=")[1].split("&")[0]
    else:
        auth_code = pasted

    import requests, hashlib
    resp = requests.post(UPSTOX_TOKEN_URL, data={
        "code": auth_code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    })
    data = resp.json()
    if "access_token" not in data:
        print(f"Token exchange failed: {data}")
        return
    TOKEN_FILE.write_text(data["access_token"])
    print(f"Token saved to {TOKEN_FILE}")
    if "expires_in" in data:
        print(f"Expires in: {data['expires_in']} seconds")


if __name__ == "__main__":
    generate_token_interactive()
