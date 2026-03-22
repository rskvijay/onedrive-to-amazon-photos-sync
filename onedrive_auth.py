"""Microsoft Graph authentication for OneDrive (device code flow)."""
from __future__ import annotations

import os
from pathlib import Path
from threading import Lock

import msal

# Microsoft Graph delegated scopes for signed-in user OneDrive access
DELEGATED_SCOPES = ["User.Read", "Files.Read"]
AUTHORITY = "https://login.microsoftonline.com/common"

# Token cache in project directory
_CACHE_PATH = Path(__file__).resolve().parent / ".onedrive_token_cache.bin"

# Serialize token refresh (MSAL + disk cache) when many threads hit 401 at once
_token_lock = Lock()


def get_client_id() -> str:
    """Read Azure app client ID from environment."""
    client_id = os.environ.get("ONEDRIVE_CLIENT_ID") or os.environ.get("AZURE_CLIENT_ID")
    if not client_id:
        raise SystemExit(
            "Missing ONEDRIVE_CLIENT_ID (or AZURE_CLIENT_ID). "
            "Set it in the environment or in a .env file (see .env.example)."
        )
    return client_id.strip()


def build_app(client_id: str) -> msal.PublicClientApplication:
    """Build MSAL public client with token cache."""
    return msal.PublicClientApplication(
        client_id=client_id,
        authority=AUTHORITY,
        token_cache=msal.SerializableTokenCache(),
    )


def load_cache(app: msal.PublicClientApplication) -> None:
    """Load token cache from file if it exists."""
    if _CACHE_PATH.exists():
        with open(_CACHE_PATH, "r", encoding="utf-8") as f:
            app.token_cache.deserialize(f.read())


def save_cache(app: msal.PublicClientApplication) -> None:
    """Persist token cache to file."""
    if app.token_cache.has_state_changed:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_PATH, "w", encoding="utf-8") as f:
            f.write(app.token_cache.serialize())


def get_access_token() -> str:
    """
    Obtain an access token for Microsoft Graph using device code flow.

    Uses MSAL's token cache: ``acquire_token_silent`` returns a valid access token and
    refreshes it with the refresh token when the previous access token has expired.

    **Call this again** after long runs (e.g. >~1 hour) or on HTTP 401 from Graph — do not
    hold a single access token string for the entire process lifetime.
    """
    with _token_lock:
        client_id = get_client_id()
        app = build_app(client_id)
        load_cache(app)

        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(DELEGATED_SCOPES, account=accounts[0])
            if result:
                save_cache(app)
                return result["access_token"]

        flow = app.initiate_device_flow(scopes=DELEGATED_SCOPES)
        if "message" not in flow:
            raise SystemExit(f"Failed to create device flow: {flow.get('error_description', flow)}")

        print(flow["message"])
        result = app.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            raise SystemExit(
                f"Authentication failed: {result.get('error_description', result)}"
            )

        save_cache(app)
        return result["access_token"]
