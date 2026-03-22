"""Amazon Photos API client from environment credentials."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import typer

# Lazy import to avoid loading amazon_photos until needed
# (and to allow patching before first use)


def reload_project_dotenv() -> None:
    """
    Reload ``.env`` from the project directory (next to this file) with ``override=True``.

    Use this before reading env-dependent options, or so fresh ``AMAZON_*`` cookies saved to
    ``.env`` during a long run (e.g. after staging) are visible in ``os.environ``.
    """
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(env_path, override=True)
    except Exception:
        pass


def get_amazon_client(console):
    """
    Build AmazonPhotos client from environment variables. Exits with error if auth missing.

    Always reloads project ``.env`` from disk first so updated ``AMAZON_*`` values take effect.
    """
    reload_project_dotenv()

    session_id = os.environ.get("AMAZON_SESSION_ID")
    ubid = os.environ.get("AMAZON_UBID_MAIN") or os.environ.get("AMAZON_UBID_ACBCA")
    at = os.environ.get("AMAZON_AT_MAIN") or os.environ.get("AMAZON_AT_ACBCA")

    # Strip whitespace (common when copying from DevTools)
    session_id = (session_id or "").strip()
    ubid = (ubid or "").strip()
    at = (at or "").strip()

    if not all((session_id, ubid, at)):
        console.print(
            "[red]Missing Amazon auth. Set env vars:[/red]\n"
            "  [dim]US:[/dim] AMAZON_SESSION_ID, AMAZON_UBID_MAIN, AMAZON_AT_MAIN\n"
            "  [dim]Canada:[/dim] AMAZON_SESSION_ID, AMAZON_UBID_ACBCA, AMAZON_AT_ACBCA\n\n"
            "Log in at https://www.amazon.com/photos/ and copy cookies from DevTools (Application → Cookies for www.amazon.com)."
        )
        raise typer.Exit(1)

    cookie_key_ubid = "ubid_main" if os.environ.get("AMAZON_UBID_MAIN") else "ubid-acbca"
    cookie_key_at = "at_main" if os.environ.get("AMAZON_AT_MAIN") else "at-acbca"

    from amazon_photos import AmazonPhotos
    from amazon_photos import _api as _ap

    # Fix from PR #27: accept both _main and -main cookie keys for TLD
    def _patched_tld(self, cookies):
        for k, v in cookies.items():
            if k.endswith("_main") or k.endswith("-main"):
                return "com"
            if k.startswith(x := "at-acb"):
                return k.split(x)[-1]
    _ap.AmazonPhotos.determine_tld = _patched_tld

    # Subclass that skips the initial full-library fetch when the DB is missing.
    # We only need query(); the default load_db() would fetch everything twice (init + our call).
    class _ListOnlyClient(AmazonPhotos):
        def load_db(self, **kwargs):
            if self.db_path.name and self.db_path.exists():
                try:
                    from amazon_photos._helpers import format_nodes
                    return format_nodes(pd.read_parquet(self.db_path, **kwargs))
                except Exception:
                    pass
            return pd.DataFrame()

    cookies_dict = {
        "session-id": session_id,
        cookie_key_ubid: ubid,
        cookie_key_at: at,
    }
    # Also send hyphen form for US (amazon.com) so the drive API accepts them
    if os.environ.get("AMAZON_UBID_MAIN"):
        cookies_dict["ubid-main"] = ubid
        cookies_dict["at-main"] = at

    return _ListOnlyClient(cookies=cookies_dict)
