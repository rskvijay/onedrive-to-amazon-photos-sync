"""Microsoft Graph API client for OneDrive."""

import socket
import time

import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Retries for download_item_content
# 429: Graph throttling — each request may wait Retry-After (or escalating backoff). With --threads N,
# many parallel requests can still exhaust retries; lower N if 429 persists.
DOWNLOAD_MAX_RETRIES = 8  # attempts per file (429 + 5xx + connection share this loop)
DOWNLOAD_RETRY_BASE_DELAY = 2  # exponential backoff for connection/5xx
DOWNLOAD_RETRY_429_DEFAULT_DELAY = 60  # base seconds when Retry-After header is missing
DOWNLOAD_RETRY_429_MAX_DELAY = 300  # cap for missing Retry-After (5 min)
# DNS / resolver blips: longer waits than generic connection errors (seconds, capped)
DOWNLOAD_RETRY_DNS_BASE_DELAY = 5
DOWNLOAD_RETRY_DNS_MAX_DELAY = 120

_ITEM_FIELDS = "id,name,size,createdDateTime,lastModifiedDateTime,folder,file,photo,webUrl"


def _list_children(
    access_token: str,
    url: str,
    params: dict | None,
    order_by_created: bool = False,
) -> list:
    """Fetch all children from a folder URL with pagination."""
    headers = {"Authorization": f"Bearer {access_token}"}
    out = []
    while url:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        out.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        params = None
    if order_by_created:
        out.sort(key=lambda i: i.get("createdDateTime") or "", reverse=True)
    return out


def list_children_by_id(access_token: str, folder_id: str, top: int = 999) -> list:
    """List direct children of a folder by its drive item id."""
    url = f"{GRAPH_BASE}/me/drive/items/{folder_id}/children"
    params = {"$select": _ITEM_FIELDS, "$top": top}
    return _list_children(access_token, url, params)


def list_photos_children(access_token: str, top: int = 999) -> list:
    """
    List items in the user's OneDrive Photos folder (special folder).
    Returns drive items with id, name, size, createdDateTime, etc.
    """
    url = f"{GRAPH_BASE}/me/drive/special/photos/children"
    params = {"$select": _ITEM_FIELDS, "$top": top}
    return _list_children(access_token, url, params, order_by_created=True)


def _exception_chain(exc: BaseException) -> list[BaseException]:
    """Walk __cause__ / __context__ for nested urllib3/requests errors."""
    seen: set[int] = set()
    out: list[BaseException] = []
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        out.append(cur)
        cur = cur.__cause__ or cur.__context__
    return out


def _looks_like_dns_failure(exc: BaseException) -> bool:
    """True if failure is likely DNS / name resolution (transient on many networks)."""
    for e in _exception_chain(exc):
        if isinstance(e, socket.gaierror):
            return True
        if "Failed to resolve" in str(e) or "nodename nor servname" in str(e).lower():
            return True
        try:
            from urllib3.exceptions import NameResolutionError

            if isinstance(e, NameResolutionError):
                return True
        except ImportError:
            pass
        tname = type(e).__name__
        if "NameResolution" in tname or "gaierror" in tname.lower():
            return True
    return False


def _is_retryable_error(exc: BaseException) -> bool:
    """True for connection/network errors and timeouts that are worth retrying."""
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == 54:  # Connection reset by peer (macOS)
        return True
    if _looks_like_dns_failure(exc):
        return True
    return False


def _delay_for_connection_error(exc: BaseException, attempt: int) -> int:
    """Shorter backoff for generic connection issues; longer for DNS-like failures."""
    if _looks_like_dns_failure(exc):
        delay = DOWNLOAD_RETRY_DNS_BASE_DELAY * (2 ** min(attempt, 5))
        return min(int(delay), DOWNLOAD_RETRY_DNS_MAX_DELAY)
    return DOWNLOAD_RETRY_BASE_DELAY ** (attempt + 1)


def _delay_for_429(resp: requests.Response, attempt: int = 0) -> int:
    """
    Return wait time in seconds for a 429 response.
    Prefer Retry-After header; otherwise escalating backoff when header is absent (Graph often omits it).
    """
    value = resp.headers.get("Retry-After")
    if value:
        value = value.strip()
        try:
            return max(1, int(value))  # seconds
        except ValueError:
            pass
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(value)
            delta = dt.timestamp() - time.time()
            return max(1, int(delta))
        except Exception:
            pass
    # No usable Retry-After: escalate so sustained throttling gets longer waits (capped)
    delay = DOWNLOAD_RETRY_429_DEFAULT_DELAY * (2 ** min(attempt, 4))
    return min(int(delay), DOWNLOAD_RETRY_429_MAX_DELAY)


def download_item_content(access_token: str, item_id: str) -> bytes:
    """
    Download full content of a drive item by id. Returns raw bytes.

    Retries on connection/timeout/429. On **401 Unauthorized**, calls ``get_access_token()``
    again so MSAL can refresh the OAuth access token (they expire ~1h; long runs must not
    reuse a single token string).
    """
    url = f"{GRAPH_BASE}/me/drive/items/{item_id}/content"
    token = access_token
    last_exc: BaseException | None = None
    for attempt in range(DOWNLOAD_MAX_RETRIES):
        try:
            headers = {"Authorization": f"Bearer {token}"}
            resp = requests.get(url, headers=headers, timeout=60, stream=True)
            if resp.status_code == 401:
                last_exc = requests.exceptions.HTTPError(
                    f"401 Unauthorized (attempt {attempt + 1}/{DOWNLOAD_MAX_RETRIES})"
                )
                if attempt < DOWNLOAD_MAX_RETRIES - 1:
                    from onedrive_auth import get_access_token

                    token = get_access_token()
                    continue
                resp.raise_for_status()
            if resp.status_code == 429:
                last_exc = requests.exceptions.HTTPError(f"429 Too Many Requests (attempt {attempt + 1}/{DOWNLOAD_MAX_RETRIES})")
                if attempt < DOWNLOAD_MAX_RETRIES - 1:
                    delay = _delay_for_429(resp, attempt)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
            if resp.status_code in (500, 502, 503, 504):
                last_exc = requests.exceptions.HTTPError(f"HTTP {resp.status_code}")
                if attempt < DOWNLOAD_MAX_RETRIES - 1:
                    delay = DOWNLOAD_RETRY_BASE_DELAY ** (attempt + 1)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_exc = e
            if _is_retryable_error(e) and attempt < DOWNLOAD_MAX_RETRIES - 1:
                delay = _delay_for_connection_error(e, attempt)
                time.sleep(delay)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("download_item_content failed after retries")
