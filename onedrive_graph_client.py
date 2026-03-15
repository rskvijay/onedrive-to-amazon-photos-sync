"""Microsoft Graph API client for OneDrive."""

import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

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
