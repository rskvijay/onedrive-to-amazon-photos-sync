"""Human-readable formatting for size and dates."""

from datetime import datetime


def format_size(num_bytes) -> str:
    """Format bytes as human-readable e.g. 1024 -> '1K', 5242880 -> '5M'. Returns '0' for missing."""
    if num_bytes is None or (isinstance(num_bytes, float) and num_bytes != num_bytes):
        return "0"
    try:
        n = int(float(num_bytes))
    except (TypeError, ValueError):
        return "0"
    if n <= 0:
        return "0"
    for unit in ("B", "K", "M", "G", "T"):
        if n < 1024:
            if unit == "B":
                return f"{n}B"
            return f"{n:.1f}{unit}".rstrip("0").rstrip(".")
        n /= 1024
    return f"{n:.1f}T".rstrip("0").rstrip(".")


def format_date_iso(iso_str: str | None) -> str:
    """Format ISO 8601 string from API as 'Jan 21 2017 14:30'. Returns '' for missing or invalid."""
    if not iso_str:
        return ""
    s = (iso_str[:19] or "").replace("T", " ")
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%b %d %Y %H:%M")
    except ValueError:
        return ""
