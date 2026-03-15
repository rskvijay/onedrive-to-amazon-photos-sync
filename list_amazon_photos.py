#!/usr/bin/env python3
"""
List Amazon Photos library and optionally write to CSV.

Use --list-amazon-photos to enumerate all items (photos and videos).
Use --csv PATH to save the output to a CSV file (required when using --list-amazon-photos).
"""
from __future__ import annotations

import csv
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.progress import Progress, TaskID

from client import get_amazon_client
from formatters import format_size

# Page size for Amazon Photos search API (must match library constant)
SEARCH_PAGE_SIZE = 200

logging.getLogger("amazon_photos").setLevel(logging.CRITICAL)
logging.getLogger("my_logger").setLevel(logging.CRITICAL)

app = typer.Typer(help="OneDrive to Amazon Photos sync — list Amazon Photos, compare with OneDrive")
console = Console()

CSV_HEADER = [
    "id",
    "file_name",
    "size_human",
    "size_bytes",
    "md5_hash",
    "content_date",
    "created_date",
    "last_modified_date",
]


def _format_hash_val(v):
    """Return non-empty string hash or empty string if missing/invalid."""
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return str(v).strip()


def _row_from_node(row, id_col, name_col, md5_col, size_col, created_col, modified_col, content_date_col):
    """Build a CSV row from a node (DataFrame row)."""
    id_val = str(row.get(id_col, "")) if id_col else ""
    name_val = str(row.get(name_col, "")) if name_col else ""

    md5_hash_val = _format_hash_val(row.get(md5_col)) if md5_col else ""

    s = row.get(size_col) if size_col else None
    if s is None or (isinstance(s, float) and s != s):
        size_bytes_val = ""
        size_human = "0"
    else:
        try:
            size_bytes_val = str(int(s)) if isinstance(s, (int, float)) else str(s)
            size_human = format_size(s)
        except (TypeError, ValueError):
            size_bytes_val = ""
            size_human = "0"

    def _format_date(val):
        if val is None or (isinstance(val, float) and val != val):
            return ""
        try:
            dt = pd.to_datetime(val)
            if pd.isna(dt):
                return ""
            return dt.strftime("%b %d %Y %H:%M")
        except (TypeError, ValueError):
            return str(val) if val else ""

    content_date_val = _format_date(row.get(content_date_col)) if content_date_col else ""
    created_val = _format_date(row.get(created_col)) if created_col else ""
    modified_val = _format_date(row.get(modified_col)) if modified_col else ""

    return [id_val, name_val, size_human, size_bytes_val, md5_hash_val, content_date_val, created_val, modified_val]


def _column_attrs(nodes):
    id_col = "id" if "id" in nodes.columns else None
    name_col = "name" if "name" in nodes.columns else (nodes.columns[0] if len(nodes.columns) else None)
    md5_col = "md5" if "md5" in nodes.columns else None
    size_col = "size" if "size" in nodes.columns else None
    created_col = "createdDate" if "createdDate" in nodes.columns else None
    modified_col = "modifiedDate" if "modifiedDate" in nodes.columns else None
    content_date_col = "contentDate" if "contentDate" in nodes.columns else None
    return id_col, name_col, md5_col, size_col, created_col, modified_col, content_date_col


def _format_elapsed(seconds: float) -> str:
    """Format seconds as human-readable e.g. 134 -> '2m 14s', 3661 -> '1h 1m 1s'."""
    n = int(round(seconds))
    if n < 60:
        return f"{n}s"
    if n < 3600:
        m, s = divmod(n, 60)
        return f"{m}m {s}s"
    h, r = divmod(n, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


def _query_parallel(ap, filters: str, limit: float = math.inf, max_workers: int = 16):
    """
    Run paginated search in parallel using ThreadPoolExecutor.
    Returns a DataFrame of nodes (same shape as ap.query()).
    """
    from amazon_photos._helpers import format_nodes

    url = f"{ap.drive_url}/search"
    base_params = ap.base_params | {
        "limit": SEARCH_PAGE_SIZE,
        "offset": 0,
        "filters": filters,
        "lowResThumbnail": "true",
        "searchContext": "customer",
        "sort": "['createdDate DESC']",
    }
    initial = ap.backoff(ap.client.get, url, params=base_params)
    if initial is None:
        return None
    initial = initial.json()
    res = [initial]
    total = initial["count"]
    if total <= SEARCH_PAGE_SIZE:
        return format_nodes(pd.json_normalize(initial.get("data", [])))

    end = total if math.isinf(limit) else min(total, int(limit))
    remaining_offsets = range(
        SEARCH_PAGE_SIZE,
        end,
        SEARCH_PAGE_SIZE,
    )
    remaining_offsets = list(remaining_offsets)

    def fetch_page(offset: int):
        params = base_params.copy()
        params["offset"] = offset
        r = ap.backoff(ap.client.get, url, params=params)
        return r.json() if r is not None else None

    with Progress(console=console) as progress:
        task: TaskID = progress.add_task(
            "[cyan]Search nodes[/cyan]",
            total=len(remaining_offsets),
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_page, off): off for off in remaining_offsets}
            for future in as_completed(futures):
                data = future.result()
                if data is not None:
                    res.append(data)
                progress.advance(task)

    return format_nodes(
        pd.json_normalize(y for x in res for y in x.get("data", []))
    )


def run_list_amazon_photos(csv_path: Path, threads: int = 16) -> None:
    """Enumerate all items from Amazon Photos and write CSV to csv_path."""
    start = time.perf_counter()
    ap = get_amazon_client(console)
    with console.status("Fetching all items from Amazon Photos..."):
        try:
            nodes = _query_parallel(ap, "type:(PHOTOS OR VIDEOS)", max_workers=threads)
        except Exception as e:
            console.print(
                "[red]Authentication failed — cookies expired or invalid.[/red]\n\n"
                "Get fresh cookies from [link=https://www.amazon.com/photos/]www.amazon.com/photos[/link]:\n"
                "  DevTools (F12) → Application → Cookies → [bold]www.amazon.com[/bold] → copy [bold]session-id[/bold], [bold]ubid-main[/bold], [bold]at-main[/bold]\n"
                "Then: [dim]export AMAZON_SESSION_ID=... AMAZON_UBID_MAIN=... AMAZON_AT_MAIN=...[/dim]\n\n"
                "[yellow]Tips:[/yellow] Use the exact cookie names above. If you use a different Amazon site (e.g. amazon.co.uk), "
                "set AMAZON_UBID_ACBCA and AMAZON_AT_ACBCA instead. Ensure there are no extra quotes or newlines in the values."
            )
            console.print(f"[dim]Error: {e}[/dim]")
            raise typer.Exit(1)

    if nodes is None or len(nodes) == 0:
        console.print("[yellow]No items found in Amazon Photos.[/yellow]")
        return

    if "id" in nodes.columns:
        nodes = nodes.drop_duplicates(subset=["id"])

    id_col, name_col, md5_col, size_col, created_col, modified_col, content_date_col = _column_attrs(nodes)

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for _, row in nodes.iterrows():
            writer.writerow(
                _row_from_node(
                    row, id_col, name_col, md5_col, size_col, created_col, modified_col, content_date_col
                )
            )

    elapsed = time.perf_counter() - start
    console.print(f"[green]Wrote {len(nodes)} items to [bold]{csv_path}[/bold].[/green]")
    console.print(f"[dim]Time elapsed: {_format_elapsed(elapsed)}[/dim]")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    list_amazon_photos: bool = typer.Option(
        False,
        "--list-amazon-photos",
        help="Enumerate all items from Amazon Photos (photos and videos).",
    ),
    csv_path: str | None = typer.Option(
        None,
        "--csv",
        help="Path to save CSV output (required when using --list-amazon-photos).",
        path_type=Path,
    ),
    threads: int = typer.Option(
        16,
        "--threads",
        min=1,
        max=64,
        help="Number of parallel request threads (1–64). Default 16.",
    ),
) -> None:
    """List Amazon Photos and optionally save to CSV."""
    if ctx.invoked_subcommand is not None:
        return

    if list_amazon_photos:
        if not csv_path:
            console.print(
                "[red]When using --list-amazon-photos, --csv PATH is required.[/red]\n"
                "Example: python list_amazon_photos.py --list-amazon-photos --csv amazon_photos.csv"
            )
            raise typer.Exit(1)
        run_list_amazon_photos(csv_path, threads=threads)
        return

    if csv_path:
        console.print("[yellow]--csv is ignored unless --list-amazon-photos is set.[/yellow]")
        return

    console.print("Use [bold]--list-amazon-photos --csv PATH[/bold] to enumerate Amazon Photos and save to CSV.")
    console.print("Example: python list_amazon_photos.py --list-amazon-photos --csv amazon_photos.csv")


if __name__ == "__main__":
    app()
