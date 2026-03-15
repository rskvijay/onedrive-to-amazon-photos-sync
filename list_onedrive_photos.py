#!/usr/bin/env python3
"""
List OneDrive Photos and write to CSV.

Use --list-onedrive-photos with --csv PATH to enumerate all files (folders excluded).
CSV columns: id, file_name (absolute path), size_human, size_bytes, created_date, last_modified_date.
"""
from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from formatters import format_date_iso, format_size
from onedrive_auth import get_access_token
from onedrive_graph_client import list_children_by_id, list_photos_children

# OneDrive CSV columns (files only; no md5 or content_date in Graph API)
ONEDRIVE_CSV_HEADER = [
    "id",
    "file_name",
    "size_human",
    "size_bytes",
    "created_date",
    "last_modified_date",
]


def _build_row(item: dict, absolute_path: str) -> list:
    """Build a CSV row for one file. file_name is the absolute path."""
    size = item.get("size")
    if size is not None:
        try:
            size_int = int(size)
        except (TypeError, ValueError):
            size_int = None
    else:
        size_int = None
    size_human = format_size(size_int) if size_int is not None else "0"
    size_bytes = str(size_int) if size_int is not None else ""

    created = format_date_iso(item.get("createdDateTime"))
    last_modified = format_date_iso(item.get("lastModifiedDateTime"))

    return [
        item.get("id", ""),
        absolute_path,
        size_human,
        size_bytes,
        created,
        last_modified,
    ]


def _count_files(access_token: str, max_workers: int) -> int:
    """Return total file count under OneDrive Photos (excludes folders)."""
    items = list_photos_children(access_token, top=999)
    if not items:
        return 0
    count: list[int] = [0]
    all_futures: list = []
    futures_lock = Lock()

    def _count_folder(
        token: str,
        folder_id: str,
        executor: ThreadPoolExecutor,
        cnt: list[int],
        futures: list,
        flock: Lock,
    ) -> None:
        children = list_children_by_id(token, folder_id)
        for item in children:
            if not item.get("folder"):
                cnt[0] += 1
            if item.get("folder"):
                fut = executor.submit(
                    _count_folder,
                    token,
                    item["id"],
                    executor,
                    cnt,
                    futures,
                    flock,
                )
                with flock:
                    futures.append(fut)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for item in items:
            if not item.get("folder"):
                count[0] += 1
            if item.get("folder"):
                fut = executor.submit(
                    _count_folder,
                    access_token,
                    item["id"],
                    executor,
                    count,
                    all_futures,
                    futures_lock,
                )
                all_futures.append(fut)
        while True:
            with futures_lock:
                pending = [f for f in all_futures if not f.done()]
            if not pending:
                break
            for f in pending:
                f.result()
    return count[0]


def _list_photos_recursive(
    access_token: str,
    folder_id: str,
    path_prefix: str,
    rows: list[list],
    write_lock: Lock,
    executor: ThreadPoolExecutor,
    item_count: list[int],
    futures: list,
    futures_lock: Lock,
) -> None:
    """Recursively list items; append rows only for files, with file_name as absolute path."""
    children = list_children_by_id(access_token, folder_id)
    for item in children:
        name = item.get("name", "")
        absolute_path = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
        if not item.get("folder"):
            row = _build_row(item, absolute_path)
            with write_lock:
                rows.append(row)
                item_count[0] += 1
        if item.get("folder"):
            f = executor.submit(
                _list_photos_recursive,
                access_token,
                item["id"],
                absolute_path,
                rows,
                write_lock,
                executor,
                item_count,
                futures,
                futures_lock,
            )
            with futures_lock:
                futures.append(f)


def run_list_onedrive_photos(csv_path: Path | str, threads: int = 16) -> None:
    """Enumerate all items from OneDrive Photos and write CSV to csv_path."""
    import time

    from rich.console import Console
    from rich.progress import Progress

    console = Console()
    csv_path = Path(csv_path)

    with console.status("Authenticating with OneDrive..."):
        token = get_access_token()

    with console.status("Counting OneDrive Photos files..."):
        total_expected = _count_files(token, threads)

    if total_expected == 0:
        console.print("[yellow]No items found in OneDrive Photos.[/yellow]")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(ONEDRIVE_CSV_HEADER)
        return

    start = time.perf_counter()
    rows: list[list] = []
    write_lock = Lock()
    futures_lock = Lock()
    item_count = [0]
    futures: list = []

    with Progress(console=console) as progress:
        task = progress.add_task(
            "[cyan]Listing OneDrive Photos[/cyan]",
            total=total_expected,
        )
        with ThreadPoolExecutor(max_workers=threads) as executor:
            items = list_photos_children(token, top=999)
            for item in items:
                name = item.get("name", "")
                absolute_path = f"/{name}"
                if not item.get("folder"):
                    row = _build_row(item, absolute_path)
                    with write_lock:
                        rows.append(row)
                        item_count[0] += 1
                        progress.update(task, completed=min(item_count[0], total_expected))
                if item.get("folder"):
                    f = executor.submit(
                        _list_photos_recursive,
                        token,
                        item["id"],
                        absolute_path,
                        rows,
                        write_lock,
                        executor,
                        item_count,
                        futures,
                        futures_lock,
                    )
                    with futures_lock:
                        futures.append(f)
            while True:
                with futures_lock:
                    pending = [f for f in futures if not f.done()]
                if not pending:
                    break
                n = item_count[0]
                progress.update(task, completed=min(n, total_expected))
                for f in pending:
                    f.result()

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ONEDRIVE_CSV_HEADER)
        w.writerows(rows)

    elapsed = time.perf_counter() - start
    console.print(f"[green]Wrote {len(rows)} items to [bold]{csv_path}[/bold].[/green]")
    # Reuse same elapsed format as Amazon
    n = int(round(elapsed))
    if n < 60:
        elapsed_str = f"{n}s"
    elif n < 3600:
        m, s = divmod(n, 60)
        elapsed_str = f"{m}m {s}s"
    else:
        h, r = divmod(n, 3600)
        m, s = divmod(r, 60)
        elapsed_str = f"{h}h {m}m {s}s"
    console.print(f"[dim]Time elapsed: {elapsed_str}[/dim]")
