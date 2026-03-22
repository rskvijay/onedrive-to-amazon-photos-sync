#!/usr/bin/env python3
"""
List OneDrive items that are not in Amazon Photos (by md5).

Downloads each OneDrive file to --download-dir, computes md5, and compares to Amazon index.
Files already in Amazon are removed from disk; only missing files are kept for Phase 2 upload.
The missing report CSV is appended row-by-row as each missing file is determined (not only at the end).
"""
from __future__ import annotations

import csv
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

from list_amazon_photos import (
    _column_attrs,
    _format_elapsed,
    _format_hash_val,
    _query_parallel,
    console,
    get_amazon_client,
)
from list_onedrive_photos import collect_onedrive_file_items
from onedrive_auth import get_access_token
from onedrive_graph_client import download_item_content

MISSING_CSV_HEADER = [
    "local_path",
    "file_path",
    "file_name",
    "content_date",
    "md5_hash",
    "size_bytes",
]


def _local_path_for_download(download_dir: Path, file_name: str) -> Path:
    """Return a path under download_dir for file_name, disambiguating with _2, _3, ... if the name already exists."""
    path = download_dir / file_name
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    n = 2
    while True:
        path = download_dir / f"{stem}_{n}{suffix}"
        if not path.exists():
            return path
        n += 1


def _build_amazon_md5_map(
    amazon_csv: Path | None,
    threads: int,
) -> tuple[set[str], dict[str, str]]:
    """Build set of Amazon md5 hashes and map md5 -> file_name (for logging). From CSV or API."""
    md5_set: set[str] = set()
    md5_to_name: dict[str, str] = {}
    if amazon_csv is not None and amazon_csv.exists():
        with open(amazon_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                h = _format_hash_val(row.get("md5_hash"))
                if h:
                    md5_set.add(h)
                    if h not in md5_to_name:
                        md5_to_name[h] = row.get("file_name", "")
        return md5_set, md5_to_name
    ap = get_amazon_client(console)
    with console.status("Fetching Amazon Photos index (for md5)..."):
        nodes = _query_parallel(ap, "type:(PHOTOS OR VIDEOS)", max_workers=threads)
    if nodes is not None and len(nodes):
        id_col, name_col, md5_col, *_ = _column_attrs(nodes)
        for _, row in nodes.iterrows():
            h = _format_hash_val(row.get(md5_col)) if md5_col else ""
            if h:
                md5_set.add(h)
                if h not in md5_to_name and name_col:
                    md5_to_name[h] = str(row.get(name_col, ""))
    return md5_set, md5_to_name


def run_list_missing(
    amazon_csv: Path | None = None,
    csv_path: Path | None = None,
    download_dir: Path | None = None,
    threads: int = 16,
) -> None:
    """List OneDrive items that are not in Amazon Photos (by md5). Downloads to download_dir; keeps only missing files. Uses OneDrive file name only (flat); disambiguates with _2, _3, ... on collision via _local_path_for_download. Optionally use --amazon-csv to skip API fetch; --csv for report path."""
    if download_dir is None or csv_path is None:
        return
    csv_path = Path(csv_path)
    download_dir = Path(download_dir)
    if amazon_csv is not None:
        amazon_csv = Path(amazon_csv)
    download_dir.mkdir(parents=True, exist_ok=True)

    console.print("[dim]Building Amazon Photos index...[/dim]")
    amazon_md5_set, amazon_md5_to_name = _build_amazon_md5_map(amazon_csv, threads)
    console.print(f"[dim]Amazon Photos index: {len(amazon_md5_set)} unique md5 hashes.[/dim]")

    with console.status("Authenticating with OneDrive..."):
        access_token = get_access_token()
    with console.status("Listing OneDrive Photos files..."):
        onedrive_items = collect_onedrive_file_items(access_token, threads=threads)
    if not onedrive_items:
        console.print("[yellow]No files found in OneDrive Photos.[/yellow]")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(MISSING_CSV_HEADER)
        return

    for item in onedrive_items:
        item["local_path"] = _local_path_for_download(download_dir, item["name"])

    print_lock = Lock()
    total_files = len(onedrive_items)
    processed_count = [0]  # mutable so process_one can increment under lock

    def process_one(item: dict) -> tuple[str, list | None]:
        local_path = item["local_path"]
        file_path = item["file_path"]
        try:
            content = download_item_content(access_token, item["id"])
        except Exception as e:
            with print_lock:
                processed_count[0] += 1
                console.print(f"[dim]{processed_count[0]}/{total_files}[/dim]:  [red]Failed to download {file_path}: {e}[/red]")
            return "error", None
        md5_hash = hashlib.md5(content).hexdigest()
        with open(local_path, "wb") as f:
            f.write(content)
        if md5_hash in amazon_md5_set:
            local_path.unlink(missing_ok=True)
            amazon_name = amazon_md5_to_name.get(md5_hash, "(unknown)")
            with print_lock:
                processed_count[0] += 1
                console.print(
                    f"[dim]{processed_count[0]}/{total_files}[/dim]:  File [cyan]{file_path}[/cyan] in OneDrive has md5 [dim]{md5_hash}[/dim] and is present in Amazon Photos as [green]{amazon_name}[/green]."
                )
            return "present", None
        row = [
            str(local_path),
            file_path,
            item["name"],
            item["content_date"],
            md5_hash,
            item["size_bytes"],
        ]
        with print_lock:
            processed_count[0] += 1
            console.print(
                f"[dim]{processed_count[0]}/{total_files}[/dim]:  File [cyan]{file_path}[/cyan] in OneDrive has md5 [dim]{md5_hash}[/dim] and is [yellow]missing from[/yellow] Amazon Photos (kept at [dim]{local_path}[/dim])."
            )
        return "missing", row

    console.print(f"[dim]Downloading and comparing {total_files} OneDrive files (parallelism={threads})...[/dim]\n")
    start = time.perf_counter()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    missing_count = 0
    # Write header first, then append one row per missing file as downloads complete (crash-safe progress).
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_f:
        csv_writer = csv.writer(csv_f)
        csv_writer.writerow(MISSING_CSV_HEADER)
        csv_f.flush()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(process_one, item): item for item in onedrive_items}
            for future in as_completed(futures):
                status, row = future.result()
                if status == "missing" and row is not None:
                    csv_writer.writerow(row)
                    csv_f.flush()
                    missing_count += 1

    elapsed = time.perf_counter() - start
    console.print(f"\n[green]Done.[/green] [bold]{missing_count}[/bold] file(s) missing from Amazon Photos (saved to [bold]{csv_path}[/bold], files kept in [bold]{download_dir}[/bold]). [dim]Time: {_format_elapsed(elapsed)}[/dim]")
