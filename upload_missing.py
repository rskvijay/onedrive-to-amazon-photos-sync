#!/usr/bin/env python3
"""
Upload files listed in missing.csv to Amazon Photos.

CLI flags are declared on the main Typer app; this module owns validation,
dispatch, and upload/dry-run implementation.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import random
import re
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from functools import partial
from pathlib import Path
from threading import Lock

import aiofiles
import pandas as pd
import typer
from httpx import AsyncClient
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from embed_metadata import embed_content_date

console = Console()

# Must match list_missing.MISSING_CSV_HEADER
MISSING_CSV_HEADER = [
    "local_path",
    "file_path",
    "file_name",
    "content_date",
    "md5_hash",
    "size_bytes",
]

# Re-export for list_amazon_photos.Option(help=...) so help text lives here
CLI_HELP_UPLOAD_MISSING = (
    "Upload files from missing.csv to Amazon Photos. "
    "Requires --csv and exactly one of --dry-run or --execute."
)
CLI_HELP_UPLOAD_DRY_RUN = (
    "With --upload-missing: print what would be uploaded; do not upload."
)
CLI_HELP_UPLOAD_EXECUTE = (
    "With --upload-missing: perform real uploads (mutually exclusive with --dry-run)."
)
CLI_HELP_UPLOAD_LIMIT = (
    "With --upload-missing: process at most this many uploadable rows (after filters), for testing."
)

# --execute embeds content_date via embed_metadata (piexif, Pillow, mutagen; optional pillow-heif for HEIC).


@dataclass
class UploadJob:
    local_path: str
    base_name: str
    amazon_file_name: str
    dt: datetime
    display_raw: str
    inferred: bool


@dataclass
class IngestStats:
    skipped_no_path: int = 0
    skipped_no_date: int = 0
    skipped_bad_date: int = 0
    inferred_from_name: int = 0
    rows_scanned: int = 0


def run_upload_missing_cli(
    csv_path: str | Path | None,
    *,
    dry_run: bool,
    execute_upload: bool,
    threads: int = 16,
    limit: int | None = None,
) -> None:
    """
    Validate CLI args for --upload-missing and run the upload or dry-run flow.

    Called from list_amazon_photos.main when --upload-missing is set.
    """
    if not csv_path:
        console.print(
            "[red]When using --upload-missing, --csv PATH is required.[/red]\n"
            "Example: ./run --upload-missing --csv missing.csv --dry-run"
        )
        raise typer.Exit(1)
    if dry_run and execute_upload:
        console.print(
            "[red]Use only one of --dry-run or --execute; not both.[/red]"
        )
        raise typer.Exit(1)
    if not dry_run and not execute_upload:
        console.print(
            "[red]--upload-missing requires either --dry-run or --execute.[/red]\n"
            "Example: ./run --upload-missing --csv missing.csv --dry-run"
        )
        raise typer.Exit(1)

    run_upload_missing(
        Path(csv_path),
        dry_run=dry_run,
        execute_upload=execute_upload,
        threads=threads,
        limit=limit,
    )


def _parse_yyyymmdd(s: str) -> datetime | None:
    """Return naive midnight datetime for YYYYMMDD, or None if invalid."""
    if len(s) != 8:
        return None
    try:
        d = datetime.strptime(s, "%Y%m%d").date()
        return datetime.combine(d, dt_time(0, 0, 0))
    except ValueError:
        return None


def _parse_hhmmss(s: str) -> dt_time | None:
    """Parse first 6 digits as HHMMSS."""
    if len(s) < 6:
        return None
    try:
        return datetime.strptime(s[:6], "%H%M%S").time()
    except ValueError:
        return None


def infer_datetime_from_filename(basename: str) -> datetime | None:
    """
    Best-effort content date/time from common camera/phone filename patterns when CSV has no content_date.

    Tries, in order:
    - Leading YYYYMMDD_HHMMSS... (e.g. 20190512_050016000_iOS.jpg)
    - WP_YYYYMMDD_... (Windows Phone / Lumia style)
    - Leading YYYYMMDD followed by _ or end of stem (date-only → midnight)
    """
    stem = Path(basename).stem

    m = re.match(r"^(\d{8})_(\d{6})\d*", stem)
    if m:
        day = _parse_yyyymmdd(m.group(1))
        t = _parse_hhmmss(m.group(2))
        if day is not None and t is not None:
            return datetime.combine(day.date(), t)

    m = re.match(r"^WP_(\d{8})(?:_|$)", stem, re.IGNORECASE)
    if m:
        return _parse_yyyymmdd(m.group(1))

    m = re.match(r"^(\d{8})(?:_|$)", stem)
    if m:
        return _parse_yyyymmdd(m.group(1))

    return None


def _parse_content_date(raw: str) -> datetime | None:
    """Parse content_date from missing.csv (ISO, or e.g. 'Jan 21 2017 14:30' from OneDrive listing)."""
    s = (raw or "").strip()
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, utc=False)
        if pd.isna(dt):
            return None
        if not isinstance(dt, pd.Timestamp):
            dt = pd.Timestamp(dt)
        if dt.tzinfo is not None:
            dt = dt.tz_convert(None)
        return dt.to_pydatetime()
    except Exception:
        return None


def _allocate_unique_amazon_filename(original_name: str, used: set[str]) -> str:
    """
    Use the same base name as the local file on Amazon Photos.
    On collision (same basename used twice), use stem_2.ext, stem_3.ext, ...
    """
    if original_name not in used:
        used.add(original_name)
        return original_name
    p = Path(original_name)
    stem, suffix = p.stem, p.suffix
    n = 2
    while True:
        c = f"{stem}_{n}{suffix}"
        if c not in used:
            used.add(c)
            return c
        n += 1


def _read_missing_csv(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.is_file():
        console.print(f"[red]CSV not found: {csv_path}[/red]")
        raise typer.Exit(1)

    rows: list[dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            expected = set(MISSING_CSV_HEADER)
            got = set(reader.fieldnames)
            if not expected.issubset(got):
                missing_cols = expected - got
                console.print(
                    f"[red]CSV must include columns {sorted(expected)}. Missing: {sorted(missing_cols)}[/red]"
                )
                raise typer.Exit(1)
        for row in reader:
            rows.append({k: (row.get(k) or "").strip() for k in MISSING_CSV_HEADER})
    return rows


def _ingest_upload_jobs(
    rows: list[dict[str, str]],
    limit: int | None,
    *,
    log_skips: bool,
    row_total: int,
) -> tuple[list[UploadJob], IngestStats]:
    """
    Build upload jobs from CSV rows. Stops when ``limit`` jobs are collected (if set).
    If log_skips, print skip/ignore lines (dry-run).
    """
    used_names: set[str] = set()
    jobs: list[UploadJob] = []
    stats = IngestStats()

    for i, row in enumerate(rows, start=1):
        stats.rows_scanned = i
        if limit is not None and len(jobs) >= limit:
            break

        local_path = row.get("local_path", "").strip()
        content_date_raw = row.get("content_date", "").strip()
        file_path = row.get("file_path", "").strip()

        if not local_path:
            stats.skipped_no_path += 1
            if log_skips:
                console.print(
                    f"[yellow]Skipping[/yellow] row {i}/{row_total}: [dim]no local_path[/dim]"
                    + (f" ({file_path})" if file_path else "")
                )
            continue

        base_name = Path(local_path).name
        inferred = False
        dt: datetime | None = None
        display_raw = content_date_raw

        if content_date_raw:
            dt = _parse_content_date(content_date_raw)
            if dt is None:
                stats.skipped_bad_date += 1
                if log_skips:
                    console.print(
                        f"[yellow]Skipping[/yellow] [cyan]{local_path}[/cyan]: "
                        f"[yellow]could not parse content_date[/yellow] [dim]{content_date_raw!r}[/dim]"
                    )
                continue
        else:
            dt = infer_datetime_from_filename(base_name)
            if dt is None:
                stats.skipped_no_date += 1
                if log_skips:
                    console.print(
                        f"[yellow]Ignoring[/yellow] [cyan]{local_path}[/cyan]: "
                        "[yellow]content_date is not available[/yellow] "
                        "[dim](could not infer from filename)[/dim]"
                    )
                continue
            inferred = True
            stats.inferred_from_name += 1
            display_raw = f"{dt:%Y-%m-%d %H:%M:%S}"

        amazon_file_name = _allocate_unique_amazon_filename(base_name, used_names)
        jobs.append(
            UploadJob(
                local_path=local_path,
                base_name=base_name,
                amazon_file_name=amazon_file_name,
                dt=dt,
                display_raw=display_raw,
                inferred=inferred,
            )
        )

    return jobs, stats


def _truncate_middle(s: str, max_len: int = 56) -> str:
    if len(s) <= max_len:
        return s
    half = (max_len - 1) // 2
    return s[:half] + "…" + s[-half:]


def _stage_jobs_with_embedded_dates(
    jobs: list[UploadJob],
    *,
    parallel_workers: int,
) -> tuple[list[tuple[UploadJob, Path]], list[tuple[UploadJob, str]], Path]:
    """
    Copy each source file into a temp directory, embed ``content_date`` with Python
    libraries (see ``embed_metadata``), and return ``(ready, failures, staging_root)``.
    Caller must ``rmtree(staging_root)``.
    """
    staging_root = Path(tempfile.mkdtemp(prefix="ap_upload_meta_"))
    lock = Lock()
    successes: list[tuple[UploadJob, Path]] = []
    failures: list[tuple[UploadJob, str]] = []

    def process_one(job: UploadJob) -> None:
        src = Path(job.local_path).expanduser().resolve()
        dst = staging_root / job.amazon_file_name
        try:
            shutil.copy2(src, dst)
        except OSError as e:
            with lock:
                failures.append((job, f"copy to staging failed: {e}"))
            return
        ok, err = embed_content_date(dst, job.dt)
        if not ok:
            try:
                dst.unlink(missing_ok=True)
            except OSError:
                pass
            with lock:
                failures.append((job, err))
            return
        with lock:
            successes.append((job, dst))

    n = len(jobs)
    workers = max(1, min(parallel_workers, n, 64))
    console.print(
        f"[dim]Parallel workers for copy + metadata:[/dim] [bold]{workers}[/bold] "
        f"([dim]each job: copy → piexif/Pillow/mutagen on the staged file[/dim])"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=6,
        transient=False,
    ) as progress:
        task_id = progress.add_task(
            "[cyan]Copy + embed metadata[/cyan]",
            total=n,
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_job = {pool.submit(process_one, job): job for job in jobs}
            for fut in as_completed(future_to_job):
                job = future_to_job[fut]
                fut.result()
                progress.advance(task_id, 1)
                progress.update(
                    task_id,
                    description=(
                        f"[cyan]Copy + embed[/cyan]  [dim]{_truncate_middle(job.amazon_file_name)}[/dim]"
                    ),
                )

    console.print(
        f"[dim]Staging done:[/dim] [green]{len(successes)}[/green] ok, "
        f"[red]{len(failures)}[/red] failed"
    )

    return successes, failures, staging_root


def _job_disambig_and_infer_rich(job: UploadJob) -> tuple[str, str]:
    """Rich-text suffixes: (disambiguation note, inferred-date prefix)."""
    disambig = ""
    if job.amazon_file_name != job.base_name:
        disambig = f" [dim](disambiguated from {job.base_name!r})[/dim]"
    infer_prefix = ""
    if job.inferred:
        infer_prefix = (
            f"[yellow]content_date not in CSV; inferring {job.dt:%Y-%m-%d %H:%M:%S} "
            f"from filename {job.base_name!r}.[/yellow] "
        )
    return disambig, infer_prefix


def _upload_jobs_from_paths(
    ap,
    jobs_and_paths: list[tuple[UploadJob, Path]],
    *,
    max_connections: int,
    chunk_size: int = 64 * 1024,
) -> list:
    """
    Upload each item by streaming bytes from ``upload_path`` (staging file after metadata).

    Uses the same cdproxy POST as amazon_photos.AmazonPhotos.upload but sets ``name`` to
    ``amazon_file_name``. Files attach under the library root node.
    """
    parent_id = ap.root["id"]

    async def stream_bytes(path: Path):
        async with aiofiles.open(path, "rb") as f:
            while chunk := await f.read(chunk_size):
                yield chunk

    async def post(
        client: AsyncClient,
        sem: asyncio.Semaphore,
        job: UploadJob,
        upload_path: Path,
        max_retries: int = 12,
        m: int = 20,
        b: int = 2,
    ):
        src = upload_path.resolve()
        for i in range(max_retries + 1):
            try:
                async with sem:
                    r = await client.post(
                        ap.cdproxy_url,
                        data=stream_bytes(src),
                        params={
                            "name": job.amazon_file_name,
                            "kind": "FILE",
                            "parentNodeId": parent_id,
                        },
                    )

                if r.status_code == 409:
                    logging.getLogger("amazon_photos").debug(
                        "%s %s", r.status_code, r.text
                    )
                    return r

                if r.status_code == 400:
                    try:
                        msg = r.json().get("message", "")
                    except Exception:
                        msg = ""
                    if isinstance(msg, str) and msg.startswith("Invalid filter:"):
                        logging.getLogger("amazon_photos").error(
                            "%s %s", r.status_code, r.text
                        )
                        return
                    logging.getLogger("amazon_photos").error(
                        "%s %s", r.status_code, r.text
                    )
                    return

                if r.status_code == 401:
                    logging.getLogger("amazon_photos").error(
                        "%s %s", r.status_code, r.text
                    )
                    logging.getLogger("amazon_photos").error(
                        "Cookies expired. Log in to Amazon Photos and copy fresh cookies."
                    )
                    return
                r.raise_for_status()
                return r
            except Exception as e:
                if i == max_retries:
                    logging.getLogger("amazon_photos").debug(
                        "Max retries exceeded\n%s", e
                    )
                    return
                t = min(random.random() * (b**i), m)
                logging.getLogger("amazon_photos").debug(
                    "Retrying in %.2fs\t%s", t, e
                )
                await asyncio.sleep(t)

    fns = (partial(post, job=j, upload_path=p) for j, p in jobs_and_paths)
    return asyncio.run(
        ap.process(fns, max_connections=max_connections, desc="Uploading files")
    )


def _console_print_execute_upload_line(job: UploadJob) -> None:
    """One console line per file for --execute (before staging copy / API upload)."""
    disambig, infer_prefix = _job_disambig_and_infer_rich(job)
    console.print(
        f"[cyan]{job.local_path}[/cyan]: {infer_prefix}"
        f"[green]Uploading[/green] to Amazon Photos as "
        f"[bold]{job.amazon_file_name}[/bold]{disambig} "
        f"(content date [dim]{job.display_raw}[/dim], parsed {job.dt:%Y-%m-%d %H:%M:%S})"
    )


def _dry_run_upload_missing(csv_path: Path, threads: int, limit: int | None) -> None:
    """Print what would be uploaded: same filename as local unless collision; set content date on Amazon."""
    rows = _read_missing_csv(csv_path)
    total = len(rows)
    lim_note = f", [bold]limit={limit}[/bold]" if limit is not None else ""
    console.print(
        f"[dim]Dry run:[/dim] {total} row(s) in [bold]{csv_path}[/bold]{lim_note} (parallelism={threads})\n"
    )

    jobs, stats = _ingest_upload_jobs(rows, limit, log_skips=True, row_total=total)

    if limit is not None and len(jobs) >= limit and stats.rows_scanned < total:
        console.print(
            f"[dim]Stopped after {limit} upload job(s) (--limit); "
            f"remaining CSV rows were not evaluated.[/dim]\n"
        )

    print_lock = Lock()

    def print_job(job: UploadJob) -> None:
        exists = Path(job.local_path).expanduser().is_file()
        missing_hint = "" if exists else " [yellow](file not found on disk)[/yellow]"
        disambig, infer_prefix = _job_disambig_and_infer_rich(job)

        with print_lock:
            console.print(
                f"[cyan]{job.local_path}[/cyan]: {infer_prefix}"
                f"[green]Would upload to[/green] Amazon Photos as "
                f"[bold]{job.amazon_file_name}[/bold]{disambig} "
                f"and set content date to [dim]{job.display_raw}[/dim] "
                f"(parsed: {job.dt:%Y-%m-%d %H:%M:%S})"
                f"{missing_hint}"
            )

    with ThreadPoolExecutor(max_workers=max(1, threads)) as executor:
        futures = [executor.submit(print_job, j) for j in jobs]
        for f in as_completed(futures):
            f.result()

    console.print()
    console.print(
        f"[dim]Summary:[/dim] would upload [bold]{len(jobs)}[/bold] file(s) "
        f"([bold]{stats.inferred_from_name}[/bold] with date inferred from filename); "
        f"ignored (no content_date, not inferable): [bold]{stats.skipped_no_date}[/bold]; "
        f"skipped (unparseable CSV date): [bold]{stats.skipped_bad_date}[/bold]; "
        f"skipped (no local_path): [bold]{stats.skipped_no_path}[/bold]"
    )
    console.print(
        "[dim]With --execute, each file is copied to a temp path, content_date is embedded "
        "via Python libraries (piexif / Pillow / mutagen; optional pillow-heif for HEIC), "
        "then uploaded; originals are not changed.[/dim]"
    )


def _execute_upload_missing(csv_path: Path, threads: int, limit: int | None) -> None:
    """Copy each file to a temp path, embed CSV content_date (Python libs), then upload."""
    logging.getLogger("amazon_photos").setLevel(logging.CRITICAL)

    rows = _read_missing_csv(csv_path)
    total = len(rows)
    lim_note = f", [bold]limit={limit}[/bold]" if limit is not None else ""
    console.print(
        f"[dim]Upload:[/dim] {total} row(s) in [bold]{csv_path}[/bold]{lim_note} "
        f"(parallelism={threads})"
    )

    jobs, stats = _ingest_upload_jobs(rows, limit, log_skips=False, row_total=total)

    if limit is not None and len(jobs) >= limit and stats.rows_scanned < total:
        console.print(
            f"[dim]Stopped after {limit} upload job(s) (--limit); "
            f"remaining CSV rows were not evaluated.[/dim]\n"
        )

    if stats.skipped_no_path or stats.skipped_no_date or stats.skipped_bad_date:
        console.print(
            f"[dim]Skipped during ingest:[/dim] "
            f"no_path={stats.skipped_no_path}, "
            f"no_date={stats.skipped_no_date}, "
            f"bad_date={stats.skipped_bad_date}"
        )

    present: list[UploadJob] = []
    for job in jobs:
        p = Path(job.local_path).expanduser()
        if p.is_file():
            present.append(job)
        else:
            console.print(
                f"[yellow]Skipping[/yellow] [cyan]{job.local_path}[/cyan]: "
                "[yellow]file not found on disk[/yellow]"
            )

    if not present:
        console.print("[red]No local files to upload (all missing or no jobs).[/red]")
        raise typer.Exit(1)

    console.print(
        f"\n[dim]Preparing {len(present)} file(s) locally — each file is copied to a temp path, "
        f"then metadata is embedded (piexif / Pillow / mutagen). Progress:[/dim]"
    )
    successes, failures, staging_root = _stage_jobs_with_embedded_dates(
        present,
        parallel_workers=max(1, min(threads, 64)),
    )
    try:
        for job, err in failures:
            console.print(
                f"[red]Metadata embed failed[/red] [cyan]{job.local_path}[/cyan]: "
                f"[red]{err}[/red]"
            )

        if not successes:
            console.print(
                "[red]No files left to upload after metadata step (all failed copy or metadata embed).[/red]"
            )
            raise typer.Exit(1)

        console.print()
        for job, _path in sorted(successes, key=lambda t: t[0].local_path):
            _console_print_execute_upload_line(job)

        console.print()
        from client import get_amazon_client, reload_project_dotenv

        # Pick up any .env edits made during staging; then optional pause for fresh cookies.
        reload_project_dotenv()
        pause_raw = (os.environ.get("AMAZON_PAUSE_BEFORE_UPLOAD_SECONDS") or "").strip()
        if pause_raw.isdigit():
            pause_s = int(pause_raw)
            if pause_s > 0:
                console.print()
                console.print(
                    f"[bold yellow]Pause before upload[/bold yellow]  "
                    f"[dim]([bold]AMAZON_PAUSE_BEFORE_UPLOAD_SECONDS={pause_s}[/bold])[/dim]"
                )
                console.print(
                    "[dim]Staging is done. If your Amazon session may have expired, do this [bold]now[/bold]:"
                    "\n  1. Open [bold]https://www.amazon.com/photos/[/bold] (logged in) → DevTools → Application → Cookies."
                    "\n  2. Copy fresh [bold]session-id[/bold], [bold]ubid-main[/bold] / [bold]ubid-acbca[/bold], "
                    "[bold]at-main[/bold] / [bold]at-acbca[/bold] into your project [bold].env[/bold] as [bold]AMAZON_*[/bold]."
                    "\n  3. [bold]Save[/bold] [bold].env[/bold]."
                    f"\n[/dim][cyan]Waiting {pause_s} seconds[/cyan] [dim]— then reloads [bold].env[/bold] and starts upload.[/dim]"
                )
                time.sleep(pause_s)
                reload_project_dotenv()
                console.print("[dim]Pause finished. Reloaded [bold].env[/bold] from disk.[/dim]")

        console.print()
        console.print(
            "[cyan]Connecting to Amazon Photos…[/cyan]  "
            "[dim](credentials from [bold].env[/bold] — reloads from disk on connect.)[/dim]"
        )
        ap = get_amazon_client(console)
        _upload_jobs_from_paths(
            ap,
            successes,
            max_connections=max(1, threads),
        )

        console.print()
        n_ok = len(successes)
        n_in = len(present)
        if n_ok == n_in:
            console.print(
                f"[green]Success:[/green] Uploaded [bold]{n_ok}[/bold] file(s) to Amazon Photos "
                f"with embedded dates from [bold]{csv_path}[/bold]."
            )
        else:
            console.print(
                f"[green]Success:[/green] Uploaded [bold]{n_ok}[/bold] of [bold]{n_in}[/bold] "
                f"file(s) to Amazon Photos ([bold]{len(failures)}[/bold] failed metadata embed)."
            )
        console.print(
            "[dim]Dates were written into each staged copy via embedded metadata (EXIF for photos, "
            "MP4/QuickTime tags for common video) before upload. Original files on disk were not modified.[/dim]"
        )
        if len(jobs) != len(present):
            console.print(
                f"[dim]Skipped missing on disk before staging: {len(jobs) - len(present)} file(s).[/dim]"
            )
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def run_upload_missing(
    csv_path: Path,
    *,
    dry_run: bool,
    execute_upload: bool,
    threads: int = 16,
    limit: int | None = None,
) -> None:
    """
    Read missing.csv and upload each row to Amazon Photos, or dry-run.

    Exactly one of dry_run or execute_upload must be True (enforced by CLI).
    """
    if dry_run:
        _dry_run_upload_missing(csv_path, threads, limit)
        raise typer.Exit(0)

    _execute_upload_missing(csv_path, threads, limit)
