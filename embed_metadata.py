"""
Embed capture/content datetime into media files using Python dependencies only (no ExifTool).

- JPEG: piexif (EXIF DateTime / DateTimeOriginal / DateTimeDigitized)
- PNG / WebP / TIFF: Pillow + piexif EXIF bytes where supported
- HEIC/HEIF: optional pillow-heif + Pillow save with EXIF
- MP4 / M4V / MOV / M4A: mutagen (MP4/QuickTime-style tags)

Unsupported extensions fail with a clear message (e.g. RAW, some MKV) — extend as needed.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

# --- Exif tag numbers (TIFF/EXIF) used with Pillow Exif ---
_EXIF_DATETIME = 306  # ImageIFD DateTime

_JPEG_SUFFIXES = {".jpg", ".jpeg", ".jpe", ".jfif"}
_PNG_SUFFIXES = {".png"}
_WEBP_SUFFIXES = {".webp"}
_TIFF_SUFFIXES = {".tif", ".tiff"}
_HEIF_SUFFIXES = {".heic", ".heif"}
_MP4_SUFFIXES = {".mp4", ".m4v", ".mov", ".m4a", ".3gp"}


def _exif_date_str(dt: datetime) -> str:
    """EXIF date/time string (colon-separated date)."""
    return dt.strftime("%Y:%m:%d %H:%M:%S")


def _embed_jpeg_piexif(path: Path, dt: datetime) -> tuple[bool, str]:
    import piexif

    date_str = _exif_date_str(dt)
    try:
        exif_dict = piexif.load(str(path))
    except Exception as e:
        return False, f"piexif load: {e}"

    exif_dict.setdefault("0th", {})
    exif_dict.setdefault("Exif", {})
    exif_dict.setdefault("1st", {})
    exif_dict.setdefault("GPS", {})
    exif_dict["0th"][piexif.ImageIFD.DateTime] = date_str.encode("utf-8")
    exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = date_str.encode("utf-8")
    exif_dict["Exif"][piexif.ExifIFD.DateTimeDigitized] = date_str.encode("utf-8")

    try:
        exif_bytes = piexif.dump(exif_dict)
    except Exception:
        exif_dict["thumbnail"] = None
        try:
            exif_bytes = piexif.dump(exif_dict)
        except Exception as e:
            return False, f"piexif dump: {e}"

    try:
        piexif.insert(exif_bytes, str(path))
    except Exception as e:
        return False, f"piexif insert: {e}"
    return True, ""


def _build_piexif_bytes(dt: datetime) -> bytes:
    """Minimal EXIF blob for Pillow (PNG/WebP/HEIF) embedding."""
    import piexif

    date_str = _exif_date_str(dt)
    exif_dict = {
        "0th": {
            piexif.ImageIFD.DateTime: date_str.encode("utf-8"),
        },
        "Exif": {
            piexif.ExifIFD.DateTimeOriginal: date_str.encode("utf-8"),
            piexif.ExifIFD.DateTimeDigitized: date_str.encode("utf-8"),
        },
        "GPS": {},
        "1st": {},
        "thumbnail": None,
    }
    return piexif.dump(exif_dict)


def _embed_pillow_exif_image(path: Path, dt: datetime, format_name: str) -> tuple[bool, str]:
    """PNG / WebP / HEIF: open with Pillow, save with EXIF bytes."""
    from PIL import Image

    try:
        exif_bytes = _build_piexif_bytes(dt)
    except Exception as e:
        return False, f"piexif build: {e}"

    try:
        img = Image.open(path)
    except Exception as e:
        return False, f"Pillow open: {e}"

    fmt = (img.format or format_name).upper()
    try:
        if fmt == "PNG":
            img.save(path, format="PNG", exif=exif_bytes)
        elif fmt == "WEBP":
            img.save(path, format="WEBP", exif=exif_bytes, method=6)
        elif fmt in ("HEIF", "HEIC"):
            # pillow-heif: preserve format; omit format= so encoder matches file
            img.save(path, exif=exif_bytes)
        else:
            return False, f"unexpected Pillow format {fmt!r}"
    except Exception as e:
        return False, f"Pillow save ({fmt}): {e}"
    return True, ""


def _embed_tiff_pillow(path: Path, dt: datetime) -> tuple[bool, str]:
    from PIL import Image

    date_str = _exif_date_str(dt)
    try:
        img = Image.open(path)
    except Exception as e:
        return False, f"Pillow open: {e}"
    if img.format not in ("TIFF", "TIF"):
        return False, f"not TIFF ({img.format!r})"

    exif = img.getexif()
    exif[_EXIF_DATETIME] = date_str
    # Exif sub-IFD (DateTimeOriginal / DateTimeDigitized) when Pillow supports set_ifd
    try:
        from PIL.ExifTags import IFD

        if hasattr(exif, "has_ifd") and exif.has_ifd(IFD.Exif):
            sub = dict(exif.get_ifd(IFD.Exif))
        else:
            sub = dict(exif.get_ifd(0x8769)) if 0x8769 in exif else {}
        sub[0x9003] = date_str  # DateTimeOriginal
        sub[0x9004] = date_str  # DateTimeDigitized
        if hasattr(exif, "set_ifd"):
            exif.set_ifd(IFD.Exif, sub)
        else:
            exif[0x8769] = sub
    except Exception:
        pass

    try:
        img.save(path, format="TIFF", exif=exif.tobytes())
    except Exception as e:
        return False, f"Pillow TIFF save: {e}"
    return True, ""


def _register_heif_opener() -> bool:
    try:
        from pillow_heif import register_heif_opener

        register_heif_opener()
        return True
    except ImportError:
        return False


def _embed_heif(path: Path, dt: datetime) -> tuple[bool, str]:
    if not _register_heif_opener():
        return (
            False,
            "HEIC/HEIF requires optional dependency pillow-heif (pip install pillow-heif)",
        )
    return _embed_pillow_exif_image(path, dt, "HEIF")


def _embed_mutagen_mp4(path: Path, dt: datetime) -> tuple[bool, str]:
    from mutagen.mp4 import MP4, MP4FreeForm

    day = dt.strftime("%Y-%m-%d")
    iso = dt.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        mp4 = MP4(path)
    except Exception as e:
        return False, f"mutagen open: {e}"

    tags = mp4.tags
    if tags is None:
        tags = {}
        mp4.tags = tags

    tags["©day"] = [day]
    # QuickTime-style creation date (bytes payload)
    try:
        tags["----:com.apple.quicktime:creationdate"] = [
            MP4FreeForm(iso.encode("utf-8"))
        ]
    except Exception:
        pass
    try:
        tags["----:com.apple.quicktime:modifydate"] = [MP4FreeForm(iso.encode("utf-8"))]
    except Exception:
        pass

    try:
        mp4.save()
    except Exception as e:
        return False, f"mutagen save: {e}"
    return True, ""


def embed_content_date(path: Path, dt: datetime) -> tuple[bool, str]:
    """
    Write ``dt`` into embedded metadata on ``path`` (file should be a writable copy).

    Returns (ok, error_message).
    """
    suffix = path.suffix.lower()

    if suffix in _JPEG_SUFFIXES:
        return _embed_jpeg_piexif(path, dt)

    if suffix in _PNG_SUFFIXES:
        return _embed_pillow_exif_image(path, dt, "PNG")

    if suffix in _WEBP_SUFFIXES:
        return _embed_pillow_exif_image(path, dt, "WEBP")

    if suffix in _TIFF_SUFFIXES:
        return _embed_tiff_pillow(path, dt)

    if suffix in _HEIF_SUFFIXES:
        return _embed_heif(path, dt)

    if suffix in _MP4_SUFFIXES:
        return _embed_mutagen_mp4(path, dt)

    return (
        False,
        f"unsupported extension {suffix!r} for embedded dates "
        f"(supported: {sorted(_JPEG_SUFFIXES | _PNG_SUFFIXES | _WEBP_SUFFIXES | _TIFF_SUFFIXES | _HEIF_SUFFIXES | _MP4_SUFFIXES)})",
    )
