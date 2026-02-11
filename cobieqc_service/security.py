import os
import re
from pathlib import Path

MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
_ALLOWED_EXTENSIONS = {".xlsx"}


def sanitize_filename(filename: str) -> str:
    name = Path(filename or "upload.xlsx").name
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return sanitized[:200] or "upload.xlsx"


def validate_upload(filename: str, size_bytes: int, max_size_bytes: int = MAX_FILE_SIZE_BYTES) -> None:
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise ValueError("Only .xlsx files are supported.")
    if size_bytes <= 0:
        raise ValueError("Uploaded file is empty.")
    if size_bytes > max_size_bytes:
        raise ValueError(f"Uploaded file exceeds {max_size_bytes // (1024 * 1024)} MB limit.")
