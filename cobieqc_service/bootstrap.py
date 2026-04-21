import logging
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from cobieqc_service.runner import cobieqc_resource_candidates, validate_cobieqc_resource_dir

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover - fallback for minimal runtimes
    requests = None

LOGGER = logging.getLogger("ifc_app.cobieqc.bootstrap")

DEFAULT_JAR_SOURCE_URL = "https://drive.google.com/file/d/19wRbk-TNoHNOmRgqqDP4AjbRqawzE7wq/view?usp=drive_link"
DEFAULT_XML_SOURCE_URL = "https://drive.google.com/drive/folders/13ZYp5lb1B57nmPpLMZnCS3zP7I--zFjg?usp=drive_link"
DEFAULT_XML_FOLDER_SOURCE_URL = DEFAULT_XML_SOURCE_URL
COBIEQC_XML_FILE_URLS_JSON_ENV = "COBIEQC_XML_FILE_URLS_JSON"
DEPRECATED_XML_ZIP_SOURCE_ENV = "COBIEQC_XML_ZIP_SOURCE_URL"
_ZIP_DEPRECATION_LOGGED = False


@dataclass
class CobieQcBootstrapStatus:
    enabled: bool
    jar_exists: bool
    jar_valid: bool
    jar_ready: bool
    resource_dir_exists: bool
    resource_dir_populated: bool
    resources_ready: bool
    jar_path: str
    resource_dir: str
    resource_source: str = "missing"
    jar_source: str = ""
    source_mode: str = "none"
    missing_files: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    last_error: str = ""
    jar_validation_error: str = ""


_LAST_STATUS: Optional[CobieQcBootstrapStatus] = None


@dataclass
class JarValidationResult:
    valid: bool
    reason: str = ""


@dataclass
class DownloadResult:
    path: Path
    content_type: str = ""
    content_length: str = ""
    http_status: str = ""


def _data_root() -> Path:
    return Path(os.getenv("COBIEQC_DATA_DIR", "/data/cobieqc")).expanduser()


def _jar_path() -> Path:
    return Path(os.getenv("COBIEQC_JAR_PATH", str(_data_root() / "CobieQcReporter.jar"))).expanduser()


def _resource_dir() -> Path:
    return Path(os.getenv("COBIEQC_RESOURCE_DIR", str(_data_root() / "xsl_xml"))).expanduser()


def parse_google_drive_file_id(url: str) -> str:
    parsed = urlparse((url or "").strip())
    if not parsed.netloc:
        raise ValueError("URL is required")

    if "drive.google.com" not in parsed.netloc:
        raise ValueError("Not a Google Drive URL")

    parts = [part for part in parsed.path.split("/") if part]
    if "file" in parts and "d" in parts:
        d_index = parts.index("d")
        if d_index + 1 < len(parts):
            return parts[d_index + 1]

    query_id = parse_qs(parsed.query).get("id", [""])[0].strip()
    if query_id:
        return query_id

    raise ValueError(f"Could not parse Google Drive file id from URL: {url}")


def google_drive_direct_download_url(url: str) -> str:
    file_id = parse_google_drive_file_id(url)
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def _is_google_drive_folder_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    return "drive.google.com" in parsed.netloc and "/drive/folders/" in parsed.path


def _classify_xml_source_url(url: str) -> str:
    if not (url or "").strip():
        return "unset"
    if _is_google_drive_folder_url(url):
        return "google_drive_folder"
    parsed = urlparse(url)
    if parsed.scheme in ("", "file"):
        return "local_folder"
    return "remote_folder_reference"


def _is_non_empty_file(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _detect_text_payload(prefix: bytes) -> str:
    snippet = (prefix or b"").lstrip().lower()
    if snippet.startswith((b"<!doctype html", b"<html")):
        return "html"
    if snippet.startswith(b"<?xml") or snippet.startswith(b"<xml"):
        return "xml"
    if snippet.startswith(b"{") or snippet.startswith(b"["):
        return "json/text"
    if snippet.startswith(b"<"):
        return "xml_or_html"
    return "binary_or_unknown"


def _header_hex(prefix: bytes, limit: int = 16) -> str:
    return (prefix or b"")[:limit].hex()


def validate_jar_file(path: Path) -> JarValidationResult:
    if not path.exists():
        return JarValidationResult(valid=False, reason="file does not exist")
    if not path.is_file():
        return JarValidationResult(valid=False, reason="path is not a file")
    file_size = path.stat().st_size
    if file_size <= 0:
        return JarValidationResult(valid=False, reason="file is empty")
    with path.open("rb") as handle:
        header = handle.read(4)
    if not header.startswith(b"PK"):
        payload_hint = _detect_text_payload(header)
        return JarValidationResult(
            valid=False,
            reason=f"invalid ZIP/JAR signature header={_header_hex(header)} payload_hint={payload_hint}",
        )
    try:
        with zipfile.ZipFile(path) as archive:
            corrupted = archive.testzip()
            if corrupted:
                return JarValidationResult(valid=False, reason=f"zip validation failed at entry: {corrupted}")
    except Exception as exc:
        return JarValidationResult(valid=False, reason=f"zip validation error: {exc}")
    try:
        proc = subprocess.run(
            ["jar", "tf", str(path)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            detail = stderr or stdout or "jar tf returned non-zero exit"
            return JarValidationResult(valid=False, reason=f"jar tf validation failed: {detail}")
    except FileNotFoundError:
        LOGGER.info("COBieQC bootstrap: jar CLI unavailable; skipped 'jar tf' validation for %s", path)
    except Exception as exc:
        return JarValidationResult(valid=False, reason=f"jar tf validation error: {exc}")
    return JarValidationResult(valid=True, reason="")


def _validate_resource_dir(path: Path) -> dict:
    return validate_cobieqc_resource_dir(path)


def _is_html_like(content_type: str, content_prefix: bytes) -> bool:
    lowered = (content_type or "").lower()
    if "text/html" in lowered:
        return True
    snippet = (content_prefix or b"").lstrip()[:200].lower()
    return snippet.startswith(b"<!doctype html") or snippet.startswith(b"<html")


def _force_resource_download_enabled() -> bool:
    value = (
        os.getenv("COBIEQC_XML_FORCE_DOWNLOAD", "")
        or os.getenv("COBIEQC_FORCE_DOWNLOAD", "")
        or os.getenv("FORCE", "")
    ).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _resource_dir_resolution_candidates(preferred_resource_dir: Path, configured_resource_dir: Path) -> list[Path]:
    candidates: list[Path] = [configured_resource_dir, preferred_resource_dir]
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.expanduser())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _resolve_existing_resource_dir(preferred_resource_dir: Path, configured_resource_dir: Path) -> tuple[Optional[Path], str]:
    for idx, candidate in enumerate(_resource_dir_resolution_candidates(preferred_resource_dir, configured_resource_dir)):
        resolved = candidate.expanduser().resolve()
        validation = _validate_resource_dir(resolved)
        if validation["valid"]:
            source = "existing_dir" if idx == 0 else "preferred_data_dir"
            return resolved, source
        if validation["exists"]:
            LOGGER.info(
                "COBieQC bootstrap: resource directory missing expected files at %s (%s)",
                resolved,
                "; ".join(validation["missing"]),
            )
    return None, "missing"


def _packaged_fallback_candidates(preferred_resource_dir: Path, configured_resource_dir: Path) -> list[Path]:
    blocked = {str(preferred_resource_dir.expanduser().resolve()), str(configured_resource_dir.expanduser().resolve())}
    fallbacks: list[Path] = []
    for candidate in cobieqc_resource_candidates():
        resolved = candidate.expanduser().resolve()
        key = str(resolved)
        if key in blocked:
            continue
        if "/data/" in key:
            continue
        fallbacks.append(resolved)
    deduped: list[Path] = []
    seen: set[str] = set()
    for item in fallbacks:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _replace_file_atomically(source_file: Path, destination_file: Path) -> None:
    destination_file.parent.mkdir(parents=True, exist_ok=True)
    temp_destination = destination_file.with_suffix(destination_file.suffix + ".tmp")
    shutil.move(str(source_file), temp_destination)
    os.replace(temp_destination, destination_file)


def _replace_dir_atomically(source_dir: Path, destination_dir: Path) -> None:
    destination_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir = destination_dir.with_name(destination_dir.name + ".bak")
    temp_dir = destination_dir.with_name(destination_dir.name + ".tmp")
    shutil.rmtree(temp_dir, ignore_errors=True)
    shutil.copytree(source_dir, temp_dir)

    if destination_dir.exists():
        shutil.rmtree(backup_dir, ignore_errors=True)
        os.replace(destination_dir, backup_dir)

    os.replace(temp_dir, destination_dir)
    shutil.rmtree(backup_dir, ignore_errors=True)


def _download_to_temp(source_url: str, suffix: str, purpose: str) -> DownloadResult:
    direct_url = google_drive_direct_download_url(source_url) if "drive.google.com" in source_url else source_url
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_path = Path(temp_file.name)
    content_type = ""
    content_length = ""
    http_status = ""
    try:
        if requests is not None:
            with requests.get(direct_url, stream=True, timeout=120, allow_redirects=True) as response:
                http_status = str(response.status_code)
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                content_length = response.headers.get("content-length", "")
                with temp_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            handle.write(chunk)
        else:
            from urllib.request import urlopen

            with urlopen(direct_url, timeout=120) as response, temp_path.open("wb") as handle:
                status = getattr(response, "status", None)
                http_status = str(status) if status is not None else ""
                content_type = response.headers.get("content-type", "")
                content_length = response.headers.get("content-length", "")
                shutil.copyfileobj(response, handle)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    file_size = temp_path.stat().st_size
    if file_size == 0:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded {purpose} is empty")
    with temp_path.open("rb") as handle:
        prefix = handle.read(1024)
    if _is_html_like(content_type, prefix):
        text_prefix = prefix.decode("utf-8", errors="ignore").lower()
        if "drive.google.com" in direct_url and ("google drive" in text_prefix or "confirm" in text_prefix):
            temp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Downloaded {purpose} appears to be a Google Drive viewer/confirmation page, not the file content"
            )
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded {purpose} appears to be HTML content instead of a file")
    return DownloadResult(
        path=temp_path,
        content_type=content_type,
        content_length=content_length,
        http_status=http_status,
    )


def _load_xml_file_urls_mapping(raw_json: str) -> dict[str, str]:
    if not raw_json.strip():
        return {}
    parsed = json.loads(raw_json)
    if not isinstance(parsed, dict):
        raise ValueError(f"{COBIEQC_XML_FILE_URLS_JSON_ENV} must be a JSON object")
    normalized: dict[str, str] = {}
    for key, value in parsed.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{COBIEQC_XML_FILE_URLS_JSON_ENV} contains an invalid filename key")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{COBIEQC_XML_FILE_URLS_JSON_ENV} entry '{key}' has an invalid URL")
        normalized[key.strip()] = value.strip()
    return normalized


def _copy_packaged_resource_dir(preferred_resource_dir: Path, configured_resource_dir: Path) -> tuple[Optional[Path], str]:
    for fallback in _packaged_fallback_candidates(preferred_resource_dir, configured_resource_dir):
        validation = _validate_resource_dir(fallback)
        if not validation["valid"]:
            continue
        _replace_dir_atomically(fallback, configured_resource_dir)
        return configured_resource_dir.expanduser().resolve(), "packaged_fallback_copy"
    return None, "missing"


def _sync_resource_folder_from_source(source_url: str, target_dir: Path) -> tuple[bool, str]:
    if not source_url:
        return False, "source URL is not configured"

    source_kind = _classify_xml_source_url(source_url)
    if source_kind == "google_drive_folder":
        return False, "COBieQC folder source configured but automatic folder sync is unavailable for Google Drive folder URLs"

    parsed = urlparse(source_url)
    local_source = None
    if parsed.scheme == "file":
        local_source = Path(parsed.path).expanduser()
    elif parsed.scheme == "":
        local_source = Path(source_url).expanduser()

    if local_source is not None:
        if not local_source.exists() or not local_source.is_dir():
            return False, f"configured folder source path is unavailable: {local_source}"
        validation = _validate_resource_dir(local_source)
        if not validation["valid"]:
            return False, f"configured folder source directory missing expected files: {'; '.join(validation['missing'])}"
        _replace_dir_atomically(local_source.resolve(), target_dir)
        return True, "local_folder_sync"

    return False, "automatic sync for this folder source URL is not supported in this runtime"


def _build_status(
    last_error: str = "",
    resolved_resource_dir: Optional[Path] = None,
    resource_source: str = "missing",
    source_mode: str = "none",
    warnings: Optional[list[str]] = None,
    errors: Optional[list[str]] = None,
    jar_validation: Optional[JarValidationResult] = None,
    jar_source: str = "",
) -> CobieQcBootstrapStatus:
    jar_path = _jar_path()
    resource_dir = (resolved_resource_dir or _resource_dir()).expanduser().resolve()
    jar_exists = _is_non_empty_file(jar_path)
    jar_validation_result = jar_validation or validate_jar_file(jar_path)
    jar_valid = bool(jar_validation_result.valid)
    resource_validation = _validate_resource_dir(resource_dir)
    resource_exists = bool(resource_validation["exists"] and resource_validation["is_dir"])
    resource_populated = bool(resource_validation["valid"])
    jar_ready = jar_exists and jar_valid
    missing_files = list(resource_validation.get("missing_required_files") or [])
    resources_ready = resource_exists and resource_populated and not missing_files
    status_warnings = list(warnings or [])
    status_errors = list(errors or [])
    if last_error and last_error not in status_errors:
        status_errors.append(last_error)
    return CobieQcBootstrapStatus(
        enabled=jar_ready and resources_ready,
        jar_exists=jar_exists,
        jar_valid=jar_valid,
        jar_ready=jar_ready,
        resource_dir_exists=resource_exists,
        resource_dir_populated=resource_populated,
        resources_ready=resources_ready,
        jar_path=str(jar_path),
        resource_dir=str(resource_dir),
        resource_source=resource_source if resources_ready else "missing",
        jar_source=jar_source,
        source_mode=source_mode,
        missing_files=missing_files,
        warnings=status_warnings,
        errors=status_errors,
        last_error=last_error,
        jar_validation_error=jar_validation_result.reason,
    )


def get_cobieqc_bootstrap_status() -> dict:
    global _LAST_STATUS
    if _LAST_STATUS is None:
        _LAST_STATUS = _build_status()
    return asdict(_LAST_STATUS)


def bootstrap_cobieqc_assets() -> None:
    global _LAST_STATUS
    global _ZIP_DEPRECATION_LOGGED

    data_root = _data_root()
    jar_path = _jar_path()
    resource_dir = _resource_dir()
    preferred_resource_dir = (_data_root() / "xsl_xml").expanduser()
    jar_source_url = os.getenv("COBIEQC_JAR_SOURCE_URL", DEFAULT_JAR_SOURCE_URL).strip() or DEFAULT_JAR_SOURCE_URL
    xml_source_url = os.getenv("COBIEQC_XML_SOURCE_URL", DEFAULT_XML_SOURCE_URL).strip() or DEFAULT_XML_SOURCE_URL
    xml_file_urls_raw = os.getenv(COBIEQC_XML_FILE_URLS_JSON_ENV, "").strip()
    xml_file_urls: dict[str, str] = {}
    legacy_xml_zip_source_url = os.getenv(DEPRECATED_XML_ZIP_SOURCE_ENV, "").strip()
    force_jar_refresh = _bool_env("COBIEQC_FORCE_JAR_REFRESH", default=False)
    jar_source = "existing_reused"
    jar_validation = JarValidationResult(valid=False, reason="not yet validated")

    data_root.mkdir(parents=True, exist_ok=True)
    LOGGER.info("COBieQC bootstrap: data root ready at %s", data_root)

    warnings: list[str] = []
    errors: list[str] = []

    if legacy_xml_zip_source_url and not _ZIP_DEPRECATION_LOGGED:
        message = f"{DEPRECATED_XML_ZIP_SOURCE_ENV} is deprecated and ignored; XML ZIP bootstrap is disabled"
        LOGGER.warning("COBieQC bootstrap: %s", message)
        warnings.append(message)
        _ZIP_DEPRECATION_LOGGED = True

    if xml_file_urls_raw:
        try:
            xml_file_urls = _load_xml_file_urls_mapping(xml_file_urls_raw)
        except Exception as exc:
            errors.append(f"Invalid {COBIEQC_XML_FILE_URLS_JSON_ENV}: {exc}")

    try:
        if force_jar_refresh and jar_path.exists():
            jar_source = "forced_refresh"
            jar_path.unlink(missing_ok=True)
            LOGGER.info("COBieQC bootstrap: removed existing JAR due to COBIEQC_FORCE_JAR_REFRESH=true path=%s", jar_path)
        existing_jar_valid = validate_jar_file(jar_path)
        if existing_jar_valid.valid:
            jar_validation = existing_jar_valid
            jar_source = "existing_reused"
            LOGGER.info("COBieQC bootstrap: existing JAR validated and kept path=%s", jar_path)
        else:
            if jar_path.exists():
                LOGGER.warning(
                    "COBieQC bootstrap: existing JAR invalid and will be replaced path=%s reason=%s",
                    jar_path,
                    existing_jar_valid.reason,
                )
                jar_path.unlink(missing_ok=True)
            jar_source = "downloaded_replacement"
            LOGGER.info("COBieQC bootstrap: downloading JAR source_url=%s destination=%s", jar_source_url, jar_path)
            jar_download = _download_to_temp(jar_source_url, ".jar", "COBieQC JAR")
            _replace_file_atomically(jar_download.path, jar_path)
            jar_validation = validate_jar_file(jar_path)
            LOGGER.info(
                "COBieQC bootstrap: downloaded JAR metadata source_url=%s destination=%s http_status=%s content_type=%s content_length=%s replaced_existing=%s validation_valid=%s validation_error=%s",
                jar_source_url,
                jar_path,
                jar_download.http_status or "unknown",
                jar_download.content_type or "unknown",
                jar_download.content_length or "unknown",
                "yes",
                jar_validation.valid,
                jar_validation.reason or "",
            )
            if not jar_validation.valid:
                prefix = b""
                with jar_path.open("rb") as handle:
                    prefix = handle.read(64)
                header_hex = _header_hex(prefix, limit=32)
                payload_hint = _detect_text_payload(prefix)
                jar_path.unlink(missing_ok=True)
                jar_source = "download_invalid_removed"
                raise RuntimeError(
                    "Downloaded JAR failed validation: "
                    f"{jar_validation.reason}; payload_hint={payload_hint}; header_hex={header_hex}"
                )
    except Exception as exc:
        errors.append(f"JAR download/install failed: {exc}")
        LOGGER.error("COBieQC bootstrap JAR install failed: %s", exc)

    resolved_resource_dir, resource_source = _resolve_existing_resource_dir(preferred_resource_dir, resource_dir)
    source_mode = "existing"

    if not resolved_resource_dir:
        fallback_dir, fallback_source = _copy_packaged_resource_dir(preferred_resource_dir, resource_dir)
        if fallback_dir:
            resolved_resource_dir, resource_source = fallback_dir, fallback_source
            LOGGER.info("COBieQC bootstrap: copied packaged fallback resources into %s", resolved_resource_dir)
            source_mode = "packaged_fallback"

    xml_source_kind = _classify_xml_source_url(xml_source_url)
    remote_folder_sync_supported = False
    LOGGER.info(
        "COBieQC bootstrap: xml_source_kind=%s xml_source_url_present=%s remote_folder_sync_supported=%s",
        xml_source_kind,
        bool(xml_source_url),
        remote_folder_sync_supported,
    )

    if _is_google_drive_folder_url(xml_source_url):
        warning = (
            "Google Drive folder URLs in COBIEQC_XML_SOURCE_URL are unsupported; "
            f"use {COBIEQC_XML_FILE_URLS_JSON_ENV} with direct file download URLs"
        )
        warnings.append(warning)
        LOGGER.warning("COBieQC bootstrap: %s", warning)
        if not resolved_resource_dir and not xml_file_urls:
            errors.append("resource bootstrap unsupported for Google Drive folder URL source mode")
            source_mode = "unsupported_google_drive_folder"

    if xml_file_urls:
        resource_dir.mkdir(parents=True, exist_ok=True)
        source_mode = "file_urls_json"
        force_download = _force_resource_download_enabled()
        for filename, file_url in xml_file_urls.items():
            destination = resource_dir / filename
            needs_download = force_download or (not destination.exists()) or destination.stat().st_size == 0
            if not needs_download:
                continue
            try:
                LOGGER.info("COBieQC bootstrap: downloading resource %s -> %s", filename, destination)
                download_result = _download_to_temp(file_url, destination.suffix or ".bin", f"COBieQC resource '{filename}'")
                _replace_file_atomically(download_result.path, destination)
            except Exception as exc:
                errors.append(f"resource download failed for {filename}: {exc}")
                LOGGER.error("COBieQC bootstrap: resource download failed for %s: %s", filename, exc)
        validation = _validate_resource_dir(resource_dir)
        if validation["valid"]:
            resolved_resource_dir = resource_dir.expanduser().resolve()
            resource_source = "file_urls_json"
        else:
            missing_required_files = validation.get("missing_required_files", [])
            errors.append(
                "COBieQC resource directory missing required files after download: "
                + ", ".join(missing_required_files or validation["missing"])
            )
    elif not resolved_resource_dir and not _is_google_drive_folder_url(xml_source_url):
        source_mode = "unconfigured"

    if resolved_resource_dir:
        LOGGER.info("COBieQC bootstrap: using COBieQC resource folder at %s", resolved_resource_dir)
    else:
        errors.append("COBieQC resource folder unavailable: missing required files in resource directory")
        LOGGER.warning("COBieQC bootstrap: COBieQC resource folder unavailable")

    last_error = " | ".join(errors)
    _LAST_STATUS = _build_status(
        last_error=last_error,
        resolved_resource_dir=resolved_resource_dir,
        resource_source=resource_source,
        source_mode=source_mode,
        warnings=warnings,
        errors=errors,
        jar_validation=jar_validation,
        jar_source=jar_source,
    )
    LOGGER.info(
        "COBieQC bootstrap complete jar_exists=%s jar_valid=%s jar_source=%s jar_validation_error=%s resources_ready=%s cobieqc_enabled=%s jar_path=%s resource_dir=%s resource_source=%s",
        _LAST_STATUS.jar_exists,
        _LAST_STATUS.jar_valid,
        _LAST_STATUS.jar_source,
        _LAST_STATUS.jar_validation_error,
        _LAST_STATUS.resources_ready,
        _LAST_STATUS.enabled,
        _LAST_STATUS.jar_path,
        _LAST_STATUS.resource_dir,
        _LAST_STATUS.resource_source,
    )


__all__ = [
    "DEFAULT_JAR_SOURCE_URL",
    "DEFAULT_XML_FOLDER_SOURCE_URL",
    "DEFAULT_XML_SOURCE_URL",
    "DEPRECATED_XML_ZIP_SOURCE_ENV",
    "bootstrap_cobieqc_assets",
    "get_cobieqc_bootstrap_status",
    "google_drive_direct_download_url",
    "parse_google_drive_file_id",
]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    bootstrap_cobieqc_assets()


if __name__ == "__main__":
    main()
