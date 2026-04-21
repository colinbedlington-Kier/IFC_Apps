import logging
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
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
DEPRECATED_XML_ZIP_SOURCE_ENV = "COBIEQC_XML_ZIP_SOURCE_URL"
_ZIP_DEPRECATION_LOGGED = False


@dataclass
class CobieQcBootstrapStatus:
    enabled: bool
    jar_exists: bool
    resource_dir_exists: bool
    resource_dir_populated: bool
    jar_path: str
    resource_dir: str
    resource_source: str = "missing"
    last_error: str = ""


_LAST_STATUS: Optional[CobieQcBootstrapStatus] = None


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


def _validate_resource_dir(path: Path) -> dict:
    return validate_cobieqc_resource_dir(path)


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


def _download_to_temp(source_url: str, suffix: str, purpose: str) -> tuple[Path, str]:
    direct_url = google_drive_direct_download_url(source_url) if "drive.google.com" in source_url else source_url
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_path = Path(temp_file.name)
    content_type = ""
    try:
        if requests is not None:
            with requests.get(direct_url, stream=True, timeout=120) as response:
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                with temp_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            handle.write(chunk)
        else:
            from urllib.request import urlopen

            with urlopen(direct_url, timeout=120) as response, temp_path.open("wb") as handle:
                shutil.copyfileobj(response, handle)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    if temp_path.stat().st_size == 0:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded {purpose} is empty")
    return temp_path, content_type


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
) -> CobieQcBootstrapStatus:
    jar_path = _jar_path()
    resource_dir = (resolved_resource_dir or _resource_dir()).expanduser().resolve()
    jar_exists = _is_non_empty_file(jar_path)
    resource_validation = _validate_resource_dir(resource_dir)
    resource_exists = bool(resource_validation["exists"] and resource_validation["is_dir"])
    resource_populated = bool(resource_validation["valid"])
    return CobieQcBootstrapStatus(
        enabled=jar_exists and resource_populated,
        jar_exists=jar_exists,
        resource_dir_exists=resource_exists,
        resource_dir_populated=resource_populated,
        jar_path=str(jar_path),
        resource_dir=str(resource_dir),
        resource_source=resource_source if resource_populated else "missing",
        last_error=last_error,
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
    legacy_xml_zip_source_url = os.getenv(DEPRECATED_XML_ZIP_SOURCE_ENV, "").strip()

    data_root.mkdir(parents=True, exist_ok=True)
    LOGGER.info("COBieQC bootstrap: data root ready at %s", data_root)

    errors: list[str] = []

    if legacy_xml_zip_source_url and not _ZIP_DEPRECATION_LOGGED:
        LOGGER.warning(
            "COBieQC bootstrap: %s is deprecated and ignored; XML ZIP bootstrap is disabled",
            DEPRECATED_XML_ZIP_SOURCE_ENV,
        )
        _ZIP_DEPRECATION_LOGGED = True

    try:
        if _is_non_empty_file(jar_path):
            LOGGER.info("COBieQC bootstrap: existing JAR kept at %s", jar_path)
        else:
            LOGGER.info("COBieQC bootstrap: downloading JAR to %s", jar_path)
            jar_download = _download_to_temp(jar_source_url, ".jar", "COBieQC JAR")
            jar_temp = jar_download[0] if isinstance(jar_download, tuple) else jar_download
            _replace_file_atomically(jar_temp, jar_path)
            LOGGER.info("COBieQC bootstrap: JAR installed at %s", jar_path)
    except Exception as exc:
        errors.append(f"JAR download/install failed: {exc}")
        LOGGER.error("COBieQC bootstrap JAR install failed: %s", exc)

    resolved_resource_dir, resource_source = _resolve_existing_resource_dir(preferred_resource_dir, resource_dir)

    if not resolved_resource_dir:
        fallback_dir, fallback_source = _copy_packaged_resource_dir(preferred_resource_dir, resource_dir)
        if fallback_dir:
            resolved_resource_dir, resource_source = fallback_dir, fallback_source
            LOGGER.info("COBieQC bootstrap: copied packaged fallback resources into %s", resolved_resource_dir)

    xml_source_kind = _classify_xml_source_url(xml_source_url)
    remote_folder_sync_supported = xml_source_kind in {"local_folder", "remote_folder_reference"}
    LOGGER.info(
        "COBieQC bootstrap: xml_source_kind=%s xml_source_url_present=%s remote_folder_sync_supported=%s",
        xml_source_kind,
        bool(xml_source_url),
        remote_folder_sync_supported,
    )

    if resolved_resource_dir:
        LOGGER.info("COBieQC bootstrap: using COBieQC resource folder at %s", resolved_resource_dir)
    elif xml_source_url:
        sync_ok, sync_message = _sync_resource_folder_from_source(xml_source_url, resource_dir)
        if sync_ok:
            validation = _validate_resource_dir(resource_dir)
            if validation["valid"]:
                resolved_resource_dir = resource_dir.expanduser().resolve()
                resource_source = "folder_sync"
                LOGGER.info("COBieQC bootstrap: COBieQC resource folder synced to %s", resolved_resource_dir)
            else:
                errors.append(
                    "COBieQC resource directory missing expected files after sync: " + "; ".join(validation["missing"])
                )
                LOGGER.warning(
                    "COBieQC bootstrap: COBieQC resource directory missing expected files after sync at %s (%s)",
                    resource_dir,
                    "; ".join(validation["missing"]),
                )
        else:
            errors.append(f"COBieQC resource folder sync failed: {sync_message}")
            LOGGER.warning("COBieQC bootstrap: COBieQC resource folder sync failed: %s", sync_message)
    else:
        errors.append("COBieQC resource folder unavailable: no local resources and no folder source configured")
        LOGGER.warning("COBieQC bootstrap: COBieQC resource folder unavailable")

    last_error = " | ".join(errors)
    _LAST_STATUS = _build_status(
        last_error=last_error,
        resolved_resource_dir=resolved_resource_dir,
        resource_source=resource_source,
    )
    LOGGER.info(
        "COBieQC bootstrap complete jar_exists=%s resource_dir_exists=%s resource_dir_populated=%s cobieqc_enabled=%s jar_path=%s resource_dir=%s resource_source=%s",
        _LAST_STATUS.jar_exists,
        _LAST_STATUS.resource_dir_exists,
        _LAST_STATUS.resource_dir_populated,
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
