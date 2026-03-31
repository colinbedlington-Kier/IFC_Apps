import logging
import os
import shutil
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from cobieqc_service.runner import cobieqc_resource_candidates

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover - fallback for minimal runtimes
    requests = None

LOGGER = logging.getLogger("ifc_app.cobieqc.bootstrap")

DEFAULT_JAR_SOURCE_URL = "https://drive.google.com/file/d/19wRbk-TNoHNOmRgqqDP4AjbRqawzE7wq/view?usp=drive_link"
DEFAULT_XML_ZIP_SOURCE_URL = "https://drive.google.com/file/d/1EKJWT7fHgTDJdt95nh0z2kxXhI3LBNJd/view?usp=drive_link"
DEFAULT_XML_FOLDER_SOURCE_URL = "https://drive.google.com/drive/folders/13ZYp5lb1B57nmPpLMZnCS3zP7I--zFjg?usp=drive_link"


@dataclass
class CobieQcBootstrapStatus:
    enabled: bool
    jar_exists: bool
    resource_dir_exists: bool
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


def _is_non_empty_file(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _dir_has_files(path: Path) -> bool:
    return path.exists() and path.is_dir() and any(child.is_file() for child in path.rglob("*"))


def is_nonempty_directory(path: Path) -> bool:
    return _dir_has_files(path)


def _resource_dir_resolution_candidates(preferred_resource_dir: Path, configured_resource_dir: Path) -> list[Path]:
    candidates: list[Path] = [preferred_resource_dir]
    if configured_resource_dir != preferred_resource_dir:
        candidates.append(configured_resource_dir)
    candidates.extend(cobieqc_resource_candidates())

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
        if not resolved.exists():
            continue
        if not resolved.is_dir():
            LOGGER.info("COBieQC bootstrap: skipping non-directory resource candidate %s", resolved)
            continue
        if not is_nonempty_directory(resolved):
            LOGGER.info("COBieQC bootstrap: resource directory exists but is empty at %s", resolved)
            continue
        source = "existing_dir" if idx == 0 else "fallback_dir"
        return resolved, source
    return None, "missing"


def _download_to_temp(source_url: str, suffix: str, purpose: str) -> Path:
    direct_url = google_drive_direct_download_url(source_url) if "drive.google.com" in source_url else source_url
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        if requests is not None:
            with requests.get(direct_url, stream=True, timeout=120) as response:
                response.raise_for_status()
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
    return temp_path


def _replace_file_atomically(source_file: Path, destination_file: Path) -> None:
    destination_file.parent.mkdir(parents=True, exist_ok=True)
    temp_destination = destination_file.with_suffix(destination_file.suffix + ".tmp")
    shutil.move(str(source_file), temp_destination)
    os.replace(temp_destination, destination_file)


def _resolve_xsl_xml_dir(extract_root: Path) -> Path:
    direct = extract_root / "xsl_xml"
    if direct.exists() and direct.is_dir():
        return direct

    nested_matches = [path for path in extract_root.rglob("xsl_xml") if path.is_dir()]
    if len(nested_matches) == 1:
        return nested_matches[0]

    top_dirs = [entry for entry in extract_root.iterdir() if entry.is_dir()]
    if len(top_dirs) == 1:
        wrapper_xsl = top_dirs[0] / "xsl_xml"
        if wrapper_xsl.exists() and wrapper_xsl.is_dir():
            return wrapper_xsl

    raise RuntimeError("Could not find xsl_xml directory in extracted ZIP")


def _replace_dir_atomically(source_dir: Path, destination_dir: Path) -> None:
    destination_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir = destination_dir.with_name(destination_dir.name + ".bak")
    temp_dir = destination_dir.with_name(destination_dir.name + ".tmp")
    shutil.rmtree(temp_dir, ignore_errors=True)
    shutil.move(str(source_dir), temp_dir)

    if destination_dir.exists():
        shutil.rmtree(backup_dir, ignore_errors=True)
        os.replace(destination_dir, backup_dir)

    os.replace(temp_dir, destination_dir)
    shutil.rmtree(backup_dir, ignore_errors=True)


def _install_xml_from_zip(zip_url: str, resource_dir: Path) -> None:
    zip_temp = _download_to_temp(zip_url, ".zip", "COBieQC XML ZIP")
    extract_temp_dir: Optional[Path] = None
    try:
        extract_temp_dir = Path(tempfile.mkdtemp(prefix="cobieqc-xml-"))
        with zipfile.ZipFile(zip_temp) as archive:
            archive.extractall(extract_temp_dir)
        normalized_xsl = _resolve_xsl_xml_dir(extract_temp_dir)
        _replace_dir_atomically(normalized_xsl, resource_dir)
    finally:
        zip_temp.unlink(missing_ok=True)
        if extract_temp_dir:
            shutil.rmtree(extract_temp_dir, ignore_errors=True)


def _build_status(
    last_error: str = "",
    resolved_resource_dir: Optional[Path] = None,
    resource_source: str = "missing",
) -> CobieQcBootstrapStatus:
    jar_path = _jar_path()
    resource_dir = (resolved_resource_dir or _resource_dir()).expanduser().resolve()
    jar_exists = _is_non_empty_file(jar_path)
    resource_exists = is_nonempty_directory(resource_dir)
    return CobieQcBootstrapStatus(
        enabled=jar_exists and resource_exists,
        jar_exists=jar_exists,
        resource_dir_exists=resource_exists,
        jar_path=str(jar_path),
        resource_dir=str(resource_dir),
        resource_source=resource_source if resource_exists else "missing",
        last_error=last_error,
    )


def get_cobieqc_bootstrap_status() -> dict:
    global _LAST_STATUS
    if _LAST_STATUS is None:
        _LAST_STATUS = _build_status()
    return asdict(_LAST_STATUS)


def bootstrap_cobieqc_assets() -> None:
    global _LAST_STATUS

    data_root = _data_root()
    jar_path = _jar_path()
    resource_dir = _resource_dir()
    preferred_resource_dir = (_data_root() / "xsl_xml").expanduser()
    jar_source_url = os.getenv("COBIEQC_JAR_SOURCE_URL", DEFAULT_JAR_SOURCE_URL).strip() or DEFAULT_JAR_SOURCE_URL
    xml_zip_source_url = os.getenv("COBIEQC_XML_ZIP_SOURCE_URL", DEFAULT_XML_ZIP_SOURCE_URL).strip() or DEFAULT_XML_ZIP_SOURCE_URL

    data_root.mkdir(parents=True, exist_ok=True)
    LOGGER.info("COBieQC bootstrap: data root ready at %s", data_root)

    errors: list[str] = []

    try:
        if _is_non_empty_file(jar_path):
            LOGGER.info("COBieQC bootstrap: existing JAR kept at %s", jar_path)
        else:
            LOGGER.info("COBieQC bootstrap: downloading JAR to %s", jar_path)
            jar_temp = _download_to_temp(jar_source_url, ".jar", "COBieQC JAR")
            _replace_file_atomically(jar_temp, jar_path)
            LOGGER.info("COBieQC bootstrap: JAR installed at %s", jar_path)
    except Exception as exc:
        errors.append(f"JAR download/install failed: {exc}")
        LOGGER.error("COBieQC bootstrap JAR install failed: %s", exc)

    resolved_resource_dir, resource_source = _resolve_existing_resource_dir(preferred_resource_dir, resource_dir)
    if resolved_resource_dir:
        LOGGER.info("COBieQC bootstrap: using existing extracted resources at %s", resolved_resource_dir)
    elif xml_zip_source_url:
        try:
            LOGGER.info("COBieQC bootstrap: downloading XML ZIP and extracting to %s", resource_dir)
            _install_xml_from_zip(xml_zip_source_url, resource_dir)
            LOGGER.info("COBieQC bootstrap: resources installed at %s", resource_dir)
            if is_nonempty_directory(resource_dir):
                resolved_resource_dir = resource_dir.expanduser().resolve()
                resource_source = "zip_extract"
            else:
                resource_source = "missing"
                LOGGER.warning(
                    "COBieQC bootstrap: ZIP extraction completed but resource directory is missing/empty at %s",
                    resource_dir,
                )
        except Exception as exc:
            fallback_resource_dir, fallback_source = _resolve_existing_resource_dir(preferred_resource_dir, resource_dir)
            if fallback_resource_dir:
                resolved_resource_dir = fallback_resource_dir
                resource_source = fallback_source
                LOGGER.warning(
                    "COBieQC bootstrap XML install failed, but existing extracted resources remain available at %s: %s",
                    fallback_resource_dir,
                    exc,
                )
            else:
                resource_source = "missing"
                errors.append(f"XML ZIP download/extract failed: {exc}")
                LOGGER.error("COBieQC bootstrap XML install failed: %s", exc)
    else:
        resource_source = "missing"
        LOGGER.warning("COBieQC bootstrap: XML ZIP source is not configured and no extracted resource directory is available")

    last_error = " | ".join(errors)
    _LAST_STATUS = _build_status(
        last_error=last_error,
        resolved_resource_dir=resolved_resource_dir,
        resource_source=resource_source,
    )
    LOGGER.info(
        "COBieQC bootstrap complete enabled=%s jar_exists=%s jar_path=%s resource_dir_exists=%s resource_dir=%s resource_source=%s",
        _LAST_STATUS.enabled,
        _LAST_STATUS.jar_exists,
        _LAST_STATUS.jar_path,
        _LAST_STATUS.resource_dir_exists,
        _LAST_STATUS.resource_dir,
        _LAST_STATUS.resource_source,
    )


__all__ = [
    "DEFAULT_JAR_SOURCE_URL",
    "DEFAULT_XML_FOLDER_SOURCE_URL",
    "DEFAULT_XML_ZIP_SOURCE_URL",
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
