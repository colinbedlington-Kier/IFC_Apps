import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

LOGGER = logging.getLogger("ifc_app.cobieqc")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("COBIEQC_TIMEOUT_SECONDS", "300"))


def _dedupe_paths(paths: List[Path]) -> List[Path]:
    deduped: List[Path] = []
    seen = set()
    for item in paths:
        key = str(item)
        if key in seen:
            continue
        deduped.append(item)
        seen.add(key)
    return deduped


def cobieqc_jar_candidates() -> List[Path]:
    configured = os.getenv("COBIEQC_JAR_PATH", "").strip()
    candidates: List[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())

    candidates.extend(
        [
            Path("/app/data/cobieqc/CobieQcReporter.jar"),
            Path("/app/data/CobieQcReporter.jar"),
            Path("/data/cobieqc/CobieQcReporter.jar"),
            Path("/app/CobieQcReporter/CobieQcReporter.jar"),
            Path("/app/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
            Path("/opt/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
        ]
    )
    return _dedupe_paths(candidates)


def cobieqc_resource_candidates() -> List[Path]:
    configured = os.getenv("COBIEQC_RESOURCE_DIR", "").strip()
    candidates: List[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())

    candidates.extend(
        [
            Path("/app/data/xsl_xml"),
            Path("/data/xsl_xml"),
            Path("/data/cobieqc/xsl_xml"),
            Path("/app/CobieQcReporter/xsl_xml"),
            Path("/app/COBieQC/CobieQcReporter/xsl_xml"),
            Path("/opt/COBieQC/CobieQcReporter/xsl_xml"),
        ]
    )
    return _dedupe_paths(candidates)


def _resolve_existing_path(candidates: List[Path], expected_kind: str, label: str) -> Path:
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        exists = resolved.exists()
        LOGGER.info("COBieQC %s check: %s (exists=%s)", label, resolved, exists)
        if not exists:
            continue
        if expected_kind == "file" and resolved.is_file():
            LOGGER.info("COBieQC %s selected: %s", label, resolved)
            return resolved
        if expected_kind == "dir" and resolved.is_dir():
            LOGGER.info("COBieQC %s selected: %s", label, resolved)
            return resolved
        LOGGER.info("COBieQC %s skipped (wrong type): %s", label, resolved)

    attempted = ", ".join(str(p.expanduser()) for p in candidates)
    if label == "JAR path":
        cwd = Path.cwd()
        app_data_exists = Path("/app/data").exists()
        raise RuntimeError(
            f"COBieQC JAR path not found. Checked: {attempted}. "
            f"Current working directory: {cwd}. /app/data exists: {app_data_exists}."
        )
    raise RuntimeError(f"COBieQC {label} not found. Checked: {attempted}")


def resolve_cobieqc_jar_path() -> Path:
    return _resolve_existing_path(cobieqc_jar_candidates(), expected_kind="file", label="JAR path")


def resolve_cobieqc_resource_dir() -> Path:
    return _resolve_existing_path(cobieqc_resource_candidates(), expected_kind="dir", label="resource dir")


def _resource_file_counts(resource_dir: Path) -> Dict[str, int]:
    xml_count = sum(1 for p in resource_dir.rglob("*.xml") if p.is_file())
    xsl_count = sum(1 for p in resource_dir.rglob("*.xsl") if p.is_file())
    return {"xml_count": xml_count, "xsl_count": xsl_count}


def _log_preflight_diagnostics(
    jar_candidates: List[Path],
    resource_candidates: List[Path],
    resolved_resource_dir: Optional[Path] = None,
) -> None:
    cwd = Path.cwd()
    LOGGER.info("COBieQC preflight cwd=%s", cwd)

    for jar_candidate in jar_candidates:
        candidate = jar_candidate.expanduser().resolve()
        LOGGER.info("COBieQC preflight jar candidate: %s exists=%s", candidate, candidate.exists())

    for resource_candidate in resource_candidates:
        candidate = resource_candidate.expanduser().resolve()
        LOGGER.info("COBieQC preflight resource candidate: %s exists=%s", candidate, candidate.exists())

    app_data = Path("/app/data")
    if app_data.exists() and app_data.is_dir():
        children = sorted(child.name for child in app_data.iterdir())
        LOGGER.info("COBieQC preflight /app/data children: %s", children)

    if resolved_resource_dir and resolved_resource_dir.exists() and resolved_resource_dir.is_dir():
        counts = _resource_file_counts(resolved_resource_dir)
        LOGGER.info(
            "COBieQC preflight resolved resource counts dir=%s xml_count=%s xsl_count=%s",
            resolved_resource_dir,
            counts["xml_count"],
            counts["xsl_count"],
        )


def get_cobieqc_runtime_diagnostics() -> Dict[str, object]:
    jar_candidates = [str(p) for p in cobieqc_jar_candidates()]
    resource_candidates = [str(p) for p in cobieqc_resource_candidates()]

    jar_path: Optional[Path] = None
    resource_dir: Optional[Path] = None
    jar_error = ""
    resource_error = ""

    try:
        jar_path = resolve_cobieqc_jar_path()
    except RuntimeError as exc:
        jar_error = str(exc)

    try:
        resource_dir = resolve_cobieqc_resource_dir()
    except RuntimeError as exc:
        resource_error = str(exc)

    counts = {"xml_count": 0, "xsl_count": 0}
    if resource_dir:
        counts = _resource_file_counts(resource_dir)

    return {
        "jar_exists": bool(jar_path),
        "resource_dir_exists": bool(resource_dir),
        "jar_path": str(jar_path) if jar_path else None,
        "resource_dir": str(resource_dir) if resource_dir else None,
        "jar_candidates": jar_candidates,
        "resource_candidates": resource_candidates,
        "xml_count": counts["xml_count"],
        "xsl_count": counts["xsl_count"],
        "jar_error": jar_error,
        "resource_error": resource_error,
    }


def _build_cobieqc_cmd(
    jar_path: Path,
    input_xlsx_path: Path,
    output_html_path: Path,
    stage: str,
    resource_dir: Path,
    include_resource_arg: bool,
) -> List[str]:
    cmd = [
        "java",
        "-jar",
        str(jar_path),
        "-i",
        str(input_xlsx_path),
        "-o",
        str(output_html_path),
        "-p",
        stage,
    ]

    if include_resource_arg:
        resource_flag = os.getenv("COBIEQC_RESOURCE_ARG", "--resource-dir").strip() or "--resource-dir"
        cmd.extend([resource_flag, str(resource_dir)])

    return cmd


def _java_executable() -> str:
    explicit = os.getenv("JAVA_BIN", "").strip()
    if explicit:
        return explicit
    return shutil.which("java") or "java"


def run_cobieqc(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
    if stage not in {"D", "C"}:
        return {"ok": False, "stdout": "", "stderr": "", "error": "Stage must be D or C."}

    input_path = Path(input_xlsx_path).resolve()
    output_filename = "report.html"
    output_html_path = Path(job_dir).resolve() / output_filename

    jar_candidates = cobieqc_jar_candidates()
    resource_candidates = cobieqc_resource_candidates()

    try:
        jar_path = _resolve_existing_path(jar_candidates, expected_kind="file", label="JAR path")
        resource_dir = _resolve_existing_path(resource_candidates, expected_kind="dir", label="resource dir")
    except RuntimeError as exc:
        _log_preflight_diagnostics(jar_candidates, resource_candidates)
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": str(exc),
        }

    _log_preflight_diagnostics(jar_candidates, resource_candidates, resolved_resource_dir=resource_dir)

    java_bin = _java_executable()
    include_resource_arg = os.getenv("COBIEQC_PASS_RESOURCE_ARG", "1").lower() not in {"0", "false", "no"}
    cmd = _build_cobieqc_cmd(jar_path, input_path, output_html_path, stage, resource_dir, include_resource_arg)

    LOGGER.info(
        "COBieQC execution context stage=%s java=%s jar=%s resources=%s input=%s cwd=%s cmd=%s",
        stage,
        java_bin,
        jar_path,
        resource_dir,
        input_path,
        resource_dir.parent,
        cmd,
    )
    cmd[0] = java_bin

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(resource_dir.parent),
            capture_output=True,
            text=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": "Java runtime not found. Ensure java is installed and available in PATH.",
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "error": f"COBieQC timed out after {DEFAULT_TIMEOUT_SECONDS} seconds.",
        }

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    if proc.returncode != 0 and include_resource_arg:
        combined = f"{stdout}\n{stderr}".lower()
        if any(token in combined for token in ["unrecognized option", "unknown option", "invalid option"]):
            LOGGER.warning("COBieQC reporter rejected resource arg; retrying without explicit resource arg")
            retry_cmd = _build_cobieqc_cmd(jar_path, input_path, output_html_path, stage, resource_dir, False)
            retry_cmd[0] = java_bin
            LOGGER.info("COBieQC retry command=%s", retry_cmd)
            proc = subprocess.run(
                retry_cmd,
                cwd=str(resource_dir.parent),
                capture_output=True,
                text=True,
                timeout=DEFAULT_TIMEOUT_SECONDS,
                check=False,
            )
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""

    if proc.returncode != 0:
        return {
            "ok": False,
            "stdout": stdout,
            "stderr": stderr,
            "error": f"COBieQC exited with code {proc.returncode}.",
        }

    if not output_html_path.exists() or output_html_path.stat().st_size <= 0:
        return {
            "ok": False,
            "stdout": stdout,
            "stderr": stderr,
            "error": "COBieQC did not produce a non-empty HTML report.",
        }

    return {
        "ok": True,
        "stdout": stdout,
        "stderr": stderr,
        "output_html": str(output_html_path.resolve()),
        "output_filename": output_filename,
        "error": "",
    }


# Backward-compatible alias for any existing imports.
def resolve_cobieqc_jar() -> tuple[Optional[Path], List[Path]]:
    candidates = cobieqc_jar_candidates()
    try:
        return resolve_cobieqc_jar_path(), candidates
    except RuntimeError:
        return None, candidates
