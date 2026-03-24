import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

LOGGER = logging.getLogger("ifc_app.cobieqc")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("COBIEQC_TIMEOUT_SECONDS", "300"))
APP_ROOT = Path(__file__).resolve().parents[1]
COBIEQC_RUNNER_BUILD_MARKER = "2026-03-24-flags-short-form"
COBIEQC_RUNNER_FLAG_MARKER = "flags=-i,-o,-p"

LOGGER.info(
    "COBieQC runner version marker: %s build_marker=%s file=%s",
    COBIEQC_RUNNER_FLAG_MARKER,
    COBIEQC_RUNNER_BUILD_MARKER,
    __file__,
)


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
            APP_ROOT / "vendor" / "cobieqc" / "CobieQcReporter.jar",
            APP_ROOT / "CobieQcReporter" / "CobieQcReporter.jar",
            APP_ROOT / "COBieQC" / "CobieQcReporter" / "CobieQcReporter.jar",
            Path.cwd() / "vendor" / "cobieqc" / "CobieQcReporter.jar",
            Path.cwd() / "CobieQcReporter" / "CobieQcReporter.jar",
            Path.cwd() / "COBieQC" / "CobieQcReporter" / "CobieQcReporter.jar",
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
            APP_ROOT / "vendor" / "cobieqc" / "xsl_xml",
            APP_ROOT / "CobieQcReporter" / "xsl_xml",
            APP_ROOT / "COBieQC" / "CobieQcReporter" / "xsl_xml",
            Path.cwd() / "vendor" / "cobieqc" / "xsl_xml",
            Path.cwd() / "CobieQcReporter" / "xsl_xml",
            Path.cwd() / "COBieQC" / "CobieQcReporter" / "xsl_xml",
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
    java_bin: str,
    jar_path: Path,
    input_xlsx_path: Path,
    output_html_path: Path,
    stage: str,
) -> List[str]:
    cmd = [
        java_bin,
        "-jar",
        str(jar_path),
        "-i_Input",
        str(input_xlsx_path),
        "-o_Output",
        str(output_html_path),
        "-p_Phase",
        stage,
    ]
    return cmd


def _java_executable() -> str:
    explicit = os.getenv("JAVA_BIN", "").strip()
    if explicit:
        return explicit

    java_home = os.getenv("JAVA_HOME", "").strip()
    candidates: List[Path] = []
    if java_home:
        candidates.append(Path(java_home).expanduser() / "bin" / "java")

    candidates.extend(
        [
            Path("/usr/bin/java"),
            Path("/usr/local/bin/java"),
            Path("/usr/lib/jvm/default-java/bin/java"),
            Path("/usr/lib/jvm/java-21-openjdk-amd64/bin/java"),
            Path("/usr/lib/jvm/java-11-openjdk-amd64/bin/java"),
        ]
    )
    for candidate in candidates:
        expanded = candidate.expanduser()
        if expanded.exists() and expanded.is_file():
            return str(expanded)

    return shutil.which("java") or "java"


def resolve_java_executable() -> str:
    return _java_executable()


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
    cmd = _build_cobieqc_cmd(java_bin, jar_path, input_path, output_html_path, stage)

    LOGGER.info(
        "COBieQC execution context stage=%s java=%s jar=%s resources=%s input=%s cwd=%s cmd=%s runner_file=%s build_marker=%s",
        stage,
        java_bin,
        jar_path,
        resource_dir,
        input_path,
        resource_dir.parent,
        cmd,
        __file__,
        COBIEQC_RUNNER_BUILD_MARKER,
    )
    LOGGER.info(
        "COBieQC final argv=%s runner_file=%s COBIEQC_RUNNER_BUILD_MARKER=%s",
        cmd,
        __file__,
        COBIEQC_RUNNER_BUILD_MARKER,
    )

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
    LOGGER.info(
        "COBieQC process completed exit_code=%s stdout=%s stderr=%s",
        proc.returncode,
        stdout,
        stderr,
    )

    if proc.returncode != 0:
        combined = f"{stdout}\n{stderr}".lower()
        usage_hint = ""
        if "usage:" in combined and "required" in combined:
            usage_hint = " COBieQC CLI usage was printed; command arguments likely mismatched the jar version."
        return {
            "ok": False,
            "stdout": stdout,
            "stderr": stderr,
            "error": f"COBieQC exited with code {proc.returncode}.{usage_hint}",
        }

    if not output_html_path.exists() or output_html_path.stat().st_size <= 0:
        output_exists = output_html_path.exists()
        output_size = output_html_path.stat().st_size if output_exists else 0
        input_exists = input_path.exists()
        input_size = input_path.stat().st_size if input_exists else 0
        jar_exists = jar_path.exists()
        jar_size = jar_path.stat().st_size if jar_exists else 0
        LOGGER.error(
            "COBieQC missing/empty output. cmd=%s input_exists=%s input_size=%s "
            "output_exists=%s output_size=%s jar_exists=%s jar_size=%s",
            cmd,
            input_exists,
            input_size,
            output_exists,
            output_size,
            jar_exists,
            jar_size,
        )
        return {
            "ok": False,
            "stdout": stdout,
            "stderr": stderr,
            "error": (
                "COBieQC did not produce a non-empty HTML report. "
                f"cmd={cmd} input_exists={input_exists} input_size={input_size} "
                f"output_exists={output_exists} output_size={output_size} "
                f"jar_exists={jar_exists} jar_size={jar_size}"
            ),
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
