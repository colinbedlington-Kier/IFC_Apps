import logging
import os
import shutil
import shlex
import subprocess
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ifc_app.cobieqc_native import run_cobieqc_native

LOGGER = logging.getLogger("ifc_app.cobieqc")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("COBIEQC_TIMEOUT_SECONDS", "300"))
APP_ROOT = Path(__file__).resolve().parents[1]
COBIEQC_DATA_ROOT = Path(os.getenv("COBIEQC_DATA_DIR", "/data/cobieqc")).expanduser()
COBIEQC_ENGINE_ENV = "COBIEQC_ENGINE"
COBIEQC_RUNNER_BUILD_MARKER = "2026-04-10-jvm-memory-guardrails"
COBIEQC_RUNNER_FLAG_MARKER = "flags=-i,-o,-p"
COBIEQC_INPUT_ARG = "-i"
COBIEQC_OUTPUT_ARG = "-o"
COBIEQC_PHASE_ARG = "-p"
COBIEQC_JAVA_LOCK = threading.Lock()
COBIEQC_DEFAULT_JAVA_XMS = "128m"
COBIEQC_DEFAULT_JAVA_XMX_MB = int(os.getenv("COBIEQC_JAVA_XMX_MB", "512"))
COBIEQC_DEFAULT_CONTAINER_SUPPORT_FLAG = "-XX:+UseContainerSupport"
COBIEQC_JAVA_DIAGNOSTIC_FLAGS = [
    "-XX:+PrintGCDetails",
    "-XX:+PrintGCDateStamps",
    "-XX:+HeapDumpOnOutOfMemoryError",
]
COBIEQC_REQUIRED_RESOURCE_FILES = [
    "SpaceReport.css",
    "iso_svrl_for_xslt2.xsl",
    "COBieExcelTemplate.xml",
    "COBieRules.sch",
    "iso_schematron_skeleton_for_saxon.xsl",
    "SVRL_HTML_altLocation.xslt",
    "COBieRules_Functions.xsl",
    "_SVRL_HTML_altLocation.xslt",
]

LOGGER.info(
    "COBieQC runner version marker: %s build_marker=%s file=%s",
    COBIEQC_RUNNER_FLAG_MARKER,
    COBIEQC_RUNNER_BUILD_MARKER,
    __file__,
)


def get_cobieqc_engine() -> str:
    return os.getenv(COBIEQC_ENGINE_ENV, "python").strip().lower() or "python"


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
            COBIEQC_DATA_ROOT / "CobieQcReporter.jar",
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
            COBIEQC_DATA_ROOT / "xsl_xml",
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
        LOGGER.debug("COBieQC %s check: %s (exists=%s)", label, resolved, exists)
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


def _resource_file_counts(resource_dir: Path) -> Dict[str, int]:
    xml_count = sum(1 for p in resource_dir.rglob("*.xml") if p.is_file())
    xsl_count = sum(1 for p in resource_dir.rglob("*.xsl") if p.is_file())
    return {"xml_count": xml_count, "xsl_count": xsl_count}


def validate_cobieqc_resource_dir(path: Path) -> Dict[str, object]:
    resolved = path.expanduser().resolve()
    result: Dict[str, object] = {
        "path": str(resolved),
        "exists": resolved.exists(),
        "is_dir": resolved.is_dir(),
        "has_files": False,
        "xml_count": 0,
        "xsl_count": 0,
        "valid": False,
        "missing": [],
        "missing_required_files": [],
    }
    missing: List[str] = []
    missing_required_files: List[str] = []
    if not resolved.exists():
        missing.append("directory not found")
    elif not resolved.is_dir():
        missing.append("path is not a directory")
    else:
        has_files = any(child.is_file() for child in resolved.rglob("*"))
        result["has_files"] = has_files
        counts = _resource_file_counts(resolved)
        result["xml_count"] = counts["xml_count"]
        result["xsl_count"] = counts["xsl_count"]
        if not has_files:
            missing.append("directory is empty")
        if counts["xml_count"] == 0:
            missing.append("missing *.xml resource files")
        if counts["xsl_count"] == 0:
            missing.append("missing *.xsl resource files")
        for required_file in COBIEQC_REQUIRED_RESOURCE_FILES:
            required_path = resolved / required_file
            if not required_path.exists() or not required_path.is_file() or required_path.stat().st_size == 0:
                missing_required_files.append(required_file)
        if missing_required_files:
            missing.append(
                "missing required COBieQC resources: " + ", ".join(missing_required_files)
            )
    result["missing"] = missing
    result["missing_required_files"] = missing_required_files
    result["valid"] = len(missing) == 0
    return result


def resolve_cobieqc_resource_dir() -> Path:
    candidates = cobieqc_resource_candidates()
    invalid_reasons: List[str] = []
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        validation = validate_cobieqc_resource_dir(resolved)
        LOGGER.debug("COBieQC resource dir validation: %s", validation)
        if validation["valid"]:
            LOGGER.info("COBieQC resource dir selected: %s", resolved)
            return resolved
        if validation["exists"]:
            invalid_reasons.append(f"{resolved} ({'; '.join(validation['missing'])})")

    attempted = ", ".join(str(p.expanduser()) for p in candidates)
    detail = f" Invalid candidates: {', '.join(invalid_reasons)}." if invalid_reasons else ""
    raise RuntimeError(f"COBieQC resource dir not found or missing expected files. Checked: {attempted}.{detail}")


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

    selected_engine = get_cobieqc_engine()
    if selected_engine == "java":
        enabled = bool(jar_path and resource_dir)
    else:
        enabled = bool(resource_dir)
    return {
        "enabled": enabled,
        "engine": selected_engine,
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
    jvm_args, _, _ = _effective_jvm_args()
    cmd = [
        java_bin,
        *jvm_args,
        "-jar",
        str(jar_path),
        COBIEQC_INPUT_ARG,
        str(input_xlsx_path),
        COBIEQC_OUTPUT_ARG,
        str(output_html_path),
        COBIEQC_PHASE_ARG,
        stage,
    ]
    return cmd


def _read_container_memory_bytes() -> Optional[int]:
    candidates = [
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ]
    for candidate in candidates:
        try:
            if not candidate.exists() or not candidate.is_file():
                continue
            raw = candidate.read_text(encoding="utf-8").strip()
            if not raw or raw == "max":
                continue
            value = int(raw)
            if value > 0:
                return value
        except (ValueError, OSError):
            continue
    return None


def _java_mem_to_bytes(value: str) -> int:
    raw = (value or "").strip().lower()
    if not raw:
        raise ValueError("empty memory value")
    multipliers = {
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }
    suffix = raw[-1]
    if suffix in multipliers:
        return int(raw[:-1]) * multipliers[suffix]
    return int(raw)


def _effective_jvm_args() -> Tuple[List[str], str, str]:
    xms = os.getenv("COBIEQC_JAVA_XMS", COBIEQC_DEFAULT_JAVA_XMS).strip() or COBIEQC_DEFAULT_JAVA_XMS
    configured_xmx = os.getenv("COBIEQC_JAVA_XMX", "").strip()
    explicit_xmx = bool(configured_xmx)
    if not configured_xmx:
        xmx_mb_raw = os.getenv("COBIEQC_JAVA_XMX_MB", str(COBIEQC_DEFAULT_JAVA_XMX_MB)).strip()
        xmx = f"{int(xmx_mb_raw)}m"
    else:
        xmx = configured_xmx
    xms_bytes = _java_mem_to_bytes(xms)
    xmx_bytes = _java_mem_to_bytes(xmx)
    container_memory_bytes = _read_container_memory_bytes()
    if container_memory_bytes and not explicit_xmx:
        safe_cap = int(container_memory_bytes * 0.75)
        if xmx_bytes > safe_cap:
            LOGGER.warning(
                "COBieQC Xmx (%s bytes) exceeds 75%% of container memory (%s bytes); clamping.",
                xmx_bytes,
                container_memory_bytes,
            )
            xmx_bytes = safe_cap
            xmx = f"{max(256, xmx_bytes // (1024 * 1024))}m"
        if xms_bytes > xmx_bytes:
            xms_bytes = min(xms_bytes, xmx_bytes)
            xms = f"{max(128, xms_bytes // (1024 * 1024))}m"
    if xms_bytes > xmx_bytes:
        raise ValueError(f"Invalid COBieQC JVM heap settings: Xms ({xms}) must be <= Xmx ({xmx}).")
    return (
        [
            f"-Xms{xms}",
            f"-Xmx{xmx}",
            COBIEQC_DEFAULT_CONTAINER_SUPPORT_FLAG,
            *COBIEQC_JAVA_DIAGNOSTIC_FLAGS,
        ],
        xms,
        xmx,
    )


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


def _run_cobieqc_java(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
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

    try:
        _, configured_xms, configured_xmx = _effective_jvm_args()
    except ValueError as exc:
        LOGGER.error("COBieQC JVM configuration error: %s", exc)
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": str(exc),
        }

    java_bin = _java_executable()
    cmd = _build_cobieqc_cmd(java_bin, jar_path, input_path, output_html_path, stage)
    jvm_args = cmd[1 : cmd.index("-jar")]
    container_memory_bytes = _read_container_memory_bytes()

    LOGGER.info(
        "COBieQC execution context stage=%s java=%s jar=%s resources=%s input=%s cwd=%s "
        "container_memory_bytes=%s xms=%s xmx=%s jvm_args=%s cmd=%s runner_file=%s build_marker=%s",
        stage,
        java_bin,
        jar_path,
        resource_dir,
        input_path,
        resource_dir.parent,
        container_memory_bytes,
        configured_xms,
        configured_xmx,
        jvm_args,
        cmd,
        __file__,
        COBIEQC_RUNNER_BUILD_MARKER,
    )
    LOGGER.info("COBieQC java launch command: %s", " ".join(cmd))
    LOGGER.info(
        "COBieQC final argv=%s runner_file=%s COBIEQC_RUNNER_BUILD_MARKER=%s flag_marker=%s",
        cmd,
        __file__,
        COBIEQC_RUNNER_BUILD_MARKER,
        COBIEQC_RUNNER_FLAG_MARKER,
    )

    try:
        if COBIEQC_JAVA_LOCK.locked():
            LOGGER.info("COBieQC java lock busy; waiting for active process to finish")
        with COBIEQC_JAVA_LOCK:
            proc = subprocess.Popen(
                cmd,
                cwd=str(resource_dir.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            LOGGER.info("COBieQC java process started pid=%s", proc.pid)
            try:
                stdout, stderr = proc.communicate(timeout=DEFAULT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                return {
                    "ok": False,
                    "stdout": stdout or "",
                    "stderr": stderr or "",
                    "error": f"COBieQC timed out after {DEFAULT_TIMEOUT_SECONDS} seconds.",
                }
    except FileNotFoundError:
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": "Java runtime not found. Ensure java is installed and available in PATH.",
        }

    stdout = stdout or ""
    stderr = stderr or ""
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
            "output_exists=%s output_size=%s jar_exists=%s jar_size=%s "
            "runner_file=%s build_marker=%s flag_marker=%s",
            cmd,
            input_exists,
            input_size,
            output_exists,
            output_size,
            jar_exists,
            jar_size,
            __file__,
            COBIEQC_RUNNER_BUILD_MARKER,
            COBIEQC_RUNNER_FLAG_MARKER,
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


def _run_cobieqc_python(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
    try:
        resource_dir = resolve_cobieqc_resource_dir()
    except RuntimeError as exc:
        return {"ok": False, "stdout": "", "stderr": "", "error": str(exc)}

    result = run_cobieqc_native(
        input_xlsx_path=input_xlsx_path,
        stage=stage,
        job_dir=job_dir,
        resources_dir=resource_dir,
    )
    return {
        "ok": result.ok,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "output_html": result.output_html,
        "output_filename": result.output_filename,
        "cobie_xml": result.cobie_xml,
        "svrl_xml": result.svrl_xml,
        "summary": result.summary,
        "error": result.error,
    }


def run_cobieqc(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
    engine = get_cobieqc_engine()
    if engine == "java":
        return _run_cobieqc_java(input_xlsx_path, stage, job_dir)
    return _run_cobieqc_python(input_xlsx_path, stage, job_dir)


# Backward-compatible alias for any existing imports.
def resolve_cobieqc_jar() -> tuple[Optional[Path], List[Path]]:
    candidates = cobieqc_jar_candidates()
    try:
        return resolve_cobieqc_jar_path(), candidates
    except RuntimeError:
        return None, candidates
