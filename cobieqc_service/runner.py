import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DEFAULT_TIMEOUT_SECONDS = int(os.getenv("COBIEQC_TIMEOUT_SECONDS", "300"))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _candidate_jar_paths() -> List[Path]:
    configured = os.getenv("COBIEQC_JAR_PATH", "").strip()
    candidates: List[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())

    # Legacy in-repo path
    candidates.append(_repo_root() / "COBieQC" / "CobieQcReporter" / "CobieQcReporter.jar")

    # Common container-mounted paths
    candidates.append(Path("/app/COBieQC/CobieQcReporter/CobieQcReporter.jar"))
    candidates.append(Path("/app/CobieQcReporter/CobieQcReporter.jar"))
    candidates.append(Path("/opt/COBieQC/CobieQcReporter/CobieQcReporter.jar"))

    # Preserve order but remove duplicates
    deduped: List[Path] = []
    seen = set()
    for c in candidates:
        key = str(c)
        if key not in seen:
            deduped.append(c)
            seen.add(key)
    return deduped


def resolve_cobieqc_jar() -> Tuple[Optional[Path], List[Path]]:
    attempted = _candidate_jar_paths()
    for candidate in attempted:
        resolved = candidate.resolve()
        if resolved.exists() and resolved.is_file():
            return resolved, attempted
    return None, attempted


def run_cobieqc(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
    if stage not in {"D", "C"}:
        return {"ok": False, "stdout": "", "stderr": "", "error": "Stage must be D or C."}

    jar_path, attempted_paths = resolve_cobieqc_jar()
    output_filename = "report.html"
    output_html_path = Path(job_dir) / output_filename

    if not jar_path:
        attempted_text = ", ".join(str(p) for p in attempted_paths)
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": (
                "COBieQC reporter JAR not found. Set COBIEQC_JAR_PATH or place "
                f"CobieQcReporter.jar in one of: {attempted_text}"
            ),
        }

    jar_dir = jar_path.parent

    cmd = [
        "java",
        "-jar",
        str(jar_path),
        "-i",
        str(Path(input_xlsx_path).resolve()),
        "-o",
        str(output_html_path.resolve()),
        "-p",
        stage,
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(jar_dir),
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
