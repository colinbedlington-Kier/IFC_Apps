import os
import subprocess
from pathlib import Path
from typing import Dict

DEFAULT_TIMEOUT_SECONDS = int(os.getenv("COBIEQC_TIMEOUT_SECONDS", "300"))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_cobieqc(input_xlsx_path: str, stage: str, job_dir: str) -> Dict[str, object]:
    if stage not in {"D", "C"}:
        return {"ok": False, "stdout": "", "stderr": "", "error": "Stage must be D or C."}

    jar_dir = _repo_root() / "COBieQC" / "CobieQcReporter"
    jar_path = jar_dir / "CobieQcReporter.jar"
    output_filename = "report.html"
    output_html_path = Path(job_dir) / output_filename

    if not jar_path.exists():
        return {
            "ok": False,
            "stdout": "",
            "stderr": "",
            "error": f"COBieQC reporter JAR not found at {jar_path}",
        }

    cmd = [
        "java",
        "-jar",
        str(jar_path.name),
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
