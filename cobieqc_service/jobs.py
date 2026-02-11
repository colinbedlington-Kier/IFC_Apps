import datetime
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_ERROR = "error"


class CobieQcJobStore:
    def __init__(self, base_dir: str | None = None, ttl_hours: int = 24) -> None:
        data_root = Path(base_dir or os.getenv("IFC_APP_DATA_DIR") or Path(tempfile.gettempdir()) / "ifc_app_data")
        self.base_dir = data_root / "jobs" / "cobieqc"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_hours = int(os.getenv("COBIEQC_JOB_TTL_HOURS", str(ttl_hours)))

    def create_job(self, stage: str, original_filename: str) -> Dict[str, Any]:
        now = datetime.datetime.utcnow().isoformat() + "Z"
        job_id = uuid.uuid4().hex
        job_dir = self.base_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "job_id": job_id,
            "status": STATUS_QUEUED,
            "stage": stage,
            "progress": 0.0,
            "message": "Queued",
            "original_filename": original_filename,
            "input_filename": "input.xlsx",
            "output_filename": "report.html",
            "started_at": None,
            "finished_at": None,
            "created_at": now,
            "updated_at": now,
        }
        self._write_job_json(job_id, payload)
        self.append_log(job_id, f"[{now}] Job created")
        return payload

    def get_job_dir(self, job_id: str) -> Path:
        return self.base_dir / job_id

    def job_exists(self, job_id: str) -> bool:
        return self.get_job_json_path(job_id).exists()

    def get_job_json_path(self, job_id: str) -> Path:
        return self.get_job_dir(job_id) / "job.json"

    def get_logs_path(self, job_id: str) -> Path:
        return self.get_job_dir(job_id) / "logs.txt"

    def _write_job_json(self, job_id: str, payload: Dict[str, Any]) -> None:
        self.get_job_json_path(job_id).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def read_job(self, job_id: str) -> Dict[str, Any]:
        path = self.get_job_json_path(job_id)
        if not path.exists():
            raise FileNotFoundError("Job not found")
        return json.loads(path.read_text(encoding="utf-8"))

    def update_job(self, job_id: str, **updates: Any) -> Dict[str, Any]:
        payload = self.read_job(job_id)
        payload.update(updates)
        payload["updated_at"] = datetime.datetime.utcnow().isoformat() + "Z"
        self._write_job_json(job_id, payload)
        return payload

    def append_log(self, job_id: str, line: str) -> None:
        with self.get_logs_path(job_id).open("a", encoding="utf-8") as handle:
            handle.write(line.rstrip("\n") + "\n")

    def logs_tail(self, job_id: str, max_chars: int = 8000) -> str:
        path = self.get_logs_path(job_id)
        if not path.exists():
            return ""
        content = path.read_text(encoding="utf-8", errors="replace")
        return content[-max_chars:]

    def cleanup_old_jobs(self) -> int:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=self.ttl_hours)
        removed = 0
        for entry in self.base_dir.iterdir():
            if not entry.is_dir():
                continue
            try:
                mtime = datetime.datetime.utcfromtimestamp(entry.stat().st_mtime)
                if mtime < cutoff:
                    shutil.rmtree(entry, ignore_errors=True)
                    removed += 1
            except Exception:
                continue
        return removed
