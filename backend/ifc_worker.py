import os
import socket
import time
from pathlib import Path

from app import run_data_extractor_job
from backend.ifc_jobs import claim_next_job, update_job

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"
POLL_SECONDS = float(os.getenv("IFC_WORKER_POLL_SECONDS", "2"))


def _progress(job_id: str, payload):
    mapped = {k: v for k, v in payload.items() if k in {"status", "progress", "message", "error", "result"}}
    if "done" in payload and payload.get("done"):
        mapped["status"] = "failed" if payload.get("error") else "done"
    if payload.get("outputs"):
        mapped["result"] = {"outputs": payload.get("outputs"), "preview": payload.get("preview")}
    update_job(job_id, **mapped)


def run_once() -> bool:
    job = claim_next_job(WORKER_ID)
    if not job:
        return False
    job_id = str(job["id"])
    try:
        options = job.get("options") or {}
        input_files = job.get("input_files") or []
        session_id = options.get("session_id")
        if not session_id:
            raise ValueError("Missing session_id in job options")
        run_data_extractor_job(
            job_id,
            session_id,
            [f["name"] for f in input_files],
            options.get("exclude_path"),
            options.get("pset_path"),
            options.get("tables") or [],
            options.get("regexes") or {},
            progress_callback=lambda p: _progress(job_id, p),
        )
    except Exception as exc:
        update_job(job_id, status="failed", message="Worker failed", error=str(exc))
    return True


def main() -> None:
    while True:
        claimed = run_once()
        if not claimed:
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
