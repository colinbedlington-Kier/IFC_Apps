import json
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def create_job(*, requested_by: Optional[str], input_files: Any, options: Any) -> Dict[str, Any]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into ifc_extraction_jobs(status, progress, message, requested_by, input_files, options)
            values ('queued', 0, 'Queued', %s, %s::jsonb, %s::jsonb)
            returning id, status, progress, message
            """,
            (requested_by, json.dumps(input_files), json.dumps(options)),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select * from ifc_extraction_jobs where id=%s", (job_id,))
        return cur.fetchone()


def update_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    cols = []
    vals = []
    for key, value in fields.items():
        cols.append(f"{key}=%s")
        vals.append(json.dumps(value) if key in {"result"} and value is not None else value)
    vals.append(job_id)
    sql = f"update ifc_extraction_jobs set {', '.join(cols)}, updated_at=now() where id=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, vals)
        conn.commit()


def claim_next_job(worker_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with next_job as (
              select id from ifc_extraction_jobs
              where status='queued'
              order by created_at
              for update skip locked
              limit 1
            )
            update ifc_extraction_jobs j
            set status='running', locked_at=now(), locked_by=%s, attempts=attempts+1, message='Worker started'
            from next_job
            where j.id = next_job.id
            returning j.*
            """,
            (worker_id,),
        )
        row = cur.fetchone()
        conn.commit()
        return row
