import asyncio
import io

from fastapi import UploadFile

import app


def test_ifc_qa_run_starts_job_and_returns_success(monkeypatch):
    captured = {}

    def _fake_starter(session_root, session_id, file_records, options, config, mode):
        captured["session_id"] = session_id
        captured["file_records"] = file_records
        captured["mode"] = mode
        return "job-test-123"

    monkeypatch.setattr(app, "IFC_QA_JOB_STARTER", _fake_starter)
    monkeypatch.setattr(app, "has_active_ifc_qa_job", lambda: False)

    session_id = app.SESSION_STORE.create()
    upload = UploadFile(filename="sample.ifc", file=io.BytesIO(b"ISO-10303-21;"))

    payload = asyncio.run(
        app.ifc_qa_run(
            files=[upload],
            session_id=session_id,
            options_json="{}",
            config_override_json=None,
        )
    )

    assert payload["success"] is True
    assert payload["job_id"] == "job-test-123"
    assert payload["session_id"] == session_id
    assert payload["message"] == "Job started"

    assert captured["session_id"] == session_id
    assert captured["mode"] == "replace"
    assert captured["file_records"][0][0] == "sample.ifc"


def test_ifc_qa_job_starter_self_check_is_callable_and_v2_alias_removed():
    assert callable(app.IFC_QA_JOB_STARTER)
    assert not hasattr(app, "start_ifc_qa_v2_job")
