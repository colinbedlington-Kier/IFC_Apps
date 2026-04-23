import asyncio
import io
import json
from pathlib import Path

import pytest
from fastapi import HTTPException, UploadFile

import app


def _upload(name: str, size: int) -> UploadFile:
    return UploadFile(filename=name, file=io.BytesIO(b"x" * size))


def test_session_upload_succeeds_just_under_limit(monkeypatch):
    monkeypatch.setattr(app, "MAX_UPLOAD_BYTES", 10)
    session_id = app.SESSION_STORE.create()

    payload = asyncio.run(app.upload_files(session_id, [_upload("ok.ifc", 10)]))

    assert payload["files"][0]["name"] == "ok.ifc"
    assert payload["files"][0]["size"] == 10


def test_session_upload_fails_just_over_limit_with_structured_error(monkeypatch):
    monkeypatch.setattr(app, "MAX_UPLOAD_BYTES", 10)
    session_id = app.SESSION_STORE.create()

    with pytest.raises(HTTPException) as exc:
        asyncio.run(app.upload_files(session_id, [_upload("too_big.ifc", 11)]))

    assert exc.value.status_code == 413
    assert exc.value.detail["code"] == "UPLOAD_TOO_LARGE"
    assert exc.value.detail["max_bytes"] == 10


def test_ifc_qa_run_returns_structured_error_for_oversize(monkeypatch):
    monkeypatch.setattr(app, "MAX_UPLOAD_BYTES", 10)
    monkeypatch.setattr(app, "has_active_ifc_qa_job", lambda: False)
    session_id = app.SESSION_STORE.create()

    response = asyncio.run(
        app.ifc_qa_run(
            files=[_upload("qa_big.ifc", 11)],
            session_id=session_id,
            options_json="{}",
            config_override_json=None,
        )
    )

    assert response.status_code == 413
    body = json.loads(response.body.decode("utf-8"))
    assert body["code"] == "UPLOAD_TOO_LARGE"
    assert body["max_bytes"] == 10
    assert app.SESSION_STORE.exists(session_id)


def test_ifc_qa_add_to_zip_uses_same_upload_limit(monkeypatch):
    monkeypatch.setattr(app, "MAX_UPLOAD_BYTES", 10)
    monkeypatch.setattr(app, "has_active_ifc_qa_job", lambda: False)
    session_id = app.SESSION_STORE.create()

    with pytest.raises(HTTPException) as exc:
        asyncio.run(app.ifc_qa_add_to_zip(files=[_upload("zip_big.ifc", 11)], session_id=session_id))

    assert exc.value.status_code == 413
    assert exc.value.detail["code"] == "UPLOAD_TOO_LARGE"


def test_frontend_shows_maximum_file_size_text():
    root = Path(__file__).resolve().parent.parent
    index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
    qa_js = (root / "static" / "ifc_qa_app.js").read_text(encoding="utf-8")

    assert "Maximum file size: 1.2 GB" in index_html
    assert "Maximum file size: 1.2 GB" in qa_js
