import uuid
from pathlib import Path

import pytest
from fastapi import HTTPException

import app


def _write_session_file(session_id: str, name: str, data: bytes = b"ISO-10303-21;\n") -> None:
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / name).write_bytes(data)


def test_data_extractor_payload_uses_single_ifc_file_from_session():
    session_id = app.SESSION_STORE.create()
    _write_session_file(session_id, "model.ifc")

    payload = app._build_ifc_job_payload(session_id, {"tables": ["Model Data Table"]})

    assert payload["session_id"] == session_id
    assert payload["ifc_files"] == ["model.ifc"]


def test_data_extractor_payload_uses_multiple_ifc_files_from_session():
    session_id = app.SESSION_STORE.create()
    _write_session_file(session_id, "a.ifc")
    _write_session_file(session_id, "b.ifczip", b"zip-bytes")
    _write_session_file(session_id, "c.ifcxml", b"<ifcXML />")

    payload = app._build_ifc_job_payload(session_id, {"tables": ["Model Data Table"]})

    assert sorted(payload["ifc_files"]) == ["a.ifc", "b.ifczip", "c.ifcxml"]


def test_data_extractor_payload_rejects_session_without_ifc_files():
    session_id = app.SESSION_STORE.create()
    _write_session_file(session_id, "notes.txt", b"hello")

    with pytest.raises(HTTPException) as exc_info:
        app._build_ifc_job_payload(session_id, {"tables": ["Model Data Table"]})

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error"] == "NO_IFC_FILES"


def test_data_extractor_payload_rejects_invalid_session_id():
    invalid_session_id = uuid.uuid4().hex

    with pytest.raises(HTTPException) as exc_info:
        app._build_ifc_job_payload(invalid_session_id, {"tables": ["Model Data Table"]})

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error"] == "SESSION_NOT_FOUND"


def test_data_extractor_payload_rejects_selected_file_not_in_session():
    session_id = app.SESSION_STORE.create()
    _write_session_file(session_id, "present.ifc")

    with pytest.raises(HTTPException) as exc_info:
        app._build_ifc_job_payload(
            session_id,
            {"tables": ["Model Data Table"], "ifc_files": ["missing.ifc"]},
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error"] == "SELECTED_FILE_NOT_IN_SESSION"
