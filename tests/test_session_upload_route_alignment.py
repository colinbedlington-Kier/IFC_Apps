import uuid
import asyncio
import io

from fastapi import UploadFile
from fastapi import HTTPException

import app


def _upload(name: str, content: bytes) -> UploadFile:
    return UploadFile(filename=name, file=io.BytesIO(content))


def test_route_list_includes_shared_session_upload_paths():
    route_paths = {getattr(route, "path", "") for route in app.app.routes}
    assert "/api/session/{session_id}/files" in route_paths
    assert "/api/session/{session_id}/upload" in route_paths


def test_get_files_for_new_session_creates_directory_and_returns_empty_list():
    session_id = uuid.uuid4().hex
    payload = app.list_files(session_id)
    assert payload["files"] == []


def test_upload_then_list_files_roundtrip():
    session_id = uuid.uuid4().hex
    uploaded = asyncio.run(app.upload_files(session_id, [_upload("sample.ifc", b"ISO-10303-21;\n")]))
    assert uploaded["files"][0]["id"] == "sample.ifc"

    files = app.list_files(session_id)["files"]
    assert any(item["id"] == "sample.ifc" for item in files)


def test_invalid_session_id_format_returns_400_for_listing():
    try:
        app.list_files("bad-session-id")
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "Invalid session id format" in str(exc.detail)
    else:
        raise AssertionError("Expected HTTPException for invalid session id")
