from pathlib import Path

import app


def test_session_file_listing_returns_raw_ifc_and_xlsx_files():
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / "model.ifc").write_bytes(b"ISO-10303-21;")
    (root / "extract.xlsx").write_bytes(b"PK\x03\x04")

    payload = app.list_files(session_id)
    names = [item["name"] for item in payload["files"]]

    assert "model.ifc" in names
    assert "extract.xlsx" in names


def test_ifc_data_qa_extract_endpoint_accepts_session_id_and_file_ids(monkeypatch):
    captured = {}

    def _fake_starter(session_root, session_id, file_records, options, config, mode):
        captured["session_id"] = session_id
        captured["file_records"] = file_records
        captured["mode"] = mode
        return "job-data-qa-1"

    monkeypatch.setattr(app, "IFC_QA_JOB_STARTER", _fake_starter)
    monkeypatch.setattr(app, "has_active_ifc_qa_job", lambda: False)

    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / "model.ifcxml").write_bytes(b"<ifcXML/>")

    payload = app.ifc_data_qa_extract({"session_id": session_id, "file_ids": ["model.ifcxml"]})

    assert payload["success"] is True
    assert payload["job_id"] == "job-data-qa-1"
    assert payload["session_id"] == session_id
    assert captured["session_id"] == session_id
    assert captured["mode"] == "replace"
    assert captured["file_records"][0][0] == "model.ifcxml"


def test_ifc_data_qa_frontend_uses_shared_session_module_and_ifc_filtering():
    root = Path(__file__).resolve().parent.parent
    qa_js = (root / "static" / "ifc_qa_app.js").read_text(encoding="utf-8")
    upload_js = (root / "static" / "app.js").read_text(encoding="utf-8")
    shared_js = (root / "static" / "session_shared.js").read_text(encoding="utf-8")

    assert "getCurrentSessionId" in qa_js
    assert "ensureSession({ createIfMissing: false })" in qa_js
    assert "ensureSession({ createIfMissing: true })" in qa_js
    assert "ifc-toolkit-session-changed" in qa_js
    assert "canonicalSessionId" in qa_js
    assert "localStorage.getItem(\"ifc_session_id\")" not in qa_js
    assert ".ifcxml" in qa_js
    assert "ensureSession({ createIfMissing: true })" in upload_js
    assert "setCurrentSessionId(state.sessionId)" in upload_js
    assert "ifc_toolkit_session_id" in shared_js
    assert "ifcToolkitSessionId" in shared_js
    assert "getActiveSessionId" in shared_js
    assert "ifc-toolkit-session-changed" in shared_js
    assert "legacyStorageKeys" in shared_js


def test_ifc_data_qa_frontend_bootstraps_session_file_loader_and_refresh_uses_same_loader():
    root = Path(__file__).resolve().parent.parent
    qa_js = (root / "static" / "ifc_qa_app.js").read_text(encoding="utf-8")

    assert "qaState.sessionLoaderBootstrapped = true;" in qa_js
    assert "function markSessionLoaderExecuted(reason = \"boot\")" in qa_js
    assert "function maybeAutoFetchSessionFiles(sessionIdHint = \"\", reason = \"auto_ready\")" in qa_js
    assert "const autoFetchedSessionIds = new Set();" in qa_js
    assert "qaState.sessionLoaderExecuted = true;" in qa_js
    assert "autoFetchedSessionIds.has(sid)" in qa_js
    assert "void loadSessionFilesNow(sid, reason);" in qa_js
    assert "bootstrapSessionFileLoader(normalized, \"session_subscribe\")" in qa_js
    assert "maybeAutoFetchSessionFiles(normalized, \"session_subscribe\")" in qa_js
    assert "bootstrapSessionFileLoader(normalized, \"toolkit_event\")" in qa_js
    assert "maybeAutoFetchSessionFiles(normalized, \"toolkit_event\")" in qa_js
    assert "bootstrapSessionFileLoader(qaState.canonicalSessionId || qaState.sessionId, \"ensureSession_resolved\")" in qa_js
    assert "maybeAutoFetchSessionFiles(qaState.canonicalSessionId || qaState.sessionId, \"session-ready\")" in qa_js
    assert "bootstrapSessionFileLoader(qaState.canonicalSessionId || qaState.sessionId, \"mount_immediate\")" in qa_js
    assert "markSessionLoaderExecuted(\"extractor_boot_effect\")" in qa_js
    assert "qaRefreshSessionFilesBtn" in qa_js
    assert "loadSessionFilesNow(qaState.canonicalSessionId || qaState.sessionId, \"manual_refresh\")" in qa_js
    assert "window.IFCSession.getSessionFiles(sid, {" in qa_js
    assert "sharedSessionLoaderUsed = true;" in qa_js
    assert "sessionLoaderSource = \"IFCSession.getSessionFiles\";" in qa_js
    assert "Shared session loader unavailable: IFCSession.getSessionFiles" in qa_js
    assert "extractor-fallback-fetch" not in qa_js
    assert "sharedSessionLoaderUsed: ${qaState.sharedSessionLoaderUsed}" in qa_js
    assert "sessionLoaderSource: ${qaState.sessionLoaderSource || \"-\"}" in qa_js


def test_ifc_data_qa_and_upload_page_use_shared_session_files_loader_contract():
    root = Path(__file__).resolve().parent.parent
    qa_js = (root / "static" / "ifc_qa_app.js").read_text(encoding="utf-8")
    upload_js = (root / "static" / "app.js").read_text(encoding="utf-8")
    shared_js = (root / "static" / "session_shared.js").read_text(encoding="utf-8")

    assert "const url = `/api/session/${sid}/files`;" in shared_js
    assert "state.files = await window.IFCSession.getSessionFiles(state.sessionId);" in upload_js
    assert "window.IFCSession.getSessionFiles(sid, {" in qa_js
    assert "sharedSessionLoaderUsed = true;" in qa_js
    assert "sessionLoaderSource = \"IFCSession.getSessionFiles\";" in qa_js
    assert "Shared session loader unavailable: IFCSession.getSessionFiles" in qa_js
    assert "extractor-fallback-fetch" not in qa_js
    assert "normalized.endsWith(\".ifc\") || normalized.endsWith(\".ifczip\") || normalized.endsWith(\".ifcxml\")" in qa_js
