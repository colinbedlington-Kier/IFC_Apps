from pathlib import Path


def _read(path: str) -> str:
    root = Path(__file__).resolve().parent.parent
    return (root / path).read_text(encoding="utf-8")


def test_upload_progress_helpers_exist_in_session_uploader():
    app_js = _read("static/app.js")
    assert "function formatBytes(bytes)" in app_js
    assert "function formatTransferSpeed(bytesPerSecond)" in app_js
    assert "function calculateUploadPercent(loaded, total)" in app_js
    assert "function createRollingSpeedTracker(" in app_js
    assert "buildPerFileProgress(files, loaded)" in app_js


def test_upload_processing_and_complete_state_text_present():
    app_js = _read("static/app.js")
    assert "Preparing upload…" in app_js
    assert "Uploading…" in app_js
    assert "Saving file to session storage..." in app_js
    assert "Complete — added to session" in app_js
    assert "Failed —" in app_js


def test_upload_tracks_overall_progress_across_selected_files():
    app_js = _read("static/app.js")
    assert "const totalBytes = files.reduce" in app_js
    assert "const perFile = buildPerFileProgress(files, loaded).map" in app_js
    assert "const bytesLabel = `${formatBytes(loaded)} / ${formatBytes(progressTotal)}`;" in app_js


def test_upload_page_contains_determinate_progress_and_per_file_rows():
    index_html = _read("static/index.html")
    assert "data-upload-progress-wrap" in index_html
    assert "data-upload-percent" in index_html
    assert "data-upload-bytes" in index_html
    assert "data-upload-speed" in index_html
    assert "upload-file-progress-list" in index_html
