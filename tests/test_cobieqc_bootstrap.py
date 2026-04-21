from pathlib import Path

from cobieqc_service import bootstrap

REQUIRED_FILES = [
    "SpaceReport.css",
    "iso_svrl_for_xslt2.xsl",
    "COBieExcelTemplate.xml",
    "COBieRules.sch",
    "iso_schematron_skeleton_for_saxon.xsl",
    "SVRL_HTML_altLocation.xslt",
    "COBieRules_Functions.xsl",
    "_SVRL_HTML_altLocation.xslt",
]


def _write_required_resources(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_FILES:
        (directory / name).write_text("x", encoding="utf-8")


def test_parse_google_drive_file_id():
    assert (
        bootstrap.parse_google_drive_file_id(
            "https://drive.google.com/file/d/19wRbk-TNoHNOmRgqqDP4AjbRqawzE7wq/view?usp=drive_link"
        )
        == "19wRbk-TNoHNOmRgqqDP4AjbRqawzE7wq"
    )


def test_google_drive_share_to_direct_link():
    share_url = "https://drive.google.com/file/d/1EKJWT7fHgTDJdt95nh0z2kxXhI3LBNJd/view?usp=drive_link"
    assert (
        bootstrap.google_drive_direct_download_url(share_url)
        == "https://drive.google.com/uc?export=download&id=1EKJWT7fHgTDJdt95nh0z2kxXhI3LBNJd"
    )


def test_bootstrap_skips_when_assets_already_exist(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    jar = root / "CobieQcReporter.jar"
    resources = root / "xsl_xml"
    _write_required_resources(resources)
    jar.write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(jar))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(resources))

    called = {"value": False}

    def _fail_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("should not download")

    monkeypatch.setattr(bootstrap, "_download_to_temp", lambda *_args, **_kwargs: (_fail_get()))

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert called["value"] is False
    assert status["enabled"] is True
    assert status["resource_source"] == "existing_dir"


def test_bootstrap_installs_jar_and_resources_from_json_mapping(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    source_dir = root / "xsl_xml"

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(source_dir))
    mapping = {name: f"https://example.test/{name}" for name in REQUIRED_FILES}
    monkeypatch.setenv("COBIEQC_XML_FILE_URLS_JSON", __import__("json").dumps(mapping))

    jar_bytes = b"jar-binary"

    def _mock_download(url, suffix, _purpose):
        if "19wRbk" in url:
            target = tmp_path / f"download{suffix}"
            target.write_bytes(jar_bytes)
            return target, "application/octet-stream"
        filename = url.split("/")[-1]
        target = tmp_path / f"resource-{filename}"
        target.write_text(f"resource:{filename}", encoding="utf-8")
        return target, "application/octet-stream"

    monkeypatch.setattr(bootstrap, "_download_to_temp", _mock_download)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is True
    assert status["resource_source"] == "file_urls_json"
    assert (root / "CobieQcReporter.jar").exists()
    for filename in REQUIRED_FILES:
        assert (root / "xsl_xml" / filename).exists()


def test_bootstrap_degrades_gracefully_when_download_fails(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))

    monkeypatch.setattr(bootstrap, "_download_to_temp", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("download failed")))

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is False
    assert "failed" in status["last_error"]
    assert status["resource_source"] == "missing"


def test_bootstrap_uses_preferred_data_resources_without_sync(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    preferred = root / "xsl_xml"
    _write_required_resources(preferred)
    (root / "CobieQcReporter.jar").write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(tmp_path / "unused" / "xsl_xml"))

    called = {"value": False}

    def _fail_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("resource sync should not run")

    monkeypatch.setattr(bootstrap, "_sync_resource_folder_from_source", _fail_get)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert called["value"] is False
    assert status["enabled"] is True
    assert status["resource_source"] == "preferred_data_dir"
    assert Path(status["resource_dir"]) == preferred.resolve()


def test_bootstrap_falls_back_to_vendor_resource_dir(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    root.mkdir(parents=True)
    fallback = tmp_path / "vendor" / "cobieqc" / "xsl_xml"
    _write_required_resources(fallback)
    (root / "CobieQcReporter.jar").write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(root / "xsl_xml"))
    monkeypatch.chdir(tmp_path)

    called = {"value": False}

    def _fail_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("resource sync should not run")

    monkeypatch.setattr(bootstrap, "_sync_resource_folder_from_source", _fail_get)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert called["value"] is False
    assert status["enabled"] is True
    assert status["resource_source"] == "packaged_fallback_copy"
    assert Path(status["resource_dir"]) == (root / "xsl_xml").resolve()


def test_google_drive_folder_source_is_not_treated_as_download(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    (root / "CobieQcReporter.jar").parent.mkdir(parents=True)
    (root / "CobieQcReporter.jar").write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(root / "xsl_xml"))
    monkeypatch.setenv("COBIEQC_XML_SOURCE_URL", "https://drive.google.com/drive/folders/13ZYp5lb1B57nmPpLMZnCS3zP7I--zFjg")

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is False
    assert "unsupported for Google Drive folder URL source mode" in status["last_error"]
    assert status["source_mode"] == "unsupported_google_drive_folder"


def test_bootstrap_downloads_resource_files_from_json_map(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    resource_dir = root / "xsl_xml"
    jar_path = root / "CobieQcReporter.jar"
    jar_path.parent.mkdir(parents=True, exist_ok=True)
    jar_path.write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(jar_path))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(resource_dir))
    mapping = {name: f"https://example.test/{name}" for name in REQUIRED_FILES}
    monkeypatch.setenv("COBIEQC_XML_FILE_URLS_JSON", __import__("json").dumps(mapping))

    def _mock_download(url, suffix, _purpose):
        filename = url.split("/")[-1]
        target = tmp_path / f"download-{filename}"
        target.write_text(f"downloaded:{filename}", encoding="utf-8")
        return target, "application/octet-stream"

    monkeypatch.setattr(bootstrap, "_download_to_temp", _mock_download)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is True
    assert status["resources_ready"] is True
    assert status["source_mode"] == "file_urls_json"
    assert status["missing_files"] == []
    for filename in REQUIRED_FILES:
        assert (resource_dir / filename).exists()
