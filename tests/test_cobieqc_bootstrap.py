from pathlib import Path

from cobieqc_service import bootstrap


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
    resources.mkdir(parents=True)
    jar.write_bytes(b"jar")
    (resources / "rules.xml").write_text("x", encoding="utf-8")
    (resources / "style.xsl").write_text("x", encoding="utf-8")

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


def test_bootstrap_installs_jar_and_uses_local_folder_source(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    source_dir = tmp_path / "source_xsl_xml"
    source_dir.mkdir(parents=True)
    (source_dir / "template.xml").write_text("<xml/>", encoding="utf-8")
    (source_dir / "style.xsl").write_text("<xsl:stylesheet version='1.0'/>", encoding="utf-8")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(root / "xsl_xml"))
    monkeypatch.setenv("COBIEQC_XML_SOURCE_URL", str(source_dir))

    jar_bytes = b"jar-binary"

    def _mock_download(url, suffix, _purpose):
        target = tmp_path / f"download{suffix}"
        if "19wRbk" in url:
            target.write_bytes(jar_bytes)
            return target
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(bootstrap, "_download_to_temp", _mock_download)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is True
    assert status["resource_source"] == "folder_sync"
    assert (root / "CobieQcReporter.jar").exists()
    assert (root / "xsl_xml" / "template.xml").exists()


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
    preferred.mkdir(parents=True)
    (root / "CobieQcReporter.jar").write_bytes(b"jar")
    (preferred / "template.xml").write_text("<xml/>", encoding="utf-8")
    (preferred / "style.xsl").write_text("<xsl:stylesheet version='1.0'/>", encoding="utf-8")

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
    fallback.mkdir(parents=True)
    (fallback / "rules.xml").write_text("x", encoding="utf-8")
    (fallback / "style.xsl").write_text("x", encoding="utf-8")
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
    assert "automatic folder sync is unavailable" in status["last_error"]
