import io
import zipfile
from pathlib import Path

from cobieqc_service import bootstrap


def _zip_bytes_with_wrapper() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("wrapper/xsl_xml/template.xml", "<xml/>")
        zf.writestr("wrapper/xsl_xml/rules/style.xsl", "<xsl:stylesheet version='1.0'/>")
    return buf.getvalue()


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


def test_bootstrap_installs_jar_and_xml_when_missing(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(root / "xsl_xml"))

    jar_bytes = b"jar-binary"
    zip_bytes = _zip_bytes_with_wrapper()

    def _mock_download(url, suffix, _purpose):
        target = tmp_path / f"download{suffix}"
        if "19wRbk" in url:
            target.write_bytes(jar_bytes)
            return target
        if "1EKJWT" in url:
            target.write_bytes(zip_bytes)
            return target
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(bootstrap, "_download_to_temp", _mock_download)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is True
    assert status["resource_source"] == "zip_extract"
    assert (root / "CobieQcReporter.jar").exists()
    assert (root / "xsl_xml").exists()
    assert any(p.is_file() for p in (root / "xsl_xml").rglob("*"))


def test_bootstrap_degrades_gracefully_when_download_fails(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))

    monkeypatch.setattr(bootstrap, "_download_to_temp", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("download failed")))

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert status["enabled"] is False
    assert "failed" in status["last_error"]
    assert status["resource_source"] == "missing"


def test_bootstrap_uses_preferred_data_resources_without_zip(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    preferred = root / "xsl_xml"
    preferred.mkdir(parents=True)
    (root / "CobieQcReporter.jar").write_bytes(b"jar")
    (preferred / "template.xml").write_text("<xml/>", encoding="utf-8")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(tmp_path / "unused" / "xsl_xml"))

    called = {"value": False}

    def _fail_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("zip install should not run")

    monkeypatch.setattr(bootstrap, "_install_xml_from_zip", _fail_get)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert called["value"] is False
    assert status["enabled"] is True
    assert status["resource_source"] == "existing_dir"
    assert Path(status["resource_dir"]) == preferred.resolve()


def test_bootstrap_falls_back_to_vendor_resource_dir(monkeypatch, tmp_path):
    root = tmp_path / "cobie"
    root.mkdir(parents=True)
    fallback = tmp_path / "vendor" / "cobieqc" / "xsl_xml"
    fallback.mkdir(parents=True)
    (fallback / "rules.xml").write_text("x", encoding="utf-8")
    (root / "CobieQcReporter.jar").write_bytes(b"jar")

    monkeypatch.setenv("COBIEQC_DATA_DIR", str(root))
    monkeypatch.setenv("COBIEQC_JAR_PATH", str(root / "CobieQcReporter.jar"))
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(root / "xsl_xml"))
    monkeypatch.chdir(tmp_path)

    called = {"value": False}

    def _fail_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("zip install should not run")

    monkeypatch.setattr(bootstrap, "_install_xml_from_zip", _fail_get)

    bootstrap.bootstrap_cobieqc_assets()
    status = bootstrap.get_cobieqc_bootstrap_status()

    assert called["value"] is False
    assert status["enabled"] is True
    assert status["resource_source"] == "fallback_dir"
    assert Path(status["resource_dir"]) == fallback.resolve()
