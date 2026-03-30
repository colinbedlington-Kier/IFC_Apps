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
