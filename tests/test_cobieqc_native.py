from pathlib import Path

import pytest
from openpyxl import Workbook

from ifc_app.cobieqc_native.engine import run_cobieqc_native
from cobieqc_service import runner

REQUIRED_RESOURCE_FILES = [
    "SpaceReport.css",
    "iso_svrl_for_xslt2.xsl",
    "COBieExcelTemplate.xml",
    "COBieRules.sch",
    "iso_schematron_skeleton_for_saxon.xsl",
    "SVRL_HTML_altLocation.xslt",
    "COBieRules_Functions.xsl",
    "_SVRL_HTML_altLocation.xslt",
]


def _make_sample_workbook(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Facility"
    ws.append(["Name", "Category"])
    ws.append(["HQ", "Office"])
    ws2 = wb.create_sheet("Space")
    ws2.append(["Name", "Floor"])
    ws2.append(["Room 101", "1"])
    wb.save(path)


def _write_resources(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_RESOURCE_FILES:
        (path / name).write_text("x", encoding="utf-8")


def test_run_cobieqc_native_produces_artifacts(tmp_path):
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    _make_sample_workbook(workbook)
    _write_resources(resources)

    result = run_cobieqc_native(str(workbook), "D", str(job_dir), resources)

    assert result.ok
    assert (job_dir / "generated_cobie.xml").exists()
    assert (job_dir / "validation_result.svrl.xml").exists()
    assert (job_dir / "final_report.html").exists()
    assert "failed_asserts" in result.summary


def test_runner_uses_python_engine_without_jar(monkeypatch, tmp_path):
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    _make_sample_workbook(workbook)
    _write_resources(resources)

    monkeypatch.setenv("COBIEQC_ENGINE", "python")
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(resources))
    monkeypatch.delenv("COBIEQC_JAR_PATH", raising=False)

    result = runner.run_cobieqc(str(workbook), "C", str(job_dir))

    assert result["ok"] is True
    assert Path(result["cobie_xml"]).exists()
    assert Path(result["svrl_xml"]).exists()
    assert Path(result["output_html"]).exists()


def test_runner_runtime_diagnostics_python_engine(monkeypatch, tmp_path):
    resources = tmp_path / "xsl_xml"
    _write_resources(resources)

    monkeypatch.setenv("COBIEQC_ENGINE", "python")
    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(resources))

    diag = runner.get_cobieqc_runtime_diagnostics()

    assert diag["engine"] == "python"
    assert diag["resource_dir_exists"] is True
    assert diag["enabled"] is True


def test_schematron_compiled_xslt_rewrites_relative_imports(monkeypatch, tmp_path):
    pytest.importorskip("lxml")
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    _make_sample_workbook(workbook)
    resources.mkdir(parents=True, exist_ok=True)

    (resources / "COBieRules.sch").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<sch:schema xmlns:sch="http://purl.oclc.org/dsdl/schematron">
  <sch:phase id="D"/>
</sch:schema>
""",
        encoding="utf-8",
    )
    (resources / "iso_svrl_for_xslt2.xsl").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:axsl="urn:test:axsl"
  xmlns:svrl="http://purl.oclc.org/dsdl/svrl">
  <xsl:output method="xml" indent="yes"/>
  <xsl:namespace-alias stylesheet-prefix="axsl" result-prefix="xsl"/>
  <xsl:param name="phase"/>
  <xsl:template match="/">
    <axsl:stylesheet version="1.0">
      <axsl:import href="COBieRules_Functions.xsl"/>
      <axsl:template match="/">
        <svrl:schematron-output>
          <svrl:active-pattern/>
          <axsl:call-template name="emit"/>
        </svrl:schematron-output>
      </axsl:template>
    </axsl:stylesheet>
  </xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )
    (resources / "COBieRules_Functions.xsl").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:svrl="http://purl.oclc.org/dsdl/svrl">
  <xsl:template name="emit">
    <svrl:successful-report test="true()">
      <svrl:text>import resolved</svrl:text>
    </svrl:successful-report>
  </xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )
    (resources / "COBieExcelTemplate.xml").write_text("<template/>", encoding="utf-8")
    (resources / "SpaceReport.css").write_text("body {}", encoding="utf-8")
    (resources / "SVRL_HTML_altLocation.xslt").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/"><html><body>ok</body></html></xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("COBIEQC_RESOURCE_DIR", str(resources))
    result = run_cobieqc_native(str(workbook), "D", str(job_dir), resources)

    compiled_xsl = job_dir / "compiled_validation.xsl"
    svrl_xml = job_dir / "validation_result.svrl.xml"
    assert result.ok
    assert compiled_xsl.exists()
    assert svrl_xml.exists()
    assert f'href="{(resources / "COBieRules_Functions.xsl").resolve().as_uri()}"' in compiled_xsl.read_text(
        encoding="utf-8"
    )
    assert "import resolved" in svrl_xml.read_text(encoding="utf-8")
