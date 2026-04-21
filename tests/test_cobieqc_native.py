from pathlib import Path
import os
import subprocess

import pytest
from openpyxl import Workbook

from ifc_app.cobieqc_native.engine import _resolve_saxon_command, _run_saxon_xslt, run_cobieqc_native
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


def test_run_cobieqc_native_produces_artifacts(monkeypatch, tmp_path):
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    _make_sample_workbook(workbook)
    _write_resources(resources)

    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "lxml")
    # These placeholder resources are intentionally non-executable XSLT fixtures,
    # so use the legacy lxml fallback path for this artifact smoke test.
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
    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "lxml")
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
    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "lxml")
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


def test_saxon_executes_compiled_xslt_with_quantified_expression(monkeypatch, tmp_path):
    try:
        _resolve_saxon_command()
    except RuntimeError:
        pytest.skip("Saxon HE is not available in this environment")

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
<xsl:stylesheet version="2.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:axsl="urn:test:axsl"
  xmlns:svrl="http://purl.oclc.org/dsdl/svrl">
  <xsl:output method="xml" indent="yes"/>
  <xsl:namespace-alias stylesheet-prefix="axsl" result-prefix="xsl"/>
  <xsl:param name="phase"/>
  <xsl:template match="/">
    <axsl:stylesheet version="2.0" xmlns:svrl="http://purl.oclc.org/dsdl/svrl">
      <axsl:template match="/">
        <svrl:schematron-output>
          <axsl:if test="some $comp in /COBieWorkbook/Sheet[@name='Facility']/Row satisfies string-length($comp/Name) &gt; 0">
            <svrl:successful-report test="quantified-expression">
              <svrl:text>quantifier_ok</svrl:text>
            </svrl:successful-report>
          </axsl:if>
        </svrl:schematron-output>
      </axsl:template>
    </axsl:stylesheet>
  </xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )
    (resources / "COBieExcelTemplate.xml").write_text("<template/>", encoding="utf-8")
    (resources / "COBieRules_Functions.xsl").write_text("<xsl:stylesheet version='2.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'/>", encoding="utf-8")
    (resources / "iso_schematron_skeleton_for_saxon.xsl").write_text("<!-- skeleton marker -->", encoding="utf-8")
    (resources / "SpaceReport.css").write_text("body {}", encoding="utf-8")
    (resources / "SVRL_HTML_altLocation.xslt").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/"><html><body>ok</body></html></xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )
    (resources / "_SVRL_HTML_altLocation.xslt").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/"><html><body>ok-fallback</body></html></xsl:template>
</xsl:stylesheet>
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "saxon")
    result = run_cobieqc_native(str(workbook), "D", str(job_dir), resources)

    assert result.ok
    svrl_text = (job_dir / "validation_result.svrl.xml").read_text(encoding="utf-8")
    assert "quantifier_ok" in svrl_text
    assert "some $comp" in (job_dir / "compiled_validation.xsl").read_text(encoding="utf-8")


def test_saxon_uses_explicit_classpath_env_vars(monkeypatch, tmp_path):
    input_xml = tmp_path / "input.xml"
    stylesheet = tmp_path / "stylesheet.xsl"
    output_file = tmp_path / "result.xml"
    saxon_jar = tmp_path / "saxon-he-12.9.jar"
    xmlresolver_jar = tmp_path / "xmlresolver.jar"
    xmlresolver_data_jar = tmp_path / "xmlresolver-data.jar"
    input_xml.write_text("<root/>", encoding="utf-8")
    stylesheet.write_text("<xsl:stylesheet version='3.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'/>", encoding="utf-8")
    saxon_jar.write_bytes(b"saxon")
    xmlresolver_jar.write_bytes(b"resolver")
    xmlresolver_data_jar.write_bytes(b"resolver-data")
    output_file.write_text("<ok/>", encoding="utf-8")

    monkeypatch.setenv("COBIEQC_SAXON_JAR_PATH", str(saxon_jar))
    monkeypatch.setenv("COBIEQC_SAXON_XMLRESOLVER_JAR_PATH", str(xmlresolver_jar))
    monkeypatch.setenv("COBIEQC_SAXON_XMLRESOLVER_DATA_JAR_PATH", str(xmlresolver_data_jar))
    monkeypatch.delenv("COBIEQC_SAXON_CMD", raising=False)

    captured: dict[str, object] = {}

    def _fake_run(command, capture_output, text):
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="saxon ok", stderr="")

    monkeypatch.setattr(subprocess, "run", _fake_run)
    logs: list[str] = []
    _run_saxon_xslt(input_xml, stylesheet, output_file, params={}, logs=logs)

    command = captured["command"]
    assert isinstance(command, list)
    assert command[0] == "java"
    assert command[1] == "-cp"
    assert command[3] == "net.sf.saxon.Transform"
    expected_classpath = f"{saxon_jar}{os.pathsep}{xmlresolver_jar}{os.pathsep}{xmlresolver_data_jar}"
    assert command[2] == expected_classpath
    assert any(line.startswith("computed_classpath=") for line in logs)
    assert any(line.startswith("saxon_argv=") for line in logs)


def test_saxon_missing_jar_path_fails_with_named_env_var(monkeypatch, tmp_path):
    monkeypatch.setenv("COBIEQC_SAXON_JAR_PATH", str(tmp_path / "missing-saxon.jar"))
    monkeypatch.setenv("COBIEQC_SAXON_XMLRESOLVER_JAR_PATH", str(tmp_path / "xmlresolver.jar"))
    monkeypatch.setenv("COBIEQC_SAXON_XMLRESOLVER_DATA_JAR_PATH", str(tmp_path / "xmlresolver-data.jar"))
    monkeypatch.delenv("COBIEQC_SAXON_CMD", raising=False)

    with pytest.raises(RuntimeError, match="COBIEQC_SAXON_JAR_PATH"):
        _resolve_saxon_command()


def test_native_pipeline_logs_cobie_and_svrl_diagnostics(monkeypatch, tmp_path):
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    _make_sample_workbook(workbook)
    resources.mkdir(parents=True, exist_ok=True)
    _write_resources(resources)

    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "lxml")
    result = run_cobieqc_native(str(workbook), "D", str(job_dir), resources)

    assert result.ok
    assert "generated_cobie_xml_diagnostics root_element=COBieWorkbook" in result.stdout
    assert "generated_cobie_xml_entity_counts" in result.stdout
    assert "svrl_diagnostics fired_rules=0" in result.stdout
    assert any("rule contexts likely did not match" in warning for warning in result.summary["warnings"])


def test_native_pipeline_compares_against_reference_xml(monkeypatch, tmp_path):
    workbook = tmp_path / "input.xlsx"
    resources = tmp_path / "xsl_xml"
    job_dir = tmp_path / "job"
    reference_xml = tmp_path / "legacy.xml"
    _make_sample_workbook(workbook)
    _write_resources(resources)
    reference_xml.write_text(
        """<?xml version='1.0'?>
<COBie xmlns="urn:example:cobie">
  <Facilities>
    <Facility/>
  </Facilities>
</COBie>
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("COBIEQC_XSLT_ENGINE", "lxml")
    monkeypatch.setenv("COBIEQC_REFERENCE_XML_PATH", str(reference_xml))
    result = run_cobieqc_native(str(workbook), "D", str(job_dir), resources)

    assert result.ok
    assert "cobie_xml_comparison_summary" in result.stdout
    assert f"reference_path={reference_xml.resolve()}" in result.stdout
    assert "cobie_xml_comparison_paths" in result.stdout
