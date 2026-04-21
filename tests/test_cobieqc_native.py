from pathlib import Path

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
