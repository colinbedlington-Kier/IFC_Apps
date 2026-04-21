import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from openpyxl import load_workbook
from xml.etree import ElementTree as ET

LOGGER = logging.getLogger("ifc_app.cobieqc.native")


def _clean_tag(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value).strip())
    return cleaned or "Column"


def _xml_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


@dataclass
class CobieQcNativeResult:
    ok: bool
    output_filename: str
    output_html: str
    cobie_xml: str
    svrl_xml: str
    summary: Dict[str, Any]
    stdout: str
    stderr: str
    error: str


class CobieWorkbookXmlBuilder:
    def __init__(self, workbook_path: Path, stage: str, template_path: Path | None = None) -> None:
        self.workbook_path = workbook_path
        self.stage = stage
        self.template_path = template_path

    def build(self) -> Tuple[bytes, List[str]]:
        warnings: List[str] = []
        wb = load_workbook(self.workbook_path, read_only=True, data_only=True)
        root = ET.Element("COBieWorkbook")
        root.set("stage", self.stage)
        root.set("source", self.workbook_path.name)

        if self.template_path and self.template_path.exists():
            root.set("template", self.template_path.name)
        else:
            warnings.append("COBieExcelTemplate.xml missing; using generic workbook mapping")

        for sheet in wb.worksheets:
            sheet_el = ET.SubElement(root, "Sheet")
            sheet_el.set("name", sheet.title)
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                warnings.append(f"Sheet '{sheet.title}' is empty")
                continue

            header = [_clean_tag(v if v is not None else "Column") for v in rows[0]]
            for row_idx, row in enumerate(rows[1:], start=2):
                if all(cell in (None, "") for cell in row):
                    continue
                row_el = ET.SubElement(sheet_el, "Row")
                row_el.set("index", str(row_idx))
                for col_idx, cell in enumerate(row):
                    col_name = header[col_idx] if col_idx < len(header) else f"Column_{col_idx + 1}"
                    cell_el = ET.SubElement(row_el, col_name)
                    if cell is not None:
                        cell_el.text = str(cell)

        return ET.tostring(root, encoding="utf-8", xml_declaration=True), warnings


class SchematronPipeline:
    def __init__(self, resources_dir: Path, stage: str) -> None:
        self.resources_dir = resources_dir
        self.stage = stage

    def validate(self, cobie_xml_path: Path, svrl_output_path: Path) -> Tuple[Dict[str, int], List[str], str]:
        warnings: List[str] = []
        try:
            from lxml import etree
            from lxml import isoschematron
        except Exception as exc:
            warnings.append(f"lxml unavailable for Schematron validation: {exc}")
            self._write_fallback_svrl(svrl_output_path, warnings, [])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": 1}, warnings, ""

        sch_path = self.resources_dir / "COBieRules.sch"
        if not sch_path.exists():
            warnings.append("COBieRules.sch missing; generated fallback SVRL")
            self._write_fallback_svrl(svrl_output_path, warnings, [])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": 1}, warnings, ""

        xml_doc = etree.parse(str(cobie_xml_path))
        sch_doc = etree.parse(str(sch_path))

        try:
            schema = isoschematron.Schematron(sch_doc, store_report=True, phase=self.stage)
            schema.validate(xml_doc)
            report_doc = schema.validation_report
            svrl_output_path.write_bytes(etree.tostring(report_doc, encoding="utf-8", pretty_print=True, xml_declaration=True))
            root = report_doc.getroot()
            failed_asserts = len(root.xpath("//*[local-name()='failed-assert']"))
            successful_reports = len(root.xpath("//*[local-name()='successful-report']"))
            diagnostics = len(root.xpath("//*[local-name()='diagnostic-reference']"))
            return (
                {
                    "failed_asserts": failed_asserts,
                    "successful_reports": successful_reports,
                    "diagnostics": diagnostics,
                },
                warnings,
                "",
            )
        except Exception as exc:
            warnings.append(f"Schematron execution fallback used: {exc}")
            self._write_fallback_svrl(svrl_output_path, warnings, [str(exc)])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": len(warnings)}, warnings, str(exc)

    def _write_fallback_svrl(self, output_path: Path, warnings: List[str], errors: List[str]) -> None:
        root = ET.Element("svrl:schematron-output", {"xmlns:svrl": "http://purl.oclc.org/dsdl/svrl"})
        for warning in warnings:
            warn_el = ET.SubElement(root, "svrl:successful-report")
            warn_el.set("role", "warning")
            text_el = ET.SubElement(warn_el, "svrl:text")
            text_el.text = warning
        for error in errors:
            err_el = ET.SubElement(root, "svrl:failed-assert")
            text_el = ET.SubElement(err_el, "svrl:text")
            text_el.text = error
        output_path.write_bytes(ET.tostring(root, encoding="utf-8", xml_declaration=True))


class SvrlHtmlRenderer:
    def __init__(self, resources_dir: Path) -> None:
        self.resources_dir = resources_dir

    def render(self, svrl_path: Path, html_output_path: Path, summary: Dict[str, Any], warnings: List[str]) -> None:
        css_path = self.resources_dir / "SpaceReport.css"
        target_css = html_output_path.parent / "SpaceReport.css"
        if css_path.exists():
            shutil.copy2(css_path, target_css)

        try:
            from lxml import etree
        except Exception:
            self._write_fallback_html(html_output_path, summary, warnings)
            return

        xslt_candidates = [
            self.resources_dir / "SVRL_HTML_altLocation.xslt",
            self.resources_dir / "_SVRL_HTML_altLocation.xslt",
        ]

        for xslt_path in xslt_candidates:
            if not xslt_path.exists():
                continue
            try:
                svrl_doc = etree.parse(str(svrl_path))
                xslt_doc = etree.parse(str(xslt_path))
                transform = etree.XSLT(xslt_doc)
                html_doc = transform(svrl_doc)
                html_output_path.write_bytes(etree.tostring(html_doc, encoding="utf-8", pretty_print=True, method="html"))
                return
            except Exception:
                continue

        self._write_fallback_html(html_output_path, summary, warnings)

    def _write_fallback_html(self, html_output_path: Path, summary: Dict[str, Any], warnings: List[str]) -> None:
        warning_items = "".join(f"<li>{_xml_escape(w)}</li>" for w in warnings)
        html = f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>COBieQC Report</title>
  <link rel=\"stylesheet\" href=\"SpaceReport.css\" />
</head>
<body>
  <h1>COBieQC Validation Report</h1>
  <ul>
    <li>Failed asserts: {summary.get('failed_asserts', 0)}</li>
    <li>Successful reports: {summary.get('successful_reports', 0)}</li>
    <li>Diagnostics: {summary.get('diagnostics', 0)}</li>
  </ul>
  <h2>Pipeline Notes</h2>
  <ul>{warning_items or '<li>None</li>'}</ul>
</body>
</html>
"""
        html_output_path.write_text(html, encoding="utf-8")


def run_cobieqc_native(input_xlsx_path: str, stage: str, job_dir: str, resources_dir: Path) -> CobieQcNativeResult:
    input_path = Path(input_xlsx_path).resolve()
    out_dir = Path(job_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cobie_xml_path = out_dir / "cobie.xml"
    svrl_xml_path = out_dir / "report.svrl.xml"
    html_path = out_dir / "report.html"
    logs: List[str] = []

    try:
        logs.append("workbook parsed")
        builder = CobieWorkbookXmlBuilder(
            workbook_path=input_path,
            stage=stage,
            template_path=resources_dir / "COBieExcelTemplate.xml",
        )
        cobie_xml_bytes, parse_warnings = builder.build()
        cobie_xml_path.write_bytes(cobie_xml_bytes)

        logs.append("XML generated")
        validator = SchematronPipeline(resources_dir=resources_dir, stage=stage)
        summary, svrl_warnings, svrl_error = validator.validate(cobie_xml_path, svrl_xml_path)
        logs.append("schematron compiled")
        logs.append("SVRL produced")

        warnings = [*parse_warnings, *svrl_warnings]
        summary["warnings"] = warnings
        summary["stage"] = stage

        renderer = SvrlHtmlRenderer(resources_dir=resources_dir)
        renderer.render(svrl_xml_path, html_path, summary, warnings)
        logs.append("HTML produced")

        return CobieQcNativeResult(
            ok=True,
            output_filename=html_path.name,
            output_html=str(html_path),
            cobie_xml=str(cobie_xml_path),
            svrl_xml=str(svrl_xml_path),
            summary=summary,
            stdout="\n".join(logs),
            stderr=svrl_error,
            error="",
        )
    except Exception as exc:
        stage_hint = logs[-1] if logs else "input handling"
        return CobieQcNativeResult(
            ok=False,
            output_filename=html_path.name,
            output_html=str(html_path),
            cobie_xml=str(cobie_xml_path),
            svrl_xml=str(svrl_xml_path),
            summary={"failed_asserts": 0, "successful_reports": 0, "diagnostics": 0, "warnings": []},
            stdout="\n".join(logs),
            stderr="",
            error=f"Pipeline failed during stage '{stage_hint}': {exc}",
        )
