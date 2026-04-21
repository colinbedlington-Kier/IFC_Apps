import logging
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import load_workbook
from xml.etree import ElementTree as ET

LOGGER = logging.getLogger("ifc_app.cobieqc.native")
APP_ROOT = Path(__file__).resolve().parents[2]


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

    def validate(
        self,
        cobie_xml_path: Path,
        svrl_output_path: Path,
        compiled_xslt_output_path: Path,
    ) -> Tuple[Dict[str, int], List[str], str, List[str]]:
        engine = _get_xslt_engine()
        logs: List[str] = [f"xslt_engine={engine}"]
        if engine == "saxon":
            return self._validate_with_saxon(
                cobie_xml_path=cobie_xml_path,
                svrl_output_path=svrl_output_path,
                compiled_xslt_output_path=compiled_xslt_output_path,
                logs=logs,
            )
        if engine == "lxml":
            return self._validate_with_lxml(
                cobie_xml_path=cobie_xml_path,
                svrl_output_path=svrl_output_path,
                compiled_xslt_output_path=compiled_xslt_output_path,
                logs=logs,
            )
        raise RuntimeError(
            f"Unsupported COBIEQC_XSLT_ENGINE='{engine}'. Expected one of: saxon, lxml."
        )

    def _validate_with_lxml(
        self,
        cobie_xml_path: Path,
        svrl_output_path: Path,
        compiled_xslt_output_path: Path,
        logs: List[str],
    ) -> Tuple[Dict[str, int], List[str], str, List[str]]:
        warnings: List[str] = []
        try:
            from lxml import etree
        except Exception as exc:
            warnings.append(f"lxml unavailable for Schematron validation: {exc}")
            self._write_fallback_svrl(svrl_output_path, warnings, [])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": 1}, warnings, "", logs

        sch_path = self.resources_dir / "COBieRules.sch"
        compile_xslt_path = self.resources_dir / "iso_svrl_for_xslt2.xsl"
        skeleton_path = self.resources_dir / "iso_schematron_skeleton_for_saxon.xsl"
        functions_path = self.resources_dir / "COBieRules_Functions.xsl"
        if not sch_path.exists():
            warnings.append("COBieRules.sch missing; generated fallback SVRL")
            self._write_fallback_svrl(svrl_output_path, warnings, [])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": 1}, warnings, "", logs
        if not compile_xslt_path.exists():
            warnings.append("iso_svrl_for_xslt2.xsl missing; generated fallback SVRL")
            self._write_fallback_svrl(svrl_output_path, warnings, [])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": 1}, warnings, "", logs

        try:
            sch_doc = etree.parse(str(sch_path))
        except Exception as exc:
            warnings.append(f"Schematron execution fallback used: compilation failed during schematron compile: {exc}")
            warnings.extend(self._safe_artifact_preview(sch_path))
            self._write_fallback_svrl(svrl_output_path, warnings, [str(exc)])
            return {"failed_asserts": 0, "successful_reports": 0, "diagnostics": len(warnings)}, warnings, str(exc), logs
        logs.append(self._phase_log("schematron_source_loaded", sch_path))
        if skeleton_path.exists():
            logs.append(self._phase_log("schematron_skeleton_loaded", skeleton_path))
        if functions_path.exists():
            logs.append(self._phase_log("schematron_functions_loaded", functions_path))

        phase_id = self._resolve_phase_id(sch_doc)
        if phase_id:
            logs.append(f"schematron_phase_selected stage={self.stage} phase={phase_id}")
        else:
            warnings.append(
                f"No explicit phase mapping found for stage '{self.stage}'. Running with default Schematron phase."
            )

        compile_step = "schematron compile"
        try:
            compiler_doc = etree.parse(str(compile_xslt_path))
            compiler = etree.XSLT(compiler_doc)
            compile_args = {"phase": etree.XSLT.strparam(phase_id)} if phase_id else {}
            compiled_validation_doc = compiler(sch_doc, **compile_args)
            compiled_xslt_bytes = etree.tostring(
                compiled_validation_doc, encoding="utf-8", pretty_print=True, xml_declaration=True
            )
            compiled_xslt_output_path.write_bytes(compiled_xslt_bytes)
            logs.append(self._phase_log("schematron_compiled_to_xslt", compiled_xslt_output_path))
            logs.append(f"compiled_validation_xslt path={compiled_xslt_output_path}")
            detected_hrefs = self._collect_xslt_dependency_hrefs(compiled_xslt_output_path)
            logs.append(f"compiled_xslt_dependencies_detected hrefs={','.join(detected_hrefs) or '(none)'}")
            rewritten = self._rewrite_xslt_dependency_hrefs(compiled_xslt_output_path)
            for source_href, target_href in rewritten:
                logs.append(f"compiled_xslt_dependency_rewritten from={source_href} to={target_href}")
            unresolved = self._find_unresolved_relative_hrefs(compiled_xslt_output_path)
            if unresolved:
                warnings.append(
                    f"compiled_xslt_unresolved_relative_dependencies={','.join(unresolved)} "
                    f"base_dir={compiled_xslt_output_path.parent}"
                )
                logs.append(f"compiled_xslt_unresolved_relative_dependencies hrefs={','.join(unresolved)}")
            logs.append(f"validation_resolver_base path={compiled_xslt_output_path.parent}")
        except Exception as exc:
            diagnostics = self._collect_schematron_diagnostics(sch_doc)
            warnings.append(
                f"Schematron execution fallback used: compilation failed during {compile_step}: {exc}"
            )
            warnings.extend(diagnostics)
            self._write_fallback_svrl(svrl_output_path, warnings, [str(exc)])
            return (
                {"failed_asserts": 0, "successful_reports": 0, "diagnostics": len(warnings)},
                warnings,
                str(exc),
                logs,
            )

        validation_step = "XML validation"
        try:
            xml_doc = etree.parse(str(cobie_xml_path))
            validation_doc = etree.parse(str(compiled_xslt_output_path))
            validation_transform = etree.XSLT(validation_doc)
            svrl_doc = validation_transform(xml_doc)
            svrl_bytes = etree.tostring(svrl_doc, encoding="utf-8", pretty_print=True, xml_declaration=True)
            svrl_output_path.write_bytes(svrl_bytes)
            logs.append(self._phase_log("validation_xslt_applied", compiled_xslt_output_path))
            logs.append(self._phase_log("svrl_generated", svrl_output_path))
        except Exception as exc:
            warnings.append(f"Schematron execution fallback used: validation failed during {validation_step}: {exc}")
            warnings.extend(self._safe_artifact_preview(compiled_xslt_output_path))
            self._write_fallback_svrl(svrl_output_path, warnings, [str(exc)])
            return (
                {"failed_asserts": 0, "successful_reports": 0, "diagnostics": len(warnings)},
                warnings,
                str(exc),
                logs,
            )

        root = etree.parse(str(svrl_output_path)).getroot()
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
            logs,
        )

    def _validate_with_saxon(
        self,
        cobie_xml_path: Path,
        svrl_output_path: Path,
        compiled_xslt_output_path: Path,
        logs: List[str],
    ) -> Tuple[Dict[str, int], List[str], str, List[str]]:
        warnings: List[str] = []
        sch_path = self.resources_dir / "COBieRules.sch"
        compile_xslt_path = self.resources_dir / "iso_svrl_for_xslt2.xsl"
        skeleton_path = self.resources_dir / "iso_schematron_skeleton_for_saxon.xsl"
        functions_path = self.resources_dir / "COBieRules_Functions.xsl"
        if not sch_path.exists():
            raise RuntimeError(f"COBieRules.sch missing at {sch_path}")
        if not compile_xslt_path.exists():
            raise RuntimeError(f"iso_svrl_for_xslt2.xsl missing at {compile_xslt_path}")

        phase_id = self._resolve_phase_id_elementtree(sch_path)
        if phase_id:
            logs.append(f"schematron_phase_selected stage={self.stage} phase={phase_id}")
        else:
            warnings.append(
                f"No explicit phase mapping found for stage '{self.stage}'. Running with default Schematron phase."
            )
        logs.append(self._phase_log("schematron_source_loaded", sch_path))
        if skeleton_path.exists():
            logs.append(self._phase_log("schematron_skeleton_loaded", skeleton_path))
        if functions_path.exists():
            logs.append(self._phase_log("schematron_functions_loaded", functions_path))

        compile_stdout, compile_stderr = _run_saxon_xslt(
            xml_input_path=sch_path,
            stylesheet_path=compile_xslt_path,
            output_path=compiled_xslt_output_path,
            params={"phase": phase_id} if phase_id else {},
            logs=logs,
        )
        logs.append(self._phase_log("schematron_compiled_to_xslt", compiled_xslt_output_path))
        logs.append(f"compiled_validation_xslt path={compiled_xslt_output_path}")
        if compile_stdout:
            logs.append(f"saxon_compile_stdout={compile_stdout.strip()}")
        if compile_stderr:
            logs.append(f"saxon_compile_stderr={compile_stderr.strip()}")

        detected_hrefs = self._collect_xslt_dependency_hrefs(compiled_xslt_output_path)
        logs.append(f"compiled_xslt_dependencies_detected hrefs={','.join(detected_hrefs) or '(none)'}")
        rewritten = self._rewrite_xslt_dependency_hrefs(compiled_xslt_output_path)
        for source_href, target_href in rewritten:
            logs.append(f"compiled_xslt_dependency_rewritten from={source_href} to={target_href}")
        unresolved = self._find_unresolved_relative_hrefs(compiled_xslt_output_path)
        if unresolved:
            warnings.append(
                f"compiled_xslt_unresolved_relative_dependencies={','.join(unresolved)} "
                f"base_dir={compiled_xslt_output_path.parent}"
            )
            logs.append(f"compiled_xslt_unresolved_relative_dependencies hrefs={','.join(unresolved)}")
        logs.append(f"validation_resolver_base path={compiled_xslt_output_path.parent}")

        validation_stdout, validation_stderr = _run_saxon_xslt(
            xml_input_path=cobie_xml_path,
            stylesheet_path=compiled_xslt_output_path,
            output_path=svrl_output_path,
            params={},
            logs=logs,
        )
        logs.append(self._phase_log("validation_xslt_applied", compiled_xslt_output_path))
        logs.append(self._phase_log("svrl_generated", svrl_output_path))
        if validation_stdout:
            logs.append(f"saxon_validation_stdout={validation_stdout.strip()}")
        if validation_stderr:
            logs.append(f"saxon_validation_stderr={validation_stderr.strip()}")

        root = ET.parse(str(svrl_output_path)).getroot()
        failed_asserts = len(root.findall(".//{*}failed-assert"))
        successful_reports = len(root.findall(".//{*}successful-report"))
        diagnostics = len(root.findall(".//{*}diagnostic-reference"))
        return (
            {
                "failed_asserts": failed_asserts,
                "successful_reports": successful_reports,
                "diagnostics": diagnostics,
            },
            warnings,
            "",
            logs,
        )

    def _resolve_phase_id(self, sch_doc: Any) -> Optional[str]:
        ns = {"sch": "http://purl.oclc.org/dsdl/schematron"}
        stage_value = (self.stage or "").strip().upper()
        phase_ids = [str(v) for v in sch_doc.xpath("/sch:schema/sch:phase/@id", namespaces=ns)]
        if not phase_ids:
            return None
        if stage_value in phase_ids:
            return stage_value
        stage_keywords = {"D": "design", "C": "construction"}
        keyword = stage_keywords.get(stage_value, stage_value.lower())
        for candidate in phase_ids:
            if keyword and keyword in candidate.lower():
                return candidate
        return phase_ids[0]

    def _resolve_phase_id_elementtree(self, sch_path: Path) -> Optional[str]:
        ns = {"sch": "http://purl.oclc.org/dsdl/schematron"}
        doc = ET.parse(str(sch_path))
        root = doc.getroot()
        phase_ids = [str(el.attrib.get("id", "")).strip() for el in root.findall("sch:phase", ns) if el.attrib.get("id")]
        if not phase_ids:
            return None
        stage_value = (self.stage or "").strip().upper()
        if stage_value in phase_ids:
            return stage_value
        stage_keywords = {"D": "design", "C": "construction"}
        keyword = stage_keywords.get(stage_value, stage_value.lower())
        for candidate in phase_ids:
            if keyword and keyword in candidate.lower():
                return candidate
        return phase_ids[0]

    def _collect_schematron_diagnostics(self, sch_doc: Any) -> List[str]:
        ns = {"sch": "http://purl.oclc.org/dsdl/schematron"}
        pattern_ids = [str(v) for v in sch_doc.xpath("/sch:schema/sch:pattern/@id", namespaces=ns)]
        rule_ids = [str(v) for v in sch_doc.xpath("//sch:rule/@id", namespaces=ns)]
        phase_refs = [str(v) for v in sch_doc.xpath("/sch:schema/sch:phase/sch:active/@pattern", namespaces=ns)]
        unresolved_refs = sorted(set(ref for ref in phase_refs if ref not in pattern_ids))
        details = [
            f"Schematron diagnostics: pattern_ids={','.join(pattern_ids) or '(none)'}",
            f"Schematron diagnostics: rule_ids={','.join(rule_ids) or '(none)'}",
            f"Schematron diagnostics: phase_pattern_refs={','.join(phase_refs) or '(none)'}",
        ]
        if unresolved_refs:
            details.append(f"Schematron diagnostics: unresolved_pattern_refs={','.join(unresolved_refs)}")
        return details

    def _phase_log(self, phase: str, artifact: Path) -> str:
        size = artifact.stat().st_size if artifact.exists() else 0
        return f"{phase} path={artifact} size_bytes={size}"

    def _collect_xslt_dependency_hrefs(self, xslt_path: Path) -> List[str]:
        try:
            from lxml import etree
        except Exception:
            return []
        doc = etree.parse(str(xslt_path))
        nodes = doc.xpath("//*[local-name()='import' or local-name()='include'][@href]")
        return [str(node.get("href", "")).strip() for node in nodes]

    def _rewrite_xslt_dependency_hrefs(self, xslt_path: Path) -> List[Tuple[str, str]]:
        try:
            from lxml import etree
        except Exception:
            return []
        parser = etree.XMLParser(remove_blank_text=False)
        doc = etree.parse(str(xslt_path), parser)
        rewritten: List[Tuple[str, str]] = []
        for node in doc.xpath("//*[local-name()='import' or local-name()='include'][@href]"):
            href = str(node.get("href", "")).strip()
            if not href or "://" in href or href.startswith("/"):
                continue
            candidate = (self.resources_dir / href).resolve()
            if candidate.exists():
                absolute_href = candidate.as_uri()
                node.set("href", absolute_href)
                rewritten.append((href, absolute_href))
        if rewritten:
            doc.write(str(xslt_path), encoding="utf-8", pretty_print=True, xml_declaration=True)
        return rewritten

    def _find_unresolved_relative_hrefs(self, xslt_path: Path) -> List[str]:
        unresolved: List[str] = []
        for href in self._collect_xslt_dependency_hrefs(xslt_path):
            if not href or "://" in href or href.startswith("/"):
                continue
            unresolved.append(href)
        return unresolved

    def _safe_artifact_preview(self, artifact: Path) -> List[str]:
        if not artifact.exists():
            return [f"artifact_preview unavailable path={artifact} reason=missing"]
        lines = artifact.read_text(encoding="utf-8", errors="replace").splitlines()[:20]
        preview = " | ".join(lines)
        return [f"artifact_preview path={artifact} lines_1_20={preview}"]

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

    def render(
        self,
        svrl_path: Path,
        html_output_path: Path,
        summary: Dict[str, Any],
        warnings: List[str],
    ) -> Tuple[List[str], str]:
        engine = _get_xslt_engine()
        logs: List[str] = []
        error = ""
        css_path = self.resources_dir / "SpaceReport.css"
        target_css = html_output_path.parent / "SpaceReport.css"
        if css_path.exists():
            shutil.copy2(css_path, target_css)
            logs.append(f"html_css_copied path={target_css} size_bytes={target_css.stat().st_size}")

        xslt_candidates = [
            self.resources_dir / "SVRL_HTML_altLocation.xslt",
            self.resources_dir / "_SVRL_HTML_altLocation.xslt",
        ]

        for xslt_path in xslt_candidates:
            if not xslt_path.exists():
                continue
            try:
                if engine == "saxon":
                    _run_saxon_xslt(
                        xml_input_path=svrl_path,
                        stylesheet_path=xslt_path,
                        output_path=html_output_path,
                        params={},
                        logs=logs,
                    )
                else:
                    from lxml import etree
                    svrl_doc = etree.parse(str(svrl_path))
                    xslt_doc = etree.parse(str(xslt_path))
                    transform = etree.XSLT(xslt_doc)
                    html_doc = transform(svrl_doc)
                    html_output_path.write_bytes(
                        etree.tostring(html_doc, encoding="utf-8", pretty_print=True, method="html")
                    )
                logs.append(f"html_generated path={html_output_path} size_bytes={html_output_path.stat().st_size}")
                return logs, error
            except Exception as exc:
                error = f"SVRL-to-HTML transform failed: {exc}"
                continue

        self._write_fallback_html(html_output_path, summary, warnings)
        logs.append(f"html_generated path={html_output_path} size_bytes={html_output_path.stat().st_size}")
        if error and svrl_path.exists():
            preview = svrl_path.read_text(encoding="utf-8", errors="replace").splitlines()[:20]
            warnings.append(f"svrl_preview_first_20={' | '.join(preview)}")
        return logs, error

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


def _get_xslt_engine() -> str:
    return os.getenv("COBIEQC_XSLT_ENGINE", "saxon").strip().lower() or "saxon"


def _resolve_saxon_command() -> List[str]:
    configured = os.getenv("COBIEQC_SAXON_CMD", "").strip()
    if configured:
        return shlex.split(configured)
    jar_candidates = [
        os.getenv("COBIEQC_SAXON_JAR_PATH", "").strip(),
        str(APP_ROOT / "vendor" / "saxon" / "Saxon-HE.jar"),
        str(APP_ROOT / "vendor" / "saxon" / "saxon-he.jar"),
        "/usr/share/java/Saxon-HE.jar",
        "/usr/share/java/saxon-he.jar",
    ]
    for candidate in jar_candidates:
        if not candidate:
            continue
        jar_path = Path(candidate).expanduser().resolve()
        if jar_path.exists() and jar_path.is_file():
            java_bin = os.getenv("JAVA_BIN", "java").strip() or "java"
            return [java_bin, "-jar", str(jar_path)]
    raise RuntimeError(
        "Saxon HE is required for COBieQC XSLT 2.0 execution but was not found. "
        "Set COBIEQC_SAXON_JAR_PATH or COBIEQC_SAXON_CMD."
    )


def _run_saxon_xslt(
    xml_input_path: Path,
    stylesheet_path: Path,
    output_path: Path,
    params: Dict[str, str],
    logs: List[str],
) -> Tuple[str, str]:
    command = [
        *_resolve_saxon_command(),
        f"-s:{xml_input_path}",
        f"-xsl:{stylesheet_path}",
        f"-o:{output_path}",
    ]
    for key, value in params.items():
        command.append(f"{key}={value}")
    logs.append(f"xslt_engine=saxon command={' '.join(shlex.quote(part) for part in command)}")
    logs.append(f"xslt_engine=saxon input_xml_path={xml_input_path}")
    logs.append(f"xslt_engine=saxon stylesheet_path={stylesheet_path}")
    logs.append(f"xslt_engine=saxon output_path={output_path}")
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.stdout.strip():
        logs.append(f"xslt_engine=saxon stdout={completed.stdout.strip()}")
    if completed.stderr.strip():
        logs.append(f"xslt_engine=saxon stderr={completed.stderr.strip()}")
    if completed.returncode != 0:
        raise RuntimeError(
            f"Saxon XSLT failed (exit={completed.returncode}) for stylesheet={stylesheet_path} input={xml_input_path}. "
            f"stderr={completed.stderr.strip()}"
        )
    if not output_path.exists():
        raise RuntimeError(
            f"Saxon XSLT did not produce expected output file: {output_path} "
            f"(stylesheet={stylesheet_path}, input={xml_input_path})"
        )
    return completed.stdout, completed.stderr


def run_cobieqc_native(input_xlsx_path: str, stage: str, job_dir: str, resources_dir: Path) -> CobieQcNativeResult:
    input_path = Path(input_xlsx_path).resolve()
    out_dir = Path(job_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cobie_xml_path = out_dir / "generated_cobie.xml"
    compiled_validation_xslt_path = out_dir / "compiled_validation.xsl"
    svrl_xml_path = out_dir / "validation_result.svrl.xml"
    html_path = out_dir / "final_report.html"
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
        logs.append(f"generated_cobie_xml path={cobie_xml_path} size_bytes={cobie_xml_path.stat().st_size}")

        logs.append("XML generated")
        validator = SchematronPipeline(resources_dir=resources_dir, stage=stage)
        summary, svrl_warnings, svrl_error, schematron_logs = validator.validate(
            cobie_xml_path,
            svrl_xml_path,
            compiled_validation_xslt_path,
        )
        logs.extend(schematron_logs)

        warnings = [*parse_warnings, *svrl_warnings]
        summary["warnings"] = warnings
        summary["stage"] = stage

        renderer = SvrlHtmlRenderer(resources_dir=resources_dir)
        html_logs, html_error = renderer.render(svrl_xml_path, html_path, summary, warnings)
        logs.extend(html_logs)
        if html_error:
            warnings.append(f"Schematron execution fallback used: html generation failed during SVRL-to-HTML transform: {html_error}")

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
