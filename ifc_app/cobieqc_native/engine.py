import logging
import os
import shlex
import shutil
import subprocess
from datetime import date, datetime
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import load_workbook
from openpyxl.utils.datetime import from_excel
from xml.etree import ElementTree as ET

LOGGER = logging.getLogger("ifc_app.cobieqc.native")
APP_ROOT = Path(__file__).resolve().parents[2]
COBIE_ENTITY_NAMES = ["Contact", "Facility", "Floor", "Space", "Zone", "Type", "Component"]


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


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _namespace_uri(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def _safe_iterparse_namespaces(xml_path: Path) -> Dict[str, str]:
    namespaces: Dict[str, str] = {}
    try:
        for _, payload in ET.iterparse(str(xml_path), events=("start-ns",)):
            prefix, uri = payload
            namespaces[prefix or "(default)"] = uri
    except Exception:
        return {}
    return namespaces


def _build_element_parent_map(root: ET.Element) -> Dict[ET.Element, Optional[ET.Element]]:
    parent_map: Dict[ET.Element, Optional[ET.Element]] = {root: None}
    for parent in root.iter():
        for child in list(parent):
            parent_map[child] = parent
    return parent_map


def _element_path(element: ET.Element, parent_map: Dict[ET.Element, Optional[ET.Element]]) -> str:
    parts = []
    cursor: Optional[ET.Element] = element
    while cursor is not None:
        parts.append(_local_name(cursor.tag))
        cursor = parent_map.get(cursor)
    return "/" + "/".join(reversed(parts))


def _find_reference_xml_path(out_dir: Path) -> Optional[Path]:
    configured = os.getenv("COBIEQC_REFERENCE_XML_PATH", "").strip()
    if configured:
        candidate = Path(configured).expanduser().resolve()
        if candidate.exists() and candidate.is_file():
            return candidate
    for filename in ("legacy_generated_cobie.xml", "legacy_cobie.xml", "known_good_cobie.xml", "bsn.xml"):
        candidate = out_dir / filename
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _find_reference_svrl_path(out_dir: Path) -> Optional[Path]:
    configured = os.getenv("COBIEQC_REFERENCE_SVRL_PATH", "").strip()
    if configured:
        candidate = Path(configured).expanduser().resolve()
        if candidate.exists() and candidate.is_file():
            return candidate
    for filename in ("legacy_validation_result.svrl.xml", "reference_validation_result.svrl.xml", "legacy.svrl.xml"):
        candidate = out_dir / filename
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _inspect_cobie_xml(cobie_xml_path: Path) -> Dict[str, Any]:
    doc = ET.parse(str(cobie_xml_path))
    root = doc.getroot()
    parent_map = _build_element_parent_map(root)
    first_level = [_local_name(child.tag) for child in list(root)]
    lower_names = {name.lower() for name in COBIE_ENTITY_NAMES}
    counts: Dict[str, int] = {}
    sample_paths: Dict[str, List[str]] = {}
    sheet_to_rows: Dict[str, int] = {}
    created_on_samples: List[str] = []
    for entity_name in COBIE_ENTITY_NAMES:
        needle = entity_name.lower()
        count = 0
        samples: List[str] = []
        for node in root.iter():
            node_name = _local_name(node.tag).lower()
            if node_name == needle or node_name == f"{needle}s":
                count += 1
                if len(samples) < 3:
                    samples.append(_element_path(node, parent_map))
        counts[entity_name] = count
        sample_paths[entity_name] = samples
    for node in root.iter():
        source_sheet = str(node.attrib.get("sourceSheet", "")).strip()
        if source_sheet:
            sheet_to_rows[source_sheet] = sheet_to_rows.get(source_sheet, 0) + 1
        if _local_name(node.tag).lower() == "createdon":
            text = (node.text or "").strip()
            if text and len(created_on_samples) < 10:
                created_on_samples.append(text)
    model_path_count = sum(1 for node in root.iter() if _local_name(node.tag).lower() in lower_names)
    cross_ref = _cross_reference_created_by(root)
    return {
        "root_element_name": _local_name(root.tag),
        "root_namespace": _namespace_uri(root.tag) or "(none)",
        "first_level_children": first_level,
        "entity_counts": counts,
        "sample_paths": sample_paths,
        "sheet_row_counts": sheet_to_rows,
        "model_path_count": model_path_count,
        "created_on_samples": created_on_samples,
        "cross_reference": cross_ref,
    }


def _cross_reference_created_by(root: ET.Element) -> Dict[str, Any]:
    contact_names = {
        (name_node.text or "").strip()
        for name_node in root.findall(".//{*}Contacts/{*}Contact/{*}Name")
        if (name_node.text or "").strip()
    }
    unmatched: List[str] = []
    total = 0
    matched = 0
    for parent in root.iter():
        for child in list(parent):
            if _local_name(child.tag).lower() != "createdby":
                continue
            total += 1
            created_by = (child.text or "").strip()
            if created_by and created_by in contact_names:
                matched += 1
                continue
            parent_sheet = parent.attrib.get("sourceSheet", "")
            parent_row = parent.attrib.get("rowNumber", "")
            unmatched.append(f"{created_by or '(blank)'}@{parent_sheet}#{parent_row}")
    return {
        "contact_name_count": len(contact_names),
        "created_by_total": total,
        "created_by_matched": matched,
        "created_by_unmatched": max(total - matched, 0),
        "unmatched_samples": unmatched[:10],
    }


def _collect_xml_structure_snapshot(xml_path: Path) -> Dict[str, Any]:
    doc = ET.parse(str(xml_path))
    root = doc.getroot()
    parent_map = _build_element_parent_map(root)
    path_counter: Counter[str] = Counter()
    frequency: Counter[str] = Counter()
    for node in root.iter():
        name = _local_name(node.tag)
        frequency[name] += 1
        path_counter[_element_path(node, parent_map)] += 1
    return {
        "root_name": _local_name(root.tag),
        "root_namespace": _namespace_uri(root.tag) or "(none)",
        "namespaces": _safe_iterparse_namespaces(xml_path),
        "element_frequency": dict(frequency),
        "xpath_inventory": dict(path_counter),
    }


def _compare_xml_structure(generated_xml_path: Path, reference_xml_path: Path) -> Dict[str, Any]:
    generated = _collect_xml_structure_snapshot(generated_xml_path)
    reference = _collect_xml_structure_snapshot(reference_xml_path)

    generated_paths = set(generated["xpath_inventory"].keys())
    reference_paths = set(reference["xpath_inventory"].keys())
    generated_elements = set(generated["element_frequency"].keys())
    reference_elements = set(reference["element_frequency"].keys())

    missing_paths = sorted(reference_paths - generated_paths)
    extra_paths = sorted(generated_paths - reference_paths)
    missing_elements = sorted(reference_elements - generated_elements)
    extra_elements = sorted(generated_elements - reference_elements)
    frequency_deltas: List[str] = []
    for key in sorted(generated_elements | reference_elements):
        gen_count = int(generated["element_frequency"].get(key, 0))
        ref_count = int(reference["element_frequency"].get(key, 0))
        if gen_count != ref_count:
            frequency_deltas.append(f"{key}:generated={gen_count},reference={ref_count}")

    return {
        "reference_path": str(reference_xml_path),
        "generated_root": generated["root_name"],
        "reference_root": reference["root_name"],
        "generated_namespace": generated["root_namespace"],
        "reference_namespace": reference["root_namespace"],
        "generated_namespaces": generated["namespaces"],
        "reference_namespaces": reference["namespaces"],
        "missing_paths": missing_paths[:100],
        "extra_paths": extra_paths[:100],
        "missing_elements": missing_elements[:100],
        "extra_elements": extra_elements[:100],
        "frequency_deltas": frequency_deltas[:100],
    }


def _inspect_svrl(svrl_output_path: Path) -> Dict[str, Any]:
    root = ET.parse(str(svrl_output_path)).getroot()
    active_patterns = root.findall(".//{*}active-pattern")
    active_pattern_ids = [str(node.attrib.get("id", "")).strip() for node in active_patterns if node.attrib.get("id")]
    fired_rules = root.findall(".//{*}fired-rule")
    failed_asserts = root.findall(".//{*}failed-assert")
    successful_reports = root.findall(".//{*}successful-report")
    diagnostics = root.findall(".//{*}diagnostic-reference")
    failed_assert_counts: Counter[str] = Counter()
    failed_assert_rows: Counter[str] = Counter()
    for node in failed_asserts:
        rule_id = str(node.attrib.get("id", "")).strip() or str(node.attrib.get("test", "")).strip() or "(unknown)"
        failed_assert_counts[rule_id] += 1
        location = str(node.attrib.get("location", "")).strip() or "(unknown)"
        failed_assert_rows[location] += 1
    top_failing_rules = [{"rule": key, "count": count} for key, count in failed_assert_counts.most_common(10)]
    return {
        "fired_rules": len(fired_rules),
        "failed_asserts": len(failed_asserts),
        "successful_reports": len(successful_reports),
        "rules_matched": len(fired_rules),
        "rules_evaluated": len(failed_asserts) + len(successful_reports),
        "active_patterns": len(active_patterns),
        "active_pattern_ids": active_pattern_ids[:5],
        "diagnostics": len(diagnostics),
        "failed_assert_count_by_rule": dict(failed_assert_counts),
        "top_failing_rules": top_failing_rules,
        "affected_rows_sample": [key for key, _ in failed_assert_rows.most_common(10)],
    }


def _extract_svrl_failure_index(root: ET.Element) -> Dict[str, Any]:
    per_rule: Counter[str] = Counter()
    per_rule_rows: Dict[str, set] = {}
    for node in root.findall(".//{*}failed-assert"):
        rule_id = str(node.attrib.get("id", "")).strip() or str(node.attrib.get("test", "")).strip() or "(unknown)"
        per_rule[rule_id] += 1
        row = str(node.attrib.get("location", "")).strip() or "(unknown)"
        if rule_id not in per_rule_rows:
            per_rule_rows[rule_id] = set()
        per_rule_rows[rule_id].add(row)
    return {"counts": per_rule, "rows": per_rule_rows}


def _compare_svrl_outputs(generated_svrl_path: Path, reference_svrl_path: Path) -> Dict[str, Any]:
    generated_root = ET.parse(str(generated_svrl_path)).getroot()
    reference_root = ET.parse(str(reference_svrl_path)).getroot()
    generated = _extract_svrl_failure_index(generated_root)
    reference = _extract_svrl_failure_index(reference_root)
    generated_rules = set(generated["counts"].keys())
    reference_rules = set(reference["counts"].keys())
    all_rules = sorted(generated_rules | reference_rules)
    count_deltas: List[str] = []
    row_deltas: List[str] = []
    for rule_id in all_rules:
        gen_count = int(generated["counts"].get(rule_id, 0))
        ref_count = int(reference["counts"].get(rule_id, 0))
        if gen_count != ref_count:
            count_deltas.append(f"{rule_id}:generated={gen_count},reference={ref_count}")
        gen_rows = generated["rows"].get(rule_id, set())
        ref_rows = reference["rows"].get(rule_id, set())
        if gen_rows != ref_rows:
            missing = sorted(ref_rows - gen_rows)[:3]
            extra = sorted(gen_rows - ref_rows)[:3]
            row_deltas.append(f"{rule_id}:missing_rows={missing},extra_rows={extra}")
    return {
        "reference_svrl_path": str(reference_svrl_path),
        "generated_failed_asserts": sum(generated["counts"].values()),
        "reference_failed_asserts": sum(reference["counts"].values()),
        "missing_rules": sorted(reference_rules - generated_rules),
        "extra_rules": sorted(generated_rules - reference_rules),
        "count_deltas": count_deltas[:100],
        "row_deltas": row_deltas[:100],
    }


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
        root = ET.Element("COBie")
        root.set("stage", self.stage)
        root.set("source", self.workbook_path.name)

        if self.template_path and self.template_path.exists():
            root.set("template", self.template_path.name)
        else:
            warnings.append("COBieExcelTemplate.xml missing; using direct entity sheet mapping")

        canonical_entity_map: Dict[str, Tuple[str, str]] = {
            "contact": ("Contacts", "Contact"),
            "facility": ("Facilities", "Facility"),
            "floor": ("Floors", "Floor"),
            "space": ("Spaces", "Space"),
            "zone": ("Zones", "Zone"),
            "type": ("Types", "Type"),
            "component": ("Components", "Component"),
        }
        container_elements: Dict[str, ET.Element] = {}

        def _resolve_container(sheet_title: str) -> Tuple[str, str]:
            normalized = _clean_tag(sheet_title).lower()
            if normalized.endswith("s"):
                normalized = normalized[:-1]
            if normalized in canonical_entity_map:
                return canonical_entity_map[normalized]
            entity = _clean_tag(sheet_title)
            if entity.endswith("s"):
                entity = entity[:-1] or entity
            return f"{entity}s", entity

        for sheet in wb.worksheets:
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                warnings.append(f"Sheet '{sheet.title}' is empty")
                continue

            container_name, entity_name = _resolve_container(sheet.title)
            container_el = container_elements.get(container_name)
            if container_el is None:
                container_el = ET.SubElement(root, container_name)
                container_elements[container_name] = container_el

            header = [_clean_tag(v if v is not None else "Column") for v in rows[0]]
            entity_ordinal = 0
            for row_idx, row in enumerate(rows[1:], start=1):
                if all(cell in (None, "") for cell in row):
                    continue
                entity_ordinal += 1
                row_el = ET.SubElement(container_el, entity_name)
                row_el.set("sourceSheet", sheet.title)
                row_el.set("rowNumber", str(row_idx))
                row_el.set("sheetOrdinal", str(entity_ordinal))
                for col_idx, cell in enumerate(row):
                    col_name = header[col_idx] if col_idx < len(header) else f"Column_{col_idx + 1}"
                    cell_el = ET.SubElement(row_el, col_name)
                    normalized = self._normalize_cell_value(
                        cell=cell,
                        column_name=col_name,
                        workbook_epoch=wb.epoch,
                        warnings=warnings,
                    )
                    if normalized is not None:
                        cell_el.text = normalized

        return ET.tostring(root, encoding="utf-8", xml_declaration=True), warnings

    def _normalize_cell_value(
        self,
        cell: Any,
        column_name: str,
        workbook_epoch: datetime,
        warnings: List[str],
    ) -> Optional[str]:
        if cell is None:
            return None
        name = column_name.strip().lower()
        if name == "createdon":
            normalized_date = self._normalize_created_on(cell, workbook_epoch, warnings)
            return normalized_date
        if isinstance(cell, str):
            trimmed = cell.strip()
            return trimmed or None
        return str(cell).strip() or None

    def _normalize_created_on(self, value: Any, workbook_epoch: datetime, warnings: List[str]) -> Optional[str]:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S")
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day).strftime("%Y-%m-%dT%H:%M:%S")
        if isinstance(value, (float, int)):
            try:
                dt = from_excel(value, epoch=workbook_epoch)
                if isinstance(dt, datetime):
                    return dt.strftime("%Y-%m-%dT%H:%M:%S")
                if isinstance(dt, date):
                    return datetime(dt.year, dt.month, dt.day).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception as exc:
                warnings.append(f"CreatedOn numeric value conversion failed value={value} error={exc}")
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            warnings.append(f"CreatedOn value not normalized to ISO8601 value='{text}'")
            return text


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

    def _phase_catalog(self, sch_root: Any, use_xpath: bool = True) -> Dict[str, Any]:
        if use_xpath:
            ns = {"sch": "http://purl.oclc.org/dsdl/schematron"}
            pattern_ids = [str(v) for v in sch_root.xpath("/sch:schema/sch:pattern/@id", namespaces=ns)]
            phase_nodes = sch_root.xpath("/sch:schema/sch:phase", namespaces=ns)
            phase_map: Dict[str, List[str]] = {}
            for phase in phase_nodes:
                phase_id = str(phase.attrib.get("id", "")).strip()
                if not phase_id:
                    continue
                refs = [str(v) for v in phase.xpath("./sch:active/@pattern", namespaces=ns)]
                phase_map[phase_id] = refs
            return {"phase_ids": list(phase_map.keys()), "phase_patterns": phase_map, "pattern_ids": pattern_ids}
        ns = {"sch": "http://purl.oclc.org/dsdl/schematron"}
        pattern_ids = [str(el.attrib.get("id", "")).strip() for el in sch_root.findall("sch:pattern", ns)]
        phase_map = {}
        for phase in sch_root.findall("sch:phase", ns):
            phase_id = str(phase.attrib.get("id", "")).strip()
            if not phase_id:
                continue
            refs = [str(active.attrib.get("pattern", "")).strip() for active in phase.findall("sch:active", ns)]
            phase_map[phase_id] = [ref for ref in refs if ref]
        return {"phase_ids": list(phase_map.keys()), "phase_patterns": phase_map, "pattern_ids": pattern_ids}

    def _resolve_phase_ids(self, phase_ids: List[str]) -> List[str]:
        configured = os.getenv("COBIEQC_SCHEMATRON_PHASES", "").strip()
        if configured:
            desired = [part.strip() for part in configured.split(",") if part.strip()]
            selected = [phase for phase in desired if phase in phase_ids]
            if selected:
                return selected
        if not phase_ids:
            return []
        stage_value = (self.stage or "").strip().upper()
        keyword_map = {"D": ("design", "information"), "C": ("construction",)}
        keywords = keyword_map.get(stage_value, (stage_value.lower(),))
        exact = [phase for phase in phase_ids if phase.upper() == stage_value]
        matching = [phase for phase in phase_ids if any(keyword in phase.lower() for keyword in keywords if keyword)]
        selected = []
        for item in [*exact, *matching]:
            if item not in selected:
                selected.append(item)
        return selected or [phase_ids[0]]

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

        catalog = self._phase_catalog(sch_doc, use_xpath=True)
        logs.append(f"schematron_available_phases={','.join(catalog['phase_ids']) or '(none)'}")
        for phase_id, patterns in catalog["phase_patterns"].items():
            logs.append(f"schematron_phase_patterns phase={phase_id} active_patterns={','.join(patterns) or '(none)'}")
        phase_ids = self._resolve_phase_ids(catalog["phase_ids"])
        logs.append(f"schematron_phase_selected stage={self.stage} phases={','.join(phase_ids) or '(default)'}")
        if not phase_ids:
            warnings.append(f"No phases discovered for stage '{self.stage}'. Running default Schematron phase.")

        if not phase_ids:
            phase_ids = [""]
        phase_svrl_roots: List[Any] = []
        phase_compiled_paths: List[Path] = []
        compile_step = "schematron compile"
        validation_step = "XML validation"
        for idx, phase_id in enumerate(phase_ids):
            phase_suffix = f".phase_{idx + 1}"
            phase_compiled_path = (
                compiled_xslt_output_path.parent / f"{compiled_xslt_output_path.stem}{phase_suffix}{compiled_xslt_output_path.suffix}"
            )
            phase_svrl_path = svrl_output_path.parent / f"{svrl_output_path.stem}{phase_suffix}{svrl_output_path.suffix}"
            try:
                compiler_doc = etree.parse(str(compile_xslt_path))
                compiler = etree.XSLT(compiler_doc)
                compile_args = {"phase": etree.XSLT.strparam(phase_id)} if phase_id else {}
                compiled_validation_doc = compiler(sch_doc, **compile_args)
                phase_compiled_path.write_bytes(
                    etree.tostring(compiled_validation_doc, encoding="utf-8", pretty_print=True, xml_declaration=True)
                )
                phase_compiled_paths.append(phase_compiled_path)
                logs.append(self._phase_log("schematron_compiled_to_xslt", phase_compiled_path))
                logs.append(f"compiled_validation_xslt phase={phase_id or '(default)'} path={phase_compiled_path}")
                detected_hrefs = self._collect_xslt_dependency_hrefs(phase_compiled_path)
                logs.append(f"compiled_xslt_dependencies_detected phase={phase_id or '(default)'} hrefs={','.join(detected_hrefs) or '(none)'}")
                rewritten = self._rewrite_xslt_dependency_hrefs(phase_compiled_path)
                for source_href, target_href in rewritten:
                    logs.append(f"compiled_xslt_dependency_rewritten phase={phase_id or '(default)'} from={source_href} to={target_href}")
                unresolved = self._find_unresolved_relative_hrefs(phase_compiled_path)
                if unresolved:
                    warnings.append(
                        f"compiled_xslt_unresolved_relative_dependencies={','.join(unresolved)} "
                        f"base_dir={phase_compiled_path.parent}"
                    )
                    logs.append(f"compiled_xslt_unresolved_relative_dependencies phase={phase_id or '(default)'} hrefs={','.join(unresolved)}")
                logs.append(f"validation_resolver_base path={phase_compiled_path.parent}")
                xml_doc = etree.parse(str(cobie_xml_path))
                validation_doc = etree.parse(str(phase_compiled_path))
                validation_transform = etree.XSLT(validation_doc)
                svrl_doc = validation_transform(xml_doc)
                phase_svrl_path.write_bytes(
                    etree.tostring(svrl_doc, encoding="utf-8", pretty_print=True, xml_declaration=True)
                )
                phase_root = etree.parse(str(phase_svrl_path)).getroot()
                phase_root.attrib["data-phase"] = phase_id or "(default)"
                phase_svrl_roots.append(phase_root)
                logs.append(self._phase_log("validation_xslt_applied", phase_compiled_path))
                logs.append(self._phase_log("svrl_generated", phase_svrl_path))
            except Exception as exc:
                diagnostics = self._collect_schematron_diagnostics(sch_doc)
                warnings.append(f"Schematron execution fallback used: failed during {compile_step}/{validation_step}: {exc}")
                warnings.extend(diagnostics)
                self._write_fallback_svrl(svrl_output_path, warnings, [str(exc)])
                return ({"failed_asserts": 0, "successful_reports": 0, "diagnostics": len(warnings)}, warnings, str(exc), logs)

        if phase_svrl_roots:
            merged_root = phase_svrl_roots[0]
            for extra_root in phase_svrl_roots[1:]:
                for child in list(extra_root):
                    merged_root.append(child)
            svrl_output_path.write_bytes(
                etree.tostring(merged_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
            )
            if phase_compiled_paths:
                shutil.copy2(phase_compiled_paths[-1], compiled_xslt_output_path)
        logs.append(self._phase_log("svrl_generated", svrl_output_path))

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

        sch_root = ET.parse(str(sch_path)).getroot()
        catalog = self._phase_catalog(sch_root, use_xpath=False)
        logs.append(f"schematron_available_phases={','.join(catalog['phase_ids']) or '(none)'}")
        for phase_id, patterns in catalog["phase_patterns"].items():
            logs.append(f"schematron_phase_patterns phase={phase_id} active_patterns={','.join(patterns) or '(none)'}")
        phase_ids = self._resolve_phase_ids(catalog["phase_ids"])
        logs.append(f"schematron_phase_selected stage={self.stage} phases={','.join(phase_ids) or '(default)'}")
        if not phase_ids:
            phase_ids = [""]
        logs.append(self._phase_log("schematron_source_loaded", sch_path))
        if skeleton_path.exists():
            logs.append(self._phase_log("schematron_skeleton_loaded", skeleton_path))
        if functions_path.exists():
            logs.append(self._phase_log("schematron_functions_loaded", functions_path))

        phase_svrl_paths: List[Path] = []
        phase_compiled_paths: List[Path] = []
        for idx, phase_id in enumerate(phase_ids):
            phase_suffix = f".phase_{idx + 1}"
            phase_compiled_path = (
                compiled_xslt_output_path.parent / f"{compiled_xslt_output_path.stem}{phase_suffix}{compiled_xslt_output_path.suffix}"
            )
            phase_svrl_path = svrl_output_path.parent / f"{svrl_output_path.stem}{phase_suffix}{svrl_output_path.suffix}"
            compile_stdout, compile_stderr = _run_saxon_xslt(
                xml_input_path=sch_path,
                stylesheet_path=compile_xslt_path,
                output_path=phase_compiled_path,
                params={"phase": phase_id} if phase_id else {},
                logs=logs,
            )
            phase_compiled_paths.append(phase_compiled_path)
            logs.append(self._phase_log("schematron_compiled_to_xslt", phase_compiled_path))
            logs.append(f"compiled_validation_xslt phase={phase_id or '(default)'} path={phase_compiled_path}")
            if compile_stdout:
                logs.append(f"saxon_compile_stdout phase={phase_id or '(default)'} stdout={compile_stdout.strip()}")
            if compile_stderr:
                logs.append(f"saxon_compile_stderr phase={phase_id or '(default)'} stderr={compile_stderr.strip()}")
            detected_hrefs = self._collect_xslt_dependency_hrefs(phase_compiled_path)
            logs.append(f"compiled_xslt_dependencies_detected phase={phase_id or '(default)'} hrefs={','.join(detected_hrefs) or '(none)'}")
            rewritten = self._rewrite_xslt_dependency_hrefs(phase_compiled_path)
            for source_href, target_href in rewritten:
                logs.append(f"compiled_xslt_dependency_rewritten phase={phase_id or '(default)'} from={source_href} to={target_href}")
            unresolved = self._find_unresolved_relative_hrefs(phase_compiled_path)
            if unresolved:
                warnings.append(
                    f"compiled_xslt_unresolved_relative_dependencies={','.join(unresolved)} "
                    f"base_dir={phase_compiled_path.parent}"
                )
                logs.append(f"compiled_xslt_unresolved_relative_dependencies phase={phase_id or '(default)'} hrefs={','.join(unresolved)}")
            logs.append(f"validation_resolver_base path={phase_compiled_path.parent}")
            validation_stdout, validation_stderr = _run_saxon_xslt(
                xml_input_path=cobie_xml_path,
                stylesheet_path=phase_compiled_path,
                output_path=phase_svrl_path,
                params={},
                logs=logs,
            )
            phase_svrl_paths.append(phase_svrl_path)
            logs.append(self._phase_log("validation_xslt_applied", phase_compiled_path))
            logs.append(self._phase_log("svrl_generated", phase_svrl_path))
            if validation_stdout:
                logs.append(f"saxon_validation_stdout phase={phase_id or '(default)'} stdout={validation_stdout.strip()}")
            if validation_stderr:
                logs.append(f"saxon_validation_stderr phase={phase_id or '(default)'} stderr={validation_stderr.strip()}")

        if phase_svrl_paths:
            base_root = ET.parse(str(phase_svrl_paths[0])).getroot()
            for extra in phase_svrl_paths[1:]:
                extra_root = ET.parse(str(extra)).getroot()
                for child in list(extra_root):
                    base_root.append(child)
            svrl_output_path.write_bytes(ET.tostring(base_root, encoding="utf-8", xml_declaration=True))
            if phase_compiled_paths:
                shutil.copy2(phase_compiled_paths[-1], compiled_xslt_output_path)
        logs.append(self._phase_log("svrl_generated", svrl_output_path))

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


def _resolve_required_jar_path(env_name: str) -> Path:
    configured = os.getenv(env_name, "").strip()
    if not configured:
        raise RuntimeError(f"Saxon configuration error: environment variable '{env_name}' is required but was not set.")
    jar_path = Path(configured).expanduser().resolve()
    if not jar_path.exists() or not jar_path.is_file():
        raise RuntimeError(
            f"Saxon configuration error: {env_name} points to a missing or non-file path: {jar_path}"
        )
    return jar_path


def _resolve_saxon_runtime_jars() -> Tuple[Path, Path, Path]:
    saxon_jar = _resolve_required_jar_path("COBIEQC_SAXON_JAR_PATH")
    xmlresolver_jar = _resolve_required_jar_path("COBIEQC_SAXON_XMLRESOLVER_JAR_PATH")
    xmlresolver_data_jar = _resolve_required_jar_path("COBIEQC_SAXON_XMLRESOLVER_DATA_JAR_PATH")
    return saxon_jar, xmlresolver_jar, xmlresolver_data_jar


def _resolve_saxon_command() -> List[str]:
    configured = os.getenv("COBIEQC_SAXON_CMD", "").strip()
    if configured:
        return shlex.split(configured)
    java_bin = os.getenv("JAVA_BIN", "java").strip() or "java"
    saxon_jar, xmlresolver_jar, xmlresolver_data_jar = _resolve_saxon_runtime_jars()
    classpath = os.pathsep.join([str(saxon_jar), str(xmlresolver_jar), str(xmlresolver_data_jar)])
    return [java_bin, "-cp", classpath, "net.sf.saxon.Transform"]


def _run_saxon_xslt(
    xml_input_path: Path,
    stylesheet_path: Path,
    output_path: Path,
    params: Dict[str, str],
    logs: List[str],
) -> Tuple[str, str]:
    logs.append("xslt_engine=saxon")
    logs.append("saxon_main_class=net.sf.saxon.Transform")
    configured_override = os.getenv("COBIEQC_SAXON_CMD", "").strip()
    if configured_override:
        command = [
            *shlex.split(configured_override),
            f"-s:{xml_input_path}",
            f"-xsl:{stylesheet_path}",
            f"-o:{output_path}",
        ]
        logs.append("saxon_command_source=override:COBIEQC_SAXON_CMD")
    else:
        saxon_jar, xmlresolver_jar, xmlresolver_data_jar = _resolve_saxon_runtime_jars()
        logs.append(f"saxon_jar_path={saxon_jar}")
        logs.append(f"saxon_jar_size_bytes={saxon_jar.stat().st_size}")
        logs.append(f"xmlresolver_jar_path={xmlresolver_jar}")
        logs.append(f"xmlresolver_jar_size_bytes={xmlresolver_jar.stat().st_size}")
        logs.append(f"xmlresolver_data_jar_path={xmlresolver_data_jar}")
        logs.append(f"xmlresolver_data_jar_size_bytes={xmlresolver_data_jar.stat().st_size}")
        classpath = os.pathsep.join([str(saxon_jar), str(xmlresolver_jar), str(xmlresolver_data_jar)])
        java_bin = os.getenv("JAVA_BIN", "java").strip() or "java"
        command = [
            java_bin,
            "-cp",
            classpath,
            "net.sf.saxon.Transform",
            f"-s:{xml_input_path}",
            f"-xsl:{stylesheet_path}",
            f"-o:{output_path}",
        ]
        logs.append(f"computed_classpath={classpath}")
        logs.append("saxon_command_source=classpath_env_vars")
    for key, value in params.items():
        command.append(f"{key}={value}")
    logs.append(f"saxon_argv={command}")
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
        cobie_diagnostics = _inspect_cobie_xml(cobie_xml_path)
        logs.append(
            "generated_cobie_xml_diagnostics "
            f"root_element={cobie_diagnostics['root_element_name']} "
            f"root_namespace={cobie_diagnostics['root_namespace']} "
            f"first_level_children={','.join(cobie_diagnostics['first_level_children']) or '(none)'}"
        )
        entity_counts = cobie_diagnostics["entity_counts"]
        logs.append(
            "generated_cobie_xml_entity_counts "
            + " ".join(f"{key}={entity_counts.get(key, 0)}" for key in COBIE_ENTITY_NAMES)
        )
        for entity_name in COBIE_ENTITY_NAMES:
            sample_paths = cobie_diagnostics["sample_paths"].get(entity_name, [])
            logs.append(
                f"generated_cobie_xml_sample_paths entity={entity_name} "
                f"paths={','.join(sample_paths) if sample_paths else '(none)'}"
            )
        sheet_row_counts = cobie_diagnostics["sheet_row_counts"]
        logs.append(
            "generated_cobie_xml_sheet_row_counts "
            + " ".join(f"{sheet}:{count}" for sheet, count in sorted(sheet_row_counts.items()))
        )
        logs.append(
            "generated_created_on_samples "
            f"values={','.join(cobie_diagnostics['created_on_samples']) or '(none)'}"
        )
        cross_ref = cobie_diagnostics["cross_reference"]
        logs.append(
            "generated_cross_reference_summary "
            f"contact_names={cross_ref['contact_name_count']} "
            f"created_by_total={cross_ref['created_by_total']} "
            f"created_by_unmatched={cross_ref['created_by_unmatched']}"
        )
        logs.append(
            "generated_cross_reference_unmatched_samples "
            f"samples={','.join(cross_ref['unmatched_samples']) or '(none)'}"
        )
        workbook_style = cobie_diagnostics["root_element_name"] == "COBieWorkbook" or any(
            child == "Sheet" for child in cobie_diagnostics["first_level_children"]
        )
        if workbook_style:
            raise RuntimeError(
                "Workbook XML shape is not compatible with COBie Schematron validation; "
                "expected COBie entity containers instead of COBieWorkbook/Sheet wrappers."
            )
        if cobie_diagnostics["root_element_name"] != "COBie":
            parse_warnings.append(
                "Generated COBie XML root element is not 'COBie'; rules may expect a COBie root container."
            )
        if cobie_diagnostics["root_namespace"] == "(none)":
            parse_warnings.append(
                "Generated COBie XML has no root namespace; rules may rely on COBie namespaces."
            )
        if cobie_diagnostics["model_path_count"] == 0:
            parse_warnings.append(
                "Generated COBie XML did not expose Contact/Facility/Floor/Space/Zone/Type/Component paths."
            )
        reference_xml_path = _find_reference_xml_path(out_dir)
        if reference_xml_path:
            comparison = _compare_xml_structure(cobie_xml_path, reference_xml_path)
            logs.append(
                "cobie_xml_comparison_summary "
                f"reference_path={comparison['reference_path']} "
                f"generated_root={comparison['generated_root']} reference_root={comparison['reference_root']} "
                f"generated_namespace={comparison['generated_namespace']} "
                f"reference_namespace={comparison['reference_namespace']}"
            )
            logs.append(
                "cobie_xml_comparison_namespace_map "
                f"generated={comparison['generated_namespaces']} "
                f"reference={comparison['reference_namespaces']}"
            )
            logs.append(
                "cobie_xml_comparison_paths "
                f"missing_count={len(comparison['missing_paths'])} extra_count={len(comparison['extra_paths'])}"
            )
            logs.append(
                "cobie_xml_comparison_elements "
                f"missing={','.join(comparison['missing_elements']) or '(none)'} "
                f"extra={','.join(comparison['extra_elements']) or '(none)'}"
            )
            logs.append(
                "cobie_xml_comparison_frequency_deltas "
                f"deltas={';'.join(comparison['frequency_deltas']) or '(none)'}"
            )
        else:
            logs.append(
                "cobie_xml_comparison_skipped reason=no_reference_xml "
                "hint=Set_COBIEQC_REFERENCE_XML_PATH_or_place_legacy_cobie.xml_in_job_dir"
            )

        logs.append("XML generated")
        validator = SchematronPipeline(resources_dir=resources_dir, stage=stage)
        summary, svrl_warnings, svrl_error, schematron_logs = validator.validate(
            cobie_xml_path,
            svrl_xml_path,
            compiled_validation_xslt_path,
        )
        logs.extend(schematron_logs)

        warnings = [*parse_warnings, *svrl_warnings]
        svrl_diagnostics = _inspect_svrl(svrl_xml_path)
        logs.append(
            "svrl_diagnostics "
            f"fired_rules={svrl_diagnostics['fired_rules']} "
            f"failed_asserts={svrl_diagnostics['failed_asserts']} "
            f"successful_reports={svrl_diagnostics['successful_reports']} "
            f"rules_matched={svrl_diagnostics['rules_matched']} "
            f"rules_evaluated={svrl_diagnostics['rules_evaluated']} "
            f"active_patterns={svrl_diagnostics['active_patterns']} "
            f"active_pattern_ids={','.join(svrl_diagnostics['active_pattern_ids']) or '(none)'}"
        )
        logs.append(
            "svrl_rule_failure_top10 "
            + ",".join(
                f"{item['rule']}:{item['count']}" for item in svrl_diagnostics["top_failing_rules"]
            )
        )
        logs.append(
            "svrl_failed_assert_count_by_rule "
            + ",".join(
                f"{rule}:{count}" for rule, count in sorted(svrl_diagnostics["failed_assert_count_by_rule"].items())
            )
        )
        reference_svrl_path = _find_reference_svrl_path(out_dir)
        if reference_svrl_path:
            svrl_comparison = _compare_svrl_outputs(svrl_xml_path, reference_svrl_path)
            logs.append(
                "svrl_comparison_summary "
                f"reference_svrl_path={svrl_comparison['reference_svrl_path']} "
                f"generated_failed_asserts={svrl_comparison['generated_failed_asserts']} "
                f"reference_failed_asserts={svrl_comparison['reference_failed_asserts']}"
            )
            logs.append(
                "svrl_comparison_rules "
                f"missing_rules={','.join(svrl_comparison['missing_rules']) or '(none)'} "
                f"extra_rules={','.join(svrl_comparison['extra_rules']) or '(none)'}"
            )
            logs.append(
                "svrl_comparison_count_deltas "
                f"deltas={';'.join(svrl_comparison['count_deltas']) or '(none)'}"
            )
            logs.append(
                "svrl_comparison_row_deltas "
                f"deltas={';'.join(svrl_comparison['row_deltas']) or '(none)'}"
            )
            summary["svrl_comparison"] = svrl_comparison
        else:
            logs.append(
                "svrl_comparison_skipped reason=no_reference_svrl "
                "hint=Set_COBIEQC_REFERENCE_SVRL_PATH_or_place_legacy_validation_result.svrl.xml_in_job_dir"
            )
        svrl_size_bytes = svrl_xml_path.stat().st_size if svrl_xml_path.exists() else 0
        non_trivial_workbook = sum(cobie_diagnostics["sheet_row_counts"].values()) >= 10
        if svrl_diagnostics["fired_rules"] == 0 and (non_trivial_workbook or svrl_size_bytes < 2048):
            warnings.append(
                "SVRL diagnostics indicate zero fired rules for a non-trivial workbook or tiny output; "
                "rule contexts likely did not match the generated COBie XML structure."
            )
        summary["warnings"] = warnings
        summary["stage"] = stage
        summary["svrl_diagnostics"] = svrl_diagnostics
        summary["cobie_xml_diagnostics"] = {
            "root_element_name": cobie_diagnostics["root_element_name"],
            "root_namespace": cobie_diagnostics["root_namespace"],
            "entity_counts": cobie_diagnostics["entity_counts"],
        }

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
