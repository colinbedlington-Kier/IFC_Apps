import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from field_access import FieldDescriptor, FieldKind


CHECK_SHEET_NAME = "09-IFC-SPF Model Checking Reqs"
CHECK_ID_RE = re.compile(r"^\d{2}\.\d{2}$")
DEFAULT_SOURCE_PATH = Path("/mnt/data/Book1.xlsx")
FALLBACK_SOURCE_PATH = Path("config/sample_model_checks.xlsx")
STATIC_SOURCE_PATH = Path("config/dfe_model_check_requirements.json")


@dataclass
class CheckDefinition:
    check_id: str
    description: str
    entity_scope: List[str]
    info_to_check: str
    applicable_models: Optional[str] = None
    milestones: List[str] = field(default_factory=list)
    field: Optional[FieldDescriptor] = None
    section: str = "General"
    mapping_status: str = "unmapped"


def _safe_path(path: Path) -> Optional[Path]:
    if path.exists():
        return path
    return None


def _resolve_source_path() -> Optional[Path]:
    target = _safe_path(DEFAULT_SOURCE_PATH)
    if target:
        return target
    fallback = _safe_path(FALLBACK_SOURCE_PATH)
    if fallback:
        return fallback
    return None


def _guess_milestones(df: pd.DataFrame, row_idx: int) -> List[str]:
    flags: List[str] = []
    for col in df.columns:
        if col.lower().startswith("stage") or "riba" in col.lower():
            val = str(df.at[row_idx, col]).strip()
            if val not in ("", "nan", "0", "False", "None"):
                flags.append(col)
    return flags


def _field_label(info_to_check: str) -> str:
    if not info_to_check:
        return ""
    if "(" in info_to_check:
        return info_to_check.split("(")[0].strip()
    return info_to_check.strip()


def infer_field(info_text: str, mapping_config: Dict[str, Dict]) -> FieldDescriptor:
    info_lower = (info_text or "").lower()
    base_label = _field_label(info_text)
    if "(attribute" in info_lower:
        return FieldDescriptor(kind=FieldKind.ATTRIBUTE, attribute_name=base_label or "Name")
    if "(property" in info_lower:
        # Use mapping defaults if present
        for entity_defaults in mapping_config.get("entity_defaults", {}).values():
            if base_label in entity_defaults:
                cfg = entity_defaults[base_label]
                if cfg.get("kind", "").lower() == "property":
                    return FieldDescriptor.from_mapping(cfg)
        return FieldDescriptor(kind=FieldKind.PROPERTY, property_name=base_label)
    if "ifcquantity" in info_lower or "(quantity" in info_lower:
        return FieldDescriptor(kind=FieldKind.QUANTITY, quantity_name=base_label, qto_name="BaseQuantities")
    if "(classification reference" in info_lower:
        sys_name = base_label or "Classification"
        return FieldDescriptor(kind=FieldKind.CLASSIFICATION, classification_system=sys_name)
    if "predefinedtype" in info_lower:
        return FieldDescriptor(kind=FieldKind.PREDEFINEDTYPE)
    return FieldDescriptor(kind=FieldKind.ATTRIBUTE, attribute_name=base_label or "Name")


def _section_for_entities(entities: List[str]) -> str:
    ents = [e.lower() for e in entities]
    if any("ifcproject" in e for e in ents):
        return "Project"
    if any("ifcsite" in e or "ifcbuilding" in e for e in ents):
        return "Site / Building"
    if any("ifcbuildingstorey" in e for e in ents):
        return "Storeys"
    if any("ifcspace" in e for e in ents):
        return "Spaces"
    if any("type" in e for e in ents):
        return "Object Types"
    if any("occurrence" in e for e in ents):
        return "Object Occurrences"
    return "Object Occurrences"


def _apply_mapping(check: CheckDefinition, mapping_config: Dict[str, Dict], expressions: Dict[str, str]) -> CheckDefinition:
    by_id = mapping_config.get("by_check_id", {})
    entity_defaults = mapping_config.get("entity_defaults", {})
    if check.check_id in by_id:
        check.field = FieldDescriptor.from_mapping(by_id[check.check_id])
    else:
        label = _field_label(check.info_to_check)
        for ent in check.entity_scope:
            defaults = entity_defaults.get(ent, {})
            if label in defaults:
                check.field = FieldDescriptor.from_mapping(defaults[label])
                break
    if not check.field:
        check.field = infer_field(check.info_to_check, mapping_config)
        check.mapping_status = "inferred"
    else:
        check.mapping_status = "mapped"

    expr = expressions.get(check.check_id) or expressions.get(getattr(check.field, "classification_system", ""), None)
    if expr and check.field:
        check.field.expression = expr
    return check


def _parse_rows(
    rows: List[Dict[str, str]], mapping_config: Dict[str, Dict], expressions: Dict[str, str]
) -> List[CheckDefinition]:
    definitions: List[CheckDefinition] = []
    for row in rows:
        check_id = str(row.get("check_id", "")).strip()
        if not CHECK_ID_RE.match(check_id):
            continue
        entities_raw = row.get("entity_scope") or ["IfcProduct"]
        if isinstance(entities_raw, str):
            entities = [e.strip() for e in entities_raw.split("/") if e.strip()]
        else:
            entities = [str(e).strip() for e in entities_raw if str(e).strip()]
        if not entities:
            entities = ["IfcProduct"]
        milestones = row.get("milestones") or []
        chk = CheckDefinition(
            check_id=check_id,
            description=str(row.get("description", "")).strip(),
            entity_scope=entities,
            info_to_check=str(row.get("info_to_check", "")).strip(),
            applicable_models=(str(row.get("applicable_models", "")).strip() or None),
            milestones=milestones,
            section=_section_for_entities(entities),
        )
        chk = _apply_mapping(chk, mapping_config, expressions)
        definitions.append(chk)
    return definitions


def _load_static_rows() -> List[Dict[str, str]]:
    if STATIC_SOURCE_PATH.exists():
        with open(STATIC_SOURCE_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return []
    return []


def _load_excel_rows() -> List[Dict[str, str]]:
    src = _resolve_source_path()
    if not src:
        return []
    df = pd.read_excel(src, sheet_name=CHECK_SHEET_NAME)
    # Some spreadsheets use the first row as labels rather than column headers
    if "DfE Ref." not in df.columns and len(df) > 0:
        header = df.iloc[0]
        df = df.iloc[1:].copy()
        df.columns = header
    rows: List[Dict[str, str]] = []
    for idx, row in df.iterrows():
        check_id = str(row.get("DfE Ref.", "")).strip()
        if not CHECK_ID_RE.match(check_id):
            continue
        entities = [e.strip() for e in str(row.get("IFC Entity", "")).split("/") if e.strip()]
        rows.append(
            {
                "check_id": check_id,
                "description": str(row.get("Description", "")).strip(),
                "entity_scope": entities,
                "info_to_check": str(row.get("Information To Be Checked", "")).strip(),
                "applicable_models": str(row.get("Applicable Models", "")).strip() or None,
                "milestones": _guess_milestones(df, idx),
            }
        )
    return rows


def load_check_definitions(mapping_config: Dict[str, Dict], expressions: Dict[str, str]) -> List[CheckDefinition]:
    rows = _load_static_rows()
    if not rows:
        rows = _load_excel_rows()
    # No data available; return empty list so the rest of the app keeps running.
    if not rows:
        return []
    return _parse_rows(rows, mapping_config, expressions)


def summarize_sections(definitions: List[CheckDefinition]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for d in definitions:
        summary[d.section] = summary.get(d.section, 0) + 1
    return summary
