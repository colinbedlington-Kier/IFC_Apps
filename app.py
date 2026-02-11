import argparse
import configparser
import csv
import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element
import ifcopenshell.util.placement
import pandas as pd
from fastapi import BackgroundTasks, Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ifcopenshell.guid import new as new_guid

from check_definitions_loader import load_check_definitions, summarize_sections
from expression_engine import ExpressionEngine
from field_access import FieldDescriptor, FieldKind, get_value, set_value
from mapping_store import (
    EXPRESSIONS_PATH,
    MAPPINGS_PATH,
    load_expression_config,
    load_mapping_config,
    save_expression_for_check,
    save_mapping_for_check,
)
from validation import validate_value
from cobieqc_service.jobs import (
    STATUS_DONE,
    STATUS_ERROR,
    STATUS_RUNNING,
    CobieQcJobStore,
)
from cobieqc_service.runner import run_cobieqc
from cobieqc_service.security import sanitize_filename as sanitize_upload_filename
from cobieqc_service.security import validate_upload

STEP2IFC_ROOT = Path(__file__).resolve().parent / "step2ifc"
if STEP2IFC_ROOT.exists():
    sys.path.append(str(STEP2IFC_ROOT))

STEP2IFC_AVAILABLE = False
STEP2IFC_IMPORT_ERROR = None
STEP2IFC_RUN_CONVERT = None
try:
    from step2ifc.auto import auto_convert
    from step2ifc.cli import run_convert as step2ifc_run_convert
    STEP2IFC_AVAILABLE = True
    STEP2IFC_RUN_CONVERT = step2ifc_run_convert
except Exception as exc:  # pragma: no cover - runtime dependency checks
    STEP2IFC_IMPORT_ERROR = str(exc)

APP_LOGGER = logging.getLogger("ifc_app")
RESOURCE_DIR = Path(__file__).resolve().parent / "resources"
QA_RESOURCE_DIR = Path(__file__).resolve().parent / "app" / "resources" / "ifc_qa"


# ----------------------------------------------------------------------------
# Session handling
# ----------------------------------------------------------------------------

def sanitize_filename(base: str) -> str:
    for c in '<>:"/\\|?*':
        base = base.replace(c, "_")
    return base


def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


class SessionStore:
    def __init__(self, base_dir: str, ttl_hours: int = 6) -> None:
        self.base_dir = base_dir
        self.ttl_hours = ttl_hours
        os.makedirs(self.base_dir, exist_ok=True)
        self.sessions: Dict[str, datetime.datetime] = {}

    def create(self) -> str:
        session_id = uuid.uuid4().hex
        os.makedirs(self.session_path(session_id), exist_ok=True)
        now = datetime.datetime.utcnow()
        self.sessions[session_id] = now
        return session_id

    def session_path(self, session_id: str) -> str:
        return os.path.join(self.base_dir, session_id)

    def touch(self, session_id: str) -> None:
        if not self.exists(session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        self.sessions[session_id] = datetime.datetime.utcnow()

    def exists(self, session_id: str) -> bool:
        return session_id in self.sessions and os.path.isdir(self.session_path(session_id))

    def cleanup_stale(self) -> None:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=self.ttl_hours)
        stale = [sid for sid, ts in self.sessions.items() if ts < cutoff]
        for sid in stale:
            self.drop(sid)
        # Remove stray dirs without bookkeeping
        for entry in os.listdir(self.base_dir):
            path = os.path.join(self.base_dir, entry)
            if os.path.isdir(path) and entry not in self.sessions:
                shutil.rmtree(path, ignore_errors=True)

    def drop(self, session_id: str) -> None:
        path = self.session_path(session_id)
        shutil.rmtree(path, ignore_errors=True)
        self.sessions.pop(session_id, None)

    def ensure(self, session_id: str) -> str:
        if not self.exists(session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        self.touch(session_id)
        return self.session_path(session_id)


SESSION_STORE = SessionStore(os.path.join(tempfile.gettempdir(), "ifc_app_sessions"))
STEP2IFC_JOBS: Dict[str, Dict[str, Any]] = {}
DATA_EXTRACT_JOBS: Dict[str, Dict[str, Any]] = {}
IFC_QA_JOBS: Dict[str, Dict[str, Any]] = {}
COBIEQC_JOB_STORE = CobieQcJobStore()


# ----------------------------------------------------------------------------
# Data extractor helpers
# ----------------------------------------------------------------------------

def load_default_config() -> Dict[str, str]:
    config_path = RESOURCE_DIR / "configfile.ini"
    parser = configparser.RawConfigParser()
    if not config_path.exists():
        return {
            "regex_ifc_name": "",
            "regex_ifc_type": "",
            "regex_ifc_system": "",
            "regex_ifc_layer": "",
            "regex_ifc_name_code": "",
            "regex_ifc_type_code": "",
            "regex_ifc_system_code": "",
        }
    parser.read(config_path)
    section = "RegularExpressions"
    return {
        "regex_ifc_name": parser.get(section, "regex_ifc_name", fallback=""),
        "regex_ifc_type": parser.get(section, "regex_ifc_type", fallback=""),
        "regex_ifc_system": parser.get(section, "regex_ifc_system", fallback=""),
        "regex_ifc_layer": parser.get(section, "regex_ifc_layer", fallback=""),
        "regex_ifc_name_code": parser.get(section, "regex_ifc_name_code", fallback=""),
        "regex_ifc_type_code": parser.get(section, "regex_ifc_type_code", fallback=""),
        "regex_ifc_system_code": parser.get(section, "regex_ifc_system_code", fallback=""),
    }


def _clean_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(_clean_value(v) for v in value if v is not None)
    return str(value)


def _line_ref(entity: Any) -> str:
    if entity is None:
        return ""
    return f"#{entity.id()}="


def _regex_check(pattern: str, value: str) -> str:
    if not pattern:
        return ""
    try:
        return "True" if re.search(pattern, value or "") else "False"
    except re.error:
        return "False"


def _regex_extract(pattern: str, value: str) -> str:
    if not pattern:
        return ""
    try:
        match = re.search(pattern, value or "")
    except re.error:
        return ""
    if not match:
        return ""
    if match.groups():
        return match.group(1)
    return match.group(0)


def _get_layers_name(entity: Any) -> str:
    try:
        layers = ifcopenshell.util.element.get_layers(entity) or []
    except Exception:
        return ""
    for layer in layers:
        name = getattr(layer, "Name", "") or ""
        if name:
            return name
    return ""


def _get_object_xyz(entity: Any) -> str:
    placement = getattr(entity, "ObjectPlacement", None)
    if not placement:
        return ""
    try:
        matrix = ifcopenshell.util.placement.get_local_placement(placement)
    except Exception:
        return ""
    if matrix is None or len(matrix) < 3:
        return ""
    try:
        x = float(matrix[0][3])
        y = float(matrix[1][3])
        z = float(matrix[2][3])
    except Exception:
        return ""
    return f"{x:.3f},{y:.3f},{z:.3f}"


def _read_csv_first_column(path: Path) -> List[str]:
    values: List[str] = []
    if not path.exists():
        return values
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            value = row[0].strip()
            if not value:
                continue
            if value.lower().startswith("exclude"):
                continue
            values.append(value)
    return values


def _load_pset_template(path: Path) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    if not path.exists():
        return mapping
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            entity_type = (row.get("IFC_Entity_Occurrence_Type") or "").strip()
            psets_raw = (row.get("Pset_Dictionaries") or "").strip()
            if not entity_type:
                continue
            psets: List[str] = []
            if psets_raw:
                sep = ";" if ";" in psets_raw else ","
                psets = [p.strip() for p in psets_raw.split(sep) if p.strip()]
            mapping[entity_type] = psets
    return mapping


def _iter_object_elements(model: ifcopenshell.file) -> List[Any]:
    objects: List[Any] = []
    spatial_types = {"IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey", "IfcSpace"}
    for obj in model.by_type("IfcProduct"):
        if obj.is_a() in spatial_types:
            continue
        objects.append(obj)
    return objects


def _safe_get_psets(entity: Any) -> Dict[str, Dict[str, Any]]:
    try:
        return ifcopenshell.util.element.get_psets(entity, psets_only=True, include_inherited=True) or {}
    except TypeError:
        return ifcopenshell.util.element.get_psets(entity, psets_only=True) or {}


def _qa_override_dir(session_id: str) -> Path:
    root = Path(SESSION_STORE.ensure(session_id))
    override_dir = root / "ifc_qa_overrides"
    override_dir.mkdir(parents=True, exist_ok=True)
    return override_dir


def _qa_default_paths() -> Dict[str, Path]:
    return {
        "qa_rules": QA_RESOURCE_DIR / "qa_rules.template.csv",
        "qa_property_requirements": QA_RESOURCE_DIR / "qa_property_requirements.template.csv",
        "qa_unacceptable_values": QA_RESOURCE_DIR / "qa_unacceptable_values.template.csv",
        "regex_patterns": QA_RESOURCE_DIR / "regex_patterns.template.csv",
        "exclude_filter": QA_RESOURCE_DIR / "exclude_filter.template.csv",
        "pset_template": QA_RESOURCE_DIR / "pset_template.template.csv",
    }


def _qa_config_path(session_id: Optional[str], key: str, override_path: Optional[Path] = None) -> Path:
    defaults = _qa_default_paths()
    if override_path and override_path.exists():
        return override_path
    if session_id:
        candidate = _qa_override_dir(session_id) / f"{key}.csv"
        if candidate.exists():
            return candidate
    return defaults[key]


def _load_regex_patterns(path: Path) -> Dict[str, Dict[str, str]]:
    patterns: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return patterns
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (row.get("key") or "").strip()
            if not key:
                continue
            patterns[key] = {
                "pattern": row.get("pattern", ""),
                "enabled": (row.get("enabled", "true") or "true").strip().lower(),
            }
    return patterns


# ----------------------------------------------------------------------------
# IFC cleaner (from original app.py)
# ----------------------------------------------------------------------------

def clean_ifc_file(
    in_path: str,
    out_path: str,
    prefix: str = "InfoDrainage",
    case_insensitive: bool = True,
    delete_psets_with_prefix: bool = True,
    delete_properties_in_other_psets: bool = True,
    drop_empty_psets: bool = True,
    also_remove_loose_props: bool = True,
) -> Dict[str, Any]:
    def starts_with(s: str) -> bool:
        if s is None:
            return False
        if case_insensitive:
            return s.lower().startswith(prefix.lower())
        return s.startswith(prefix)

    report = {
        "input": os.path.basename(in_path),
        "output": os.path.basename(out_path),
        "prefix": prefix,
        "case_insensitive": case_insensitive,
        "removed": {
            "IfcPropertySet": 0,
            "IfcRelDefinesByProperties": 0,
            "emptied_psets": 0,
            "IfcPropertySingleValue": 0,
            "IfcComplexProperty": 0,
            "IfcPropertyEnumeratedValue": 0,
            "IfcPropertyReferenceValue": 0,
            "IfcPropertyListValue": 0,
            "IfcPropertyTableValue": 0,
            "loose_properties": 0,
        },
        "notes": [],
        "status": "started",
    }

    f = ifcopenshell.open(in_path)

    psets_to_delete = set()
    if delete_psets_with_prefix:
        for pset in f.by_type("IfcPropertySet"):
            try:
                if starts_with(getattr(pset, "Name", None)):
                    psets_to_delete.add(pset)
            except Exception:
                pass

    prop_types = [
        "IfcPropertySingleValue",
        "IfcComplexProperty",
        "IfcPropertyEnumeratedValue",
        "IfcPropertyReferenceValue",
        "IfcPropertyListValue",
        "IfcPropertyTableValue",
    ]
    prop_removed_count = {t: 0 for t in prop_types}
    emptied_pset_count = 0

    if delete_properties_in_other_psets:
        for pset in f.by_type("IfcPropertySet"):
            try:
                if pset in psets_to_delete:
                    continue

                props = list(getattr(pset, "HasProperties", []) or [])
                if not props:
                    continue

                to_keep = []
                for p in props:
                    nm = getattr(p, "Name", None)
                    if nm and starts_with(nm):
                        try:
                            kind = p.is_a()
                        except Exception:
                            kind = None
                        try:
                            f.remove(p)
                            if kind in prop_removed_count:
                                prop_removed_count[kind] += 1
                        except Exception:
                            if kind in prop_removed_count:
                                prop_removed_count[kind] += 1
                    else:
                        to_keep.append(p)

                try:
                    pset.HasProperties = tuple(to_keep)
                except Exception:
                    pass

                if drop_empty_psets and len(getattr(pset, "HasProperties", []) or []) == 0:
                    psets_to_delete.add(pset)
                    emptied_pset_count += 1
            except Exception:
                pass

    rel_del_count = 0
    pset_del_count = 0

    for pset in list(psets_to_delete):
        inverses = []
        try:
            inverses = f.get_inverse(pset)
        except Exception:
            pass

        for rel in list(inverses or []):
            try:
                if rel.is_a("IfcRelDefinesByProperties") and rel.RelatingPropertyDefinition == pset:
                    f.remove(rel)
                    rel_del_count += 1
            except Exception:
                pass

        try:
            f.remove(pset)
            pset_del_count += 1
        except Exception:
            pass

    loose_removed = 0
    if also_remove_loose_props:
        for t in prop_types:
            for p in list(f.by_type(t)):
                nm = getattr(p, "Name", None)
                if nm and starts_with(nm):
                    try:
                        f.remove(p)
                        prop_removed_count[t] += 1
                        loose_removed += 1
                    except Exception:
                        pass

    try:
        f.write(out_path)
        status = "success"
    except Exception as e:
        status = "save_failed"
        report["notes"].append(f"Save error: {e!r}")

    report["removed"]["IfcPropertySet"] = pset_del_count
    report["removed"]["IfcRelDefinesByProperties"] = rel_del_count
    report["removed"]["emptied_psets"] = emptied_pset_count
    report["removed"]["loose_properties"] = loose_removed
    for t, c in prop_removed_count.items():
        report["removed"][t] = c

    report["status"] = status
    return report


# ----------------------------------------------------------------------------
# Excel extractor/updater (from app (1).py)
# ----------------------------------------------------------------------------

COBIE_MAPPING = {
    "COBie_Specification": {"scope": "T", "props": [
        ("NominalLength", "Length"),
        ("NominalWidth", "Length"),
        ("NominalHeight", "Length"),
        ("Shape", "Text"),
        ("Size", "Text"),
        ("Color", "Text"),
        ("Finish", "Text"),
        ("Grade", "Text"),
        ("Material", "Text"),
        ("Constituents", "Text"),
        ("Features", "Text"),
        ("AccessibilityPerformance", "Text"),
        ("CodePerformance", "Text"),
        ("SustainabilityPerformance", "Text"),
    ]},
    "COBie_Component": {"scope": "I", "props": [
        ("COBie", "Boolean"),
        ("InstallationDate", "Text"),
        ("WarrantyStartDate", "Text"),
        ("TagNumber", "Text"),
        ("AssetIdentifier", "Text"),
        ("Space", "Text"),
        ("CreatedBy", "Text"),
        ("CreatedOn", "Text"),
        ("Name", "Text"),
        ("Description", "Text"),
        ("Area", "Area"),
        ("Length", "Length"),
    ]},
    "COBie_Asset": {"scope": "T", "props": [
        ("AssetType", "Text"),
    ]},
    "COBie_Warranty": {"scope": "T", "props": [
        ("WarrantyDurationParts", "Real"),
        ("WarrantyGuarantorLabor", "Text"),
        ("WarrantyDurationLabor", "Real"),
        ("WarrantyDurationDescription", "Text"),
        ("WarrantyDurationUnit", "Text"),
        ("WarrantyGuarantorParts", "Text"),
    ]},
    "Pset_ManufacturerOccurence": {"scope": "I", "props": [
        ("SerialNumber", "Text"),
        ("BarCode", "Text"),
    ]},
    "COBie_ServiceLife": {"scope": "T", "props": [
        ("ServiceLifeDuration", "Real"),
        ("DurationUnit", "Text"),
    ]},
    "COBie_EconomicalImpactValues": {"scope": "T", "props": [
        ("ReplacementCost", "Real"),
    ]},
    "COBie_Type": {"scope": "T", "props": [
        ("COBie", "Boolean"),
        ("CreatedBy", "Text"),
        ("CreatedOn", "Text"),
        ("Name", "Text"),
        ("Description", "Text"),
        ("Category", "Text"),
        ("Area", "Area"),
        ("Length", "Length"),
    ]},
    "COBie_System": {"scope": "I", "props": [
        ("Name", "Text"),
        ("Description", "Text"),
        ("Category", "Text"),
    ]},
    "Classification_General": {"scope": "T", "props": [
        ("Classification.Uniclass.Pr.Number", "Text"),
        ("Classification.Uniclass.Pr.Description", "Text"),
        ("Classification.Uniclass.Ss.Number", "Text"),
        ("Classification.Uniclass.Ss.Description", "Text"),
        ("Classification.NRM1.Number", "Text"),
        ("Classification.NRM1.Description", "Text"),
    ]},
    "Pset_ManufacturerTypeInformation": {"scope": "T", "props": [
        ("Manufacturer", "Text"),
        ("ModelNumber", "Text"),
        ("ModelReference", "Text"),
    ]},
    "PPset_DoorCommon": {"scope": "T", "props": [
        ("FireRating", "Text"),
    ]},
    "Pset_BuildingCommon": {"scope": "T", "props": [
        ("NumberOfStoreys", "Text"),
    ]},
    "COBie_Space": {"scope": "T", "props": [
        ("RoomTag", "Text"),
    ]},
    "COBie_BuildingCommon_UK": {"scope": "T", "props": [
        ("UPRN", "Text"),
    ]},
    "Additional_Pset_BuildingCommon": {"scope": "T", "props": [
        ("BlockConstructionType", "Text"),
        ("MaximumBlockHeight", "Text"),
    ]},
    "Additional_Pset_SystemCommon": {"scope": "T", "props": [
        ("SystemCategory", "Text"),
        ("SystemDescription", "Text"),
        ("SystemName", "Text"),
    ]},
}


RE_SPLIT_LIST = re.compile(r"[;,|\n]+|\s{2,}")


def path_of(f):
    return f if isinstance(f, str) else getattr(f, "name", f)


def clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
    return v


def ensure_aggregates(parent, child, ifc):
    rel = None
    for r in parent.IsDecomposedBy or []:
        if r.is_a("IfcRelAggregates"):
            rel = r
            break
    if rel is None:
        ifc.create_entity(
            "IfcRelAggregates",
            GlobalId=new_guid(),
            RelatingObject=parent,
            RelatedObjects=[child],
        )
    else:
        if child not in rel.RelatedObjects:
            rel.RelatedObjects = list(rel.RelatedObjects) + [child]


def reassign_aggregate(parent, child, ifc):
    for rel in list(child.Decomposes or []):
        if not rel.is_a("IfcRelAggregates"):
            continue
        if rel.RelatingObject == parent:
            continue
        related = list(rel.RelatedObjects)
        if child in related:
            related.remove(child)
            if related:
                rel.RelatedObjects = related
            else:
                ifc.remove(rel)
    ensure_aggregates(parent, child, ifc)


def parse_required_pairs(raw):
    if not raw or not isinstance(raw, str):
        return []
    items = [t.strip() for t in RE_SPLIT_LIST.split(raw) if t.strip()]
    pairs = []
    for token in items:
        if "." in token:
            pset, prop = token.split(".", 1)
            pset, prop = pset.strip(), prop.strip()
            if pset and prop:
                pairs.append((pset, prop))
    return pairs


def get_pset_value(elem, pset_name, prop_name):
    psets = ifcopenshell.util.element.get_psets(elem)
    if pset_name in psets and prop_name in psets[pset_name]:
        return psets[pset_name][prop_name]

    type_obj = None
    for rel in elem.IsDefinedBy or []:
        if rel.is_a("IfcRelDefinesByType"):
            type_obj = rel.RelatingType
            break
    if type_obj is not None:
        type_psets = ifcopenshell.util.element.get_psets(type_obj)
        if pset_name in type_psets and prop_name in type_psets[pset_name]:
            return type_psets[pset_name][prop_name]
    return ""


def extract_to_excel(ifc_path: str, output_path: str) -> str:
    ifc = ifcopenshell.open(ifc_path)

    project_data = []
    project = ifc.by_type("IfcProject")[0]
    site = ifc.by_type("IfcSite")[0] if ifc.by_type("IfcSite") else None
    building = ifc.by_type("IfcBuilding")[0] if ifc.by_type("IfcBuilding") else None

    project_data.append({
        "DataType": "Project",
        "Name": getattr(project, "Name", ""),
        "Description": getattr(project, "Description", ""),
        "Phase": getattr(project, "Phase", ""),
    })
    if site:
        project_data.append({
            "DataType": "Site",
            "Name": getattr(site, "Name", ""),
            "Description": getattr(site, "Description", ""),
            "Phase": "",
        })
    else:
        project_data.append({"DataType": "Site", "Name": "", "Description": "", "Phase": ""})
    if building:
        project_data.append({
            "DataType": "Building",
            "Name": getattr(building, "Name", ""),
            "Description": getattr(building, "Description", ""),
            "Phase": "",
        })
    project_df = pd.DataFrame(project_data)

    element_data = []
    for elem in ifc.by_type("IfcElement"):
        elem_name = getattr(elem, "Name", "")
        elem_type = getattr(elem, "ObjectType", "")
        elem_desc = getattr(elem, "Description", "")
        type_obj = None
        for rel in ifc.get_inverse(elem):
            if rel.is_a("IfcRelDefinesByType"):
                type_obj = rel.RelatingType
        type_name = type_obj.Name if type_obj else ""
        element_data.append([
            elem.GlobalId,
            elem.is_a(),
            elem_name,
            elem_type,
            type_name,
            elem_desc
        ])
    elements_df = pd.DataFrame(
        element_data,
        columns=["GlobalId", "Class", "OccurrenceName", "OccurrenceType", "TypeName", "TypeDescription"]
    )

    prop_data = []
    for elem in ifc.by_type("IfcElement"):
        for definition in elem.IsDefinedBy or []:
            if definition.is_a("IfcRelDefinesByProperties"):
                pset = definition.RelatingPropertyDefinition
                if pset.is_a("IfcPropertySet"):
                    for prop in pset.HasProperties:
                        val = None
                        if prop.is_a("IfcPropertySingleValue"):
                            if prop.NominalValue:
                                val = prop.NominalValue.wrappedValue
                        elif prop.is_a("IfcPropertyEnumeratedValue"):
                            if prop.EnumerationValues:
                                val = ", ".join(str(v.wrappedValue) for v in prop.EnumerationValues)
                        prop_data.append([
                            elem.GlobalId,
                            elem.is_a(),
                            pset.Name,
                            prop.Name,
                            val,
                        ])
    props_df = pd.DataFrame(prop_data, columns=["GlobalId", "Class", "PropertySet", "Property", "Value"])

    cobie_cols = ["GlobalId", "IFCElement.Name", "IFCElementType.Name"]

    dynamic_pairs = set()
    for elem in ifc.by_type("IfcElement"):
        psets_elem = ifcopenshell.util.element.get_psets(elem)
        add_pset = psets_elem.get("Additional_Pset_GeneralCommon", {})
        dynamic_pairs.update(parse_required_pairs(add_pset.get("RequiredForCOBie", "")))
        dynamic_pairs.update(parse_required_pairs(add_pset.get("RequiredForCOBieComponent", "")))

        type_obj = None
        for rel in elem.IsDefinedBy or []:
            if rel.is_a("IfcRelDefinesByType"):
                type_obj = rel.RelatingType
                break
        if type_obj is not None:
            psets_type = ifcopenshell.util.element.get_psets(type_obj)
            add_pset_t = psets_type.get("Additional_Pset_GeneralCommon", {})
            dynamic_pairs.update(parse_required_pairs(add_pset_t.get("RequiredForCOBie", "")))
            dynamic_pairs.update(parse_required_pairs(add_pset_t.get("RequiredForCOBieComponent", "")))

    mapping_pairs = []
    if COBIE_MAPPING:
        for pset, info in COBIE_MAPPING.items():
            for pname, _ in info["props"]:
                mapping_pairs.append((pset, pname))

    all_pairs = mapping_pairs + sorted(dynamic_pairs - set(mapping_pairs))
    for pset, pname in all_pairs:
        cobie_cols.append(f"{pset}.{pname}")

    cobie_rows = []
    for elem in ifc.by_type("IfcElement"):
        type_name = ""
        for rel in ifc.get_inverse(elem):
            if rel.is_a("IfcRelDefinesByType"):
                if rel.RelatingType:
                    type_name = getattr(rel.RelatingType, "Name", "")
                    break

        row = {
            "GlobalId": elem.GlobalId,
            "IFCElement.Name": getattr(elem, "Name", ""),
            "IFCElementType.Name": type_name
        }

        for pset, pname in all_pairs:
            key = f"{pset}.{pname}"
            row[key] = get_pset_value(elem, pset, pname)

        cobie_rows.append(row)

    cobie_df = pd.DataFrame(cobie_rows, columns=cobie_cols)

    def extract_uniclass(elem, target_name, is_ifc2x3):
        reference = ""
        name = ""
        for rel in getattr(elem, "HasAssociations", []) or []:
            if rel.is_a("IfcRelAssociatesClassification"):
                classification_ref = rel.RelatingClassification
                if classification_ref and classification_ref.is_a("IfcClassificationReference"):
                    if is_ifc2x3:
                        if getattr(classification_ref, "Name", "") == target_name:
                            return getattr(classification_ref, "ItemReference", ""), getattr(classification_ref, "Name", "")
                    else:
                        src = getattr(classification_ref, "ReferencedSource", None)
                        if src and getattr(src, "Name", "") == target_name:
                            return getattr(classification_ref, "ItemReference", ""), getattr(classification_ref, "Name", "")
        return reference, name

    is_ifc2x3 = ifc.schema == "IFC2X3"
    pr_rows, ss_rows = [], []
    for elem in ifc.by_type("IfcElement"):
        pr_ref, pr_name = extract_uniclass(elem, "Uniclass Pr Products", is_ifc2x3)
        ss_ref, ss_name = extract_uniclass(elem, "Uniclass Ss Systems", is_ifc2x3)
        pr_rows.append({"GlobalId": elem.GlobalId, "Reference": pr_ref, "Name": pr_name})
        ss_rows.append({"GlobalId": elem.GlobalId, "Reference": ss_ref, "Name": ss_name})

    uniclass_pr_df = pd.DataFrame(pr_rows)
    uniclass_ss_df = pd.DataFrame(ss_rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        project_df.to_excel(writer, sheet_name="ProjectData", index=False)
        elements_df.to_excel(writer, sheet_name="Elements", index=False)
        props_df.to_excel(writer, sheet_name="Properties", index=False)
        cobie_df.to_excel(writer, sheet_name="COBieMapping", index=False)
        uniclass_pr_df.to_excel(writer, sheet_name="Uniclass_Pr", index=False)
        uniclass_ss_df.to_excel(writer, sheet_name="Uniclass_Ss", index=False)
    return output_path


def update_ifc_from_excel(ifc_file, excel_file, output_path: str, update_mode="update", add_new="no"):
    ifc_path = path_of(ifc_file)
    xls_path = path_of(excel_file)
    ifc = ifcopenshell.open(ifc_path)
    xls = pd.ExcelFile(xls_path)
    elements_df = pd.read_excel(xls, "Elements")
    props_df = pd.read_excel(xls, "Properties")
    cobie_df = pd.read_excel(xls, "COBieMapping")
    project_df = pd.read_excel(xls, "ProjectData")
    try:
        uniclass_pr_df = pd.read_excel(xls, "Uniclass_Pr")
    except Exception:
        uniclass_pr_df = None
    try:
        uniclass_ss_df = pd.read_excel(xls, "Uniclass_Ss")
    except Exception:
        uniclass_ss_df = None

    project = ifc.by_type("IfcProject")[0]
    site = ifc.by_type("IfcSite")[0] if ifc.by_type("IfcSite") else None
    building = ifc.by_type("IfcBuilding")[0] if ifc.by_type("IfcBuilding") else None

    for _, row in project_df.iterrows():
        dt = row["DataType"]
        if dt == "Project":
            if pd.notna(row.get("Name")):
                project.Name = clean_value(row["Name"]) or project.Name
            if pd.notna(row.get("Description")):
                project.Description = clean_value(row["Description"]) or project.Description
            if pd.notna(row.get("Phase")):
                project.Phase = clean_value(row["Phase"]) or project.Phase
        elif dt == "Site":
            name = clean_value(row.get("Name"))
            desc = clean_value(row.get("Description"))
            if site is None and (add_new == "yes" or name or desc):
                site = ifc.create_entity("IfcSite", GlobalId=new_guid(), Name=name or "Site")
            if site is not None:
                if name is not None:
                    site.Name = name
                if desc is not None:
                    site.Description = desc
                ensure_aggregates(project, site, ifc)
                if building is not None:
                    reassign_aggregate(site, building, ifc)
        elif dt == "Building":
            if building is None and add_new == "yes":
                building = ifc.create_entity(
                    "IfcBuilding",
                    GlobalId=new_guid(),
                    Name=clean_value(row.get("Name")) or "Building",
                )
            if building is not None:
                if pd.notna(row.get("Name")):
                    building.Name = clean_value(row["Name"]) or building.Name
                if pd.notna(row.get("Description")):
                    building.Description = clean_value(row["Description"]) or building.Description
                if site is not None:
                    reassign_aggregate(site, building, ifc)
                else:
                    ensure_aggregates(project, building, ifc)

    for _, row in elements_df.iterrows():
        elem = ifc.by_guid(row["GlobalId"]) if pd.notna(row.get("GlobalId")) else None
        if not elem:
            continue
        if pd.notna(row.get("OccurrenceName")):
            elem.Name = clean_value(row["OccurrenceName"]) or elem.Name
        if pd.notna(row.get("OccurrenceType")):
            elem.ObjectType = clean_value(row["OccurrenceType"]) or elem.ObjectType
        if pd.notna(row.get("TypeDescription")):
            elem.Description = clean_value(row["TypeDescription"]) or elem.Description
        if pd.notna(row.get("TypeName")):
            type_name = str(clean_value(row["TypeName"]))
            type_obj = None
            for rel in ifc.get_inverse(elem):
                if rel.is_a("IfcRelDefinesByType"):
                    type_obj = rel.RelatingType
            if not type_obj and add_new == "yes":
                type_class = (
                    elem.is_a() + "Type"
                    if elem.is_a().endswith("Element") or elem.is_a().endswith("Door")
                    else "IfcTypeObject"
                )
                try:
                    type_obj = ifcopenshell.api.run("type.create_type", ifc, ifc_class=type_class, name=type_name)
                except Exception:
                    type_obj = ifc.create_entity(
                        "IfcBuildingElementType",
                        GlobalId=new_guid(),
                        Name=type_name,
                    )
                ifcopenshell.api.run("type.assign_type", ifc, related_objects=[elem], relating_type=type_obj)
            elif type_obj:
                type_obj.Name = type_name

    if cobie_df is not None:
        mapping_keys = set()
        if COBIE_MAPPING is not None:
            for pset, info in COBIE_MAPPING.items():
                for pname, _ in info["props"]:
                    mapping_keys.add(f"{pset}.{pname}")

        candidate_cols = [
            c
            for c in cobie_df.columns
            if c not in ("GlobalId", "IFCElement.Name", "IFCElementType.Name") and "." in c
        ]

        for _, row in cobie_df.iterrows():
            guid = row.get("GlobalId")
            if pd.isna(guid):
                continue
            elem = ifc.by_guid(guid)
            if not elem:
                continue

            for col in candidate_cols:
                if pd.isna(row.get(col)):
                    continue
                val = row[col]
                pset, pname = col.split(".", 1)
                pset, pname = pset.strip(), pname.strip()

                psets = ifcopenshell.util.element.get_psets(elem)
                if pset not in psets and add_new == "no":
                    continue

                pset_entity = None
                for rel in elem.IsDefinedBy or []:
                    if (
                        rel.is_a("IfcRelDefinesByProperties")
                        and rel.RelatingPropertyDefinition
                        and getattr(rel.RelatingPropertyDefinition, "Name", "") == pset
                    ):
                        pset_entity = rel.RelatingPropertyDefinition
                        break
                if pset_entity is None and add_new == "yes":
                    pset_entity = ifcopenshell.api.run("pset.add_pset", ifc, product=elem, name=pset)

                if pset_entity:
                    try:
                        ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset_entity, properties={pname: val})
                    except Exception:
                        pass

    def set_uniclass(df, source_name):
        if df is None:
            return
        cls_src = None
        for c in ifc.by_type("IfcClassification"):
            if getattr(c, "Name", "") == source_name:
                cls_src = c
                break
        if cls_src is None and add_new == "yes":
            cls_src = ifc.create_entity(
                "IfcClassification",
                Name=source_name,
                Source="https://www.thenbs.com/our-tools/uniclass-2015",
                Edition="2015",
            )
        for _, r in df.iterrows():
            guid = r.get("GlobalId")
            if pd.isna(guid):
                continue
            elem = ifc.by_guid(guid)
            if not elem:
                continue
            ref = clean_value(r.get("Reference"))
            nm = clean_value(r.get("Name"))
            if ref is None and nm is None:
                continue
            existing_ref = None
            for rel in getattr(elem, "HasAssociations", []) or []:
                if rel.is_a("IfcRelAssociatesClassification"):
                    cref = rel.RelatingClassification
                    if cref and cref.is_a("IfcClassificationReference"):
                        src = getattr(cref, "ReferencedSource", None)
                        if src and getattr(src, "Name", "") == source_name:
                            existing_ref = cref
                            break
            if existing_ref:
                if ref is not None:
                    existing_ref.ItemReference = str(ref)
                if nm is not None:
                    existing_ref.Name = str(nm)
            elif add_new == "yes" and (ref is not None or nm is not None) and cls_src is not None:
                cref = ifc.create_entity(
                    "IfcClassificationReference",
                    ItemReference=str(ref) if ref is not None else None,
                    Name=str(nm) if nm is not None else None,
                )
                cref.ReferencedSource = cls_src
                ifc.create_entity(
                    "IfcRelAssociatesClassification",
                    GlobalId=new_guid(),
                    RelatedObjects=[elem],
                    RelatingClassification=cref,
                )

    set_uniclass(uniclass_pr_df, "Uniclass Pr Products")
    set_uniclass(uniclass_ss_df, "Uniclass Ss Systems")

    ifc.write(output_path)
    return output_path


# ----------------------------------------------------------------------------
# Storey / Global Z + BaseQuantities (from app (2).py)
# ----------------------------------------------------------------------------

_SI_PREFIX_TO_M = {None: 1.0, "MILLI": 1e-3, "CENTI": 1e-2, "DECI": 1e-1, "KILO": 1e3}


def model_length_unit_in_m(model) -> float:
    try:
        projs = model.by_type("IfcProject")
        if not projs:
            return 1.0
        units = projs[0].UnitsInContext
        if units and getattr(units, "Units", None):
            for u in units.Units:
                if u.is_a("IfcSIUnit") and getattr(u, "UnitType", None) == "LENGTHUNIT":
                    prefix = getattr(u, "Prefix", None)
                    factor = _SI_PREFIX_TO_M.get(prefix, 1.0)
                    return factor
    except Exception:
        pass
    return 1.0


def to_model_units_length(value, input_unit_code, model) -> float:
    if value in (None, ""):
        return None
    try:
        val = float(value)
    except Exception:
        return None
    if input_unit_code == "m":
        return val / model_length_unit_in_m(model)
    if input_unit_code == "mm":
        return (val * 0.001) / model_length_unit_in_m(model)
    return val


def ui_to_meters(value, units_code) -> float:
    if value in (None, ""):
        return 0.0
    val = float(value)
    return val if units_code == "m" else val / 1000.0


def meters_to_model_units(val_m, model) -> float:
    mu = model_length_unit_in_m(model)
    return val_m / mu if mu else val_m


def get_first_owner_history(model):
    oh = model.by_type("IfcOwnerHistory")
    return oh[0] if oh else None


def find_storeys(model):
    storeys = []
    for st in model.by_type("IfcBuildingStorey"):
        label = f"{st.Name or '(unnamed)'} — Elev: {getattr(st, 'Elevation', None)}"
        storeys.append((st.id(), label, st, getattr(st, "Elevation", None)))
    storeys.sort(key=lambda s: (s[3] is None, s[3]))
    return storeys


def get_existing_elq(model, storey):
    if not storey or not storey.IsDefinedBy:
        return None
    for rel in storey.IsDefinedBy:
        if rel.is_a("IfcRelDefinesByProperties"):
            pdef = rel.RelatingPropertyDefinition
            if pdef and pdef.is_a("IfcElementQuantity") and getattr(pdef, "Name", "") == "BaseQuantities":
                return pdef
    return None


def find_qtylength(elq, name):
    if not elq or not getattr(elq, "Quantities", None):
        return None
    for q in elq.Quantities:
        if q.is_a("IfcQuantityLength") and getattr(q, "Name", "") == name:
            return q
    return None


def ensure_qtylength(model, elq, name, value_model_units, description=None):
    ql = find_qtylength(elq, name)
    if ql:
        ql.LengthValue = float(value_model_units)
        if description is not None:
            ql.Description = description
        return ql
    ql = model.create_entity(
        "IfcQuantityLength",
        Name=name,
        LengthValue=float(value_model_units),
        Description=description,
        Unit=None,
    )
    if getattr(elq, "Quantities", None):
        elq.Quantities = list(elq.Quantities) + [ql]
    else:
        elq.Quantities = [ql]
    return ql


def create_or_update_storey_basequantities(
    model,
    storey,
    gross_val_ui=None,
    net_val_ui=None,
    input_unit_code="m",
    method_of_measurement=None,
    mirror_to_qto=False,
):
    owner_history = get_first_owner_history(model)
    elq = get_existing_elq(model, storey)
    if not elq:
        elq = model.create_entity(
            "IfcElementQuantity",
            GlobalId=new_guid(),
            OwnerHistory=owner_history,
            Name="BaseQuantities",
            MethodOfMeasurement=method_of_measurement,
        )
        rel = model.create_entity(
            "IfcRelDefinesByProperties",
            GlobalId=new_guid(),
            OwnerHistory=owner_history,
            Name="BaseQuantities",
            Description=None,
            RelatingPropertyDefinition=elq,
            RelatedObjects=[storey],
        )
        if getattr(storey, "IsDefinedBy", None):
            storey.IsDefinedBy = list(storey.IsDefinedBy) + [rel]
        else:
            storey.IsDefinedBy = [rel]
    else:
        if method_of_measurement is not None:
            elq.MethodOfMeasurement = method_of_measurement

    mu_factor = model_length_unit_in_m(model)
    if gross_val_ui is not None:
        gross_in_model = to_model_units_length(gross_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "GrossHeight", gross_in_model)
    if net_val_ui is not None:
        net_in_model = to_model_units_length(net_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "NetHeight", net_in_model)

    if mirror_to_qto:
        elq.Name = "Qto_BuildingStoreyBaseQuantities"


def ascend_to_root_local_placement(lp):
    cur = lp
    while cur and getattr(cur, "PlacementRelTo", None) is not None:
        cur = cur.PlacementRelTo
    return cur


def get_location_cartesian_point(lp):
    if lp is None:
        return None
    loc = getattr(lp, "Location", None)
    if loc and loc.is_a("IfcCartesianPoint"):
        return loc
    return None


def get_all_map_conversions(model):
    mcs = []
    for site in model.by_type("IfcSite"):
        if getattr(site, "RefLatitude", None) is not None:
            for rel in site.HasCoordinateOperation or []:
                if rel.is_a("IfcMapConversion"):
                    mcs.append(rel)
    return mcs


def countershift_product_local_points(model, delta_model):
    c = 0
    for prod in model.by_type("IfcProduct"):
        lp = getattr(prod, "ObjectPlacement", None)
        if lp and lp.is_a("IfcLocalPlacement"):
            loc = get_location_cartesian_point(lp.RelativePlacement)
            if loc is None:
                continue
            coords = list(loc.Coordinates)
            if len(coords) < 3:
                coords += [0.0] * (3 - len(coords))
            try:
                coords[2] = float(coords[2] or 0.0) - delta_model
                new_pt = model.create_entity(
                    "IfcCartesianPoint",
                    Coordinates=(
                        float(coords[0]) if coords[0] is not None else 0.0,
                        float(coords[1]) if coords[1] is not None else 0.0,
                        coords[2],
                    ),
                )
                lp.RelativePlacement.Location = new_pt
                c += 1
            except Exception:
                pass
    return c


def adjust_local_placement_z(lp, delta_model):
    if lp is None or not lp.is_a("IfcLocalPlacement"):
        return
    rel = getattr(lp, "RelativePlacement", None)
    if rel and rel.is_a("IfcAxis2Placement3D"):
        loc = getattr(rel, "Location", None)
        if loc and loc.is_a("IfcCartesianPoint"):
            coords = list(loc.Coordinates)
            if len(coords) < 3:
                coords += [0.0] * (3 - len(coords))
            coords[2] = float(coords[2] or 0.0) + delta_model
            loc.Coordinates = tuple(coords)


def parse_ifc_storeys(ifc_path: str) -> Dict[str, Any]:
    size = os.path.getsize(ifc_path)
    model = ifcopenshell.open(ifc_path)
    storeys = find_storeys(model)
    unit_m = model_length_unit_in_m(model)
    unit_label = "m" if abs(unit_m - 1.0) < 1e-12 else ("mm" if abs(unit_m - 1e-3) < 1e-12 else f"{unit_m} m/unit")
    mc_list = get_all_map_conversions(model)
    choices = [
        {
            "id": sid,
            "label": lbl,
        }
        for (sid, lbl, _ent, _elev) in storeys
    ]
    return {
        "storeys": choices,
        "summary": f"{len(choices)} storey level(s); model unit: {unit_label}; size {human_size(size)}",
        "map_conversions": len(mc_list),
        "unit_label": unit_label,
    }


def apply_storey_changes(
    ifc_path: str,
    storey_id: Optional[int],
    units_code: str,
    gross_val: Optional[float],
    net_val: Optional[float],
    mom_txt: Optional[str],
    mirror: bool,
    target_z: Optional[float],
    countershift_geometry: bool,
    use_crs_mode: bool,
    update_all_mcs: bool,
    show_diag: bool,
    crs_set_storey_elev: bool,
    output_path: str,
) -> Tuple[str, str]:
    model = ifcopenshell.open(ifc_path)
    storey = model.by_id(int(storey_id)) if storey_id else None
    if not storey:
        raise ValueError("Selected storey not found")

    gross_maybe = gross_val if gross_val not in (None, "") else None
    net_maybe = net_val if net_val not in (None, "") else None
    if gross_maybe is not None or net_maybe is not None:
        create_or_update_storey_basequantities(
            model,
            storey,
            gross_val_ui=gross_maybe,
            net_val_ui=net_maybe,
            input_unit_code=units_code,
            method_of_measurement=(mom_txt or None),
            mirror_to_qto=bool(mirror),
        )

    delta_model = 0.0
    used_path = "root-local"
    diag_lines: List[str] = []

    mc_list = []
    if use_crs_mode and target_z not in (None, ""):
        all_mcs = get_all_map_conversions(model)
        if all_mcs:
            mc_list = all_mcs if update_all_mcs else [all_mcs[0]]

    if mc_list:
        new_m = ui_to_meters(target_z, units_code)
        delta_m = new_m
        for mc in mc_list:
            old_height = float(getattr(mc, "OrthogonalHeight", 0.0) or 0.0)
            mc.OrthogonalHeight = new_m
            if show_diag:
                diag_lines.append(f"MapConversion {mc.id()} OrthogonalHeight {old_height} → {new_m} m")
        if crs_set_storey_elev:
            delta_model = meters_to_model_units(delta_m, model)
            old_storey_elev = float(getattr(storey, "Elevation", 0.0) or 0.0)
            storey.Elevation = old_storey_elev + delta_model
            if show_diag:
                diag_lines.append(f"Storey.Elevation {old_storey_elev} mu → {storey.Elevation} mu")
        else:
            delta_model = meters_to_model_units(delta_m, model)
        used_path = "crs-mapconversion(all)" if (update_all_mcs and len(mc_list) > 1) else "crs-mapconversion"
    else:
        if target_z not in (None, ""):
            root_lp = ascend_to_root_local_placement(storey.ObjectPlacement)
            root_pt = get_location_cartesian_point(root_lp)
            if root_pt is None:
                raise ValueError("Could not find root CartesianPoint for the storey's placement.")
            coords = list(root_pt.Coordinates)
            if len(coords) < 3:
                raise ValueError("Root CartesianPoint has no Z coordinate.")
            old_z = float(coords[2]) if coords[2] is not None else 0.0
            new_z = to_model_units_length(target_z, units_code, model)
            delta_model = new_z - old_z
            coords[2] = new_z
            root_pt.Coordinates = tuple(coords)
            old_storey_elev = float(getattr(storey, "Elevation", 0.0) or 0.0)
            storey.Elevation = float(new_z)
            if show_diag:
                diag_lines.append(f"RootLP {old_z} mu → {new_z} mu (Δ={delta_model} mu)")
                diag_lines.append(f"Storey.Elevation (Root mode) {old_storey_elev} mu → {storey.Elevation} mu")
            used_path = "root-local"

    shifted = 0
    if countershift_geometry and abs(delta_model) > 0:
        shifted = countershift_product_local_points(model, delta_model)

    model.write(output_path)

    mu_m = model_length_unit_in_m(model)
    mu_label = "m" if abs(mu_m - 1.0) < 1e-12 else ("mm" if abs(mu_m - 1e-3) < 1e-12 else f"{mu_m} m/unit")
    parts = [
        "Done ✅",
        f"Schema: {model.schema}",
        f"Model length unit: {mu_label}",
        f"Mode: {'CRS (IfcMapConversion)' if used_path.startswith('crs-mapconversion') else 'Root LocalPlacement'}",
        f"Target Z = {target_z if target_z not in (None,'') else ''} {units_code}",
        f"Δ applied (model units) = {delta_model}",
        (f"Counter-shifted {shifted} product placements by −Δ (kept world positions)." if shifted else None),
    ]
    try:
        site = (model.by_type("IfcSite") or [None])[0]
        site_ref = float(getattr(site, "RefElevation", 0.0) or 0.0) if site else 0.0
        parts.append(f"Site.RefElevation = {site_ref} mu")
        parts.append(f"Storey.Elevation = {float(getattr(storey,'Elevation',0.0) or 0.0)} mu")
        mcs = get_all_map_conversions(model)
        if mcs:
            parts.append(f"MapConversion.OrthogonalHeight = {float(getattr(mcs[0],'OrthogonalHeight',0.0) or 0.0)} m")
    except Exception:
        pass
    if show_diag and diag_lines:
        parts.append("Diagnostics:")
        parts.extend([" • " + d for d in diag_lines])
    if gross_maybe is not None:
        parts.append(f" • GrossHeight (UI {units_code}) = {gross_maybe}")
    if net_maybe is not None:
        parts.append(f" • NetHeight  (UI {units_code}) = {net_maybe}")
    if mom_txt:
        parts.append(f" • MethodOfMeasurement = '{mom_txt}'")
    parts.append(f"Output: {os.path.basename(output_path)}")
    return "\n".join([p for p in parts if p]), output_path


# ----------------------------------------------------------------------------
# Level manager: list, update, delete (with reassignment), add
# ----------------------------------------------------------------------------


def storey_elevation(storey) -> float:
    try:
        return float(getattr(storey, "Elevation", 0.0) or 0.0)
    except Exception:
        return 0.0


def storey_comp_height(storey) -> Optional[float]:
    # IFC4 adds ElevationOfRefHeight; fall back to None
    if hasattr(storey, "ElevationOfRefHeight"):
        val = getattr(storey, "ElevationOfRefHeight", None)
        return float(val) if val not in (None, "") else None
    return None


def list_storey_objects(storey) -> List[Any]:
    objs = []
    for rel in storey.ContainsElements or []:
        if rel.is_a("IfcRelContainedInSpatialStructure"):
            for el in rel.RelatedElements or []:
                objs.append(el)
    # Include spatial children (e.g., IfcSpace) aggregated beneath the storey
    for rel in storey.IsDecomposedBy or []:
        if rel.is_a("IfcRelAggregates"):
            for child in rel.RelatedObjects or []:
                if child.is_a("IfcSpace"):
                    objs.append(child)
    # Deduplicate by express ID to avoid double-counting if an element appears in multiple relations
    seen = {}
    for obj in objs:
        seen[obj.id()] = obj
    return list(seen.values())


def ensure_storey_associations(storey) -> List[Any]:
    try:
        assoc = getattr(storey, "HasAssociations", None)
    except Exception:
        assoc = None
    if assoc in (None, (), []):
        assoc = []
    else:
        assoc = list(assoc)
    try:
        storey.HasAssociations = assoc
    except Exception:
        try:
            setattr(storey, "HasAssociations", assoc)
        except Exception:
            pass
    return assoc


def containment_rels(model, obj) -> List[Any]:
    rels = []
    if hasattr(obj, "ContainedInStructure"):
        try:
            rels = list(obj.ContainedInStructure or [])
        except Exception:
            rels = []
    if not rels:
        try:
            rels = [
                r
                for r in model.get_inverse(obj) or []
                if r.is_a("IfcRelContainedInSpatialStructure") and obj in (r.RelatedElements or [])
            ]
        except Exception:
            rels = []
    return rels


def ensure_storey_associations(storey) -> List[Any]:
    try:
        assoc = getattr(storey, "HasAssociations", None)
    except Exception:
        assoc = None
    if assoc in (None, (), []):
        assoc = []
    else:
        assoc = list(assoc)
    try:
        storey.HasAssociations = assoc
    except Exception:
        try:
            setattr(storey, "HasAssociations", assoc)
        except Exception:
            pass
    return assoc


def move_objects_to_storey(model, objects, source_storey, target_storey):
    if target_storey is None:
        return
    delta = storey_elevation(source_storey) - storey_elevation(target_storey)
    # Ensure target containment relation
    target_rel = None
    for rel in target_storey.ContainsElements or []:
        if rel.is_a("IfcRelContainedInSpatialStructure"):
            target_rel = rel
            break
    if target_rel is None:
        target_rel = model.create_entity(
            "IfcRelContainedInSpatialStructure",
            GlobalId=new_guid(),
            RelatedElements=[],
            RelatingStructure=target_storey,
        )
        if getattr(target_storey, "ContainsElements", None):
            target_storey.ContainsElements = list(target_storey.ContainsElements) + [target_rel]
        else:
            target_storey.ContainsElements = [target_rel]

    for obj in objects:
        # Remove from current containment
        for rel in containment_rels(model, obj):
            if rel.RelatingStructure == source_storey and rel.is_a("IfcRelContainedInSpatialStructure"):
                rel.RelatedElements = [e for e in rel.RelatedElements if e != obj]
        # Adjust placement to keep world position
        adjust_local_placement_z(getattr(obj, "ObjectPlacement", None), delta)
        # Add to target relation
        if obj not in target_rel.RelatedElements:
            target_rel.RelatedElements = list(target_rel.RelatedElements) + [obj]


def cleanup_empty_containment(model, storey):
    for rel in list(storey.ContainsElements or []):
        if rel.is_a("IfcRelContainedInSpatialStructure") and not rel.RelatedElements:
            try:
                storey.ContainsElements = [r for r in storey.ContainsElements if r != rel]
                model.remove(rel)
            except Exception:
                pass


def remove_storey_from_aggregates(model, storey):
    for rel in list(storey.Decomposes or []):
        if rel.is_a("IfcRelAggregates"):
            rel.RelatedObjects = [o for o in rel.RelatedObjects if o != storey]
            if not rel.RelatedObjects:
                model.remove(rel)


COBIE_FLOOR_CLASS_NAME = "COBie Floors"


def _get_cobie_floor_classification(storey) -> Tuple[Optional[str], Optional[str]]:
    """Return (item_reference, name) for the COBie Floors classification reference if present."""
    for rel in ensure_storey_associations(storey):
        if not rel.is_a("IfcRelAssociatesClassification"):
            continue
        cref = getattr(rel, "RelatingClassification", None)
        if not cref or not cref.is_a("IfcClassificationReference"):
            continue
        source = getattr(cref, "ReferencedSource", None)
        if getattr(source, "Name", "") == COBIE_FLOOR_CLASS_NAME or getattr(cref, "Name", "") == COBIE_FLOOR_CLASS_NAME:
            return getattr(cref, "ItemReference", None), getattr(cref, "Name", None)
    return None, None


def list_levels(ifc_path: str) -> Dict[str, Any]:
    model = ifcopenshell.open(ifc_path)
    result = []
    for st in model.by_type("IfcBuildingStorey"):
        objs = list_storey_objects(st)
        cobie_ref, cobie_name = _get_cobie_floor_classification(st)
        result.append(
            {
                "id": st.id(),
                "name": getattr(st, "Name", ""),
                "description": getattr(st, "Description", ""),
                "elevation": storey_elevation(st),
                "comp_height": storey_comp_height(st),
                "global_id": getattr(st, "GlobalId", None),
                "cobie_floor": cobie_ref or cobie_name,
                "object_count": len(objs),
                "objects": [
                    {"id": o.id(), "name": getattr(o, "Name", ""), "type": o.is_a()}
                    for o in objs
                ],
            }
        )
    return {"levels": result}


def update_level(ifc_path: str, storey_id: int, payload: Dict[str, Any], output_path: str) -> str:
    model = ifcopenshell.open(ifc_path)
    storey = model.by_id(int(storey_id))
    if not storey:
        raise ValueError("Storey not found")
    if "name" in payload:
        storey.Name = payload.get("name")
    if "description" in payload:
        storey.Description = payload.get("description")
    if "global_id" in payload and payload.get("global_id"):
        storey.GlobalId = payload.get("global_id")
    if "elevation" in payload and payload.get("elevation") not in (None, ""):
        storey.Elevation = float(payload.get("elevation"))
    if "comp_height" in payload and hasattr(storey, "ElevationOfRefHeight"):
        comp = payload.get("comp_height")
        if comp not in (None, ""):
            storey.ElevationOfRefHeight = float(comp)

    if "cobie_floor" in payload:
        desired_ref = payload.get("cobie_floor")
        existing_rel = None
        existing_cref = None
        associations = ensure_storey_associations(storey)
        for rel in list(associations):
            if rel.is_a("IfcRelAssociatesClassification"):
                cref = getattr(rel, "RelatingClassification", None)
                source = getattr(cref, "ReferencedSource", None) if cref else None
                if cref and cref.is_a("IfcClassificationReference") and (
                    getattr(source, "Name", "") == COBIE_FLOOR_CLASS_NAME
                    or getattr(cref, "Name", "") == COBIE_FLOOR_CLASS_NAME
                ):
                    existing_rel = rel
                    existing_cref = cref
                    break

        if desired_ref in (None, ""):
            if existing_rel:
                try:
                    storey.HasAssociations = [r for r in associations if r != existing_rel]
                    model.remove(existing_rel)
                except Exception:
                    pass
        else:
            classification = None
            for cls in model.by_type("IfcClassification"):
                if getattr(cls, "Name", "") == COBIE_FLOOR_CLASS_NAME:
                    classification = cls
                    break
            if classification is None:
                classification = model.create_entity(
                    "IfcClassification",
                    Name=COBIE_FLOOR_CLASS_NAME,
                    Source="COBie",
                )

            if existing_cref is None:
                existing_cref = model.create_entity(
                    "IfcClassificationReference",
                    ItemReference=str(desired_ref),
                    Name=COBIE_FLOOR_CLASS_NAME,
                    ReferencedSource=classification,
                )
                existing_rel = model.create_entity(
                    "IfcRelAssociatesClassification",
                    GlobalId=new_guid(),
                    RelatedObjects=[storey],
                    RelatingClassification=existing_cref,
                )
                associations = ensure_storey_associations(storey)
                storey.HasAssociations = list(associations) + [existing_rel]
            else:
                existing_cref.ItemReference = str(desired_ref)
                if getattr(existing_cref, "Name", "") in (None, "", COBIE_FLOOR_CLASS_NAME):
                    existing_cref.Name = COBIE_FLOOR_CLASS_NAME

    model.write(output_path)
    return output_path


def delete_level(ifc_path: str, storey_id: int, target_storey_id: int, object_ids: Optional[List[int]], output_path: str) -> str:
    model = ifcopenshell.open(ifc_path)
    storey = model.by_id(int(storey_id))
    target = model.by_id(int(target_storey_id)) if target_storey_id else None
    if not storey or not target:
        raise ValueError("Storey or target storey not found")

    objs = list_storey_objects(storey)
    if object_ids:
        objs = [o for o in objs if o.id() in object_ids]
    move_objects_to_storey(model, objs, storey, target)
    cleanup_empty_containment(model, storey)
    remove_storey_from_aggregates(model, storey)
    try:
        model.remove(storey)
    except Exception:
        pass
    model.write(output_path)
    return output_path


def add_level(
    ifc_path: str,
    name: str,
    description: Optional[str],
    elevation: Optional[float],
    comp_height: Optional[float],
    object_ids: Optional[List[int]],
    output_path: str,
) -> str:
    model = ifcopenshell.open(ifc_path)
    building = (model.by_type("IfcBuilding") or [None])[0]
    site = (model.by_type("IfcSite") or [None])[0]
    parent = building or site
    if parent is None:
        raise ValueError("No Building or Site found to host the new level")

    storey = model.create_entity(
        "IfcBuildingStorey",
        GlobalId=new_guid(),
        Name=name or "New Storey",
        Description=description,
        Elevation=float(elevation) if elevation not in (None, "") else None,
    )
    if comp_height not in (None, "") and hasattr(storey, "ElevationOfRefHeight"):
        storey.ElevationOfRefHeight = float(comp_height)

    ensure_aggregates(parent, storey, model)

    if object_ids:
        objs = [model.by_id(int(oid)) for oid in object_ids if model.by_id(int(oid))]
        for obj in objs:
            # find original storey for delta
            origin_storey = None
            for rel in containment_rels(model, obj):
                if rel.is_a("IfcRelContainedInSpatialStructure"):
                    origin_storey = rel.RelatingStructure
                    break
            delta = 0.0
            if origin_storey:
                delta = storey_elevation(origin_storey) - storey_elevation(storey)
            adjust_local_placement_z(getattr(obj, "ObjectPlacement", None), delta)
            # remove from old relations
            for rel in containment_rels(model, obj):
                if rel.is_a("IfcRelContainedInSpatialStructure"):
                    rel.RelatedElements = [e for e in rel.RelatedElements if e != obj]
            # add to new relation
            target_rel = None
            for rel in storey.ContainsElements or []:
                if rel.is_a("IfcRelContainedInSpatialStructure"):
                    target_rel = rel
                    break
            if target_rel is None:
                target_rel = model.create_entity(
                    "IfcRelContainedInSpatialStructure",
                    GlobalId=new_guid(),
                    RelatedElements=[],
                    RelatingStructure=storey,
                )
                storey.ContainsElements = list(storey.ContainsElements or []) + [target_rel]
            target_rel.RelatedElements = list(target_rel.RelatedElements) + [obj]

    model.write(output_path)
    return output_path


def reassign_objects(
    ifc_path: str,
    source_storey_id: int,
    target_storey_id: int,
    object_ids: Optional[List[int]],
    output_path: str,
) -> str:
    model = ifcopenshell.open(ifc_path)
    source = model.by_id(int(source_storey_id))
    target = model.by_id(int(target_storey_id))
    if not source or not target:
        raise ValueError("Source or target storey not found")
    objs = list_storey_objects(source)
    if object_ids:
        objs = [o for o in objs if o.id() in object_ids]
    move_objects_to_storey(model, objs, source, target)
    cleanup_empty_containment(model, source)
    model.write(output_path)
    return output_path


def apply_level_actions(ifc_path: str, actions: List[Dict[str, Any]], output_path: str) -> str:
    if not actions:
        raise ValueError("No actions supplied")
    work_path = output_path
    current_path = ifc_path
    for idx, action in enumerate(actions):
        kind = action.get("type")
        if kind == "update":
            update_level(
                current_path,
                int(action["storey_id"]),
                action.get("payload", {}),
                work_path,
            )
        elif kind == "delete":
            delete_level(
                current_path,
                int(action["storey_id"]),
                int(action["target_storey_id"]),
                action.get("object_ids"),
                work_path,
            )
        elif kind == "add":
            add_level(
                current_path,
                name=action.get("name"),
                description=action.get("description"),
                elevation=action.get("elevation"),
                comp_height=action.get("comp_height"),
                object_ids=action.get("object_ids"),
                output_path=work_path,
            )
        elif kind == "reassign":
            reassign_objects(
                current_path,
                int(action["source_storey_id"]),
                int(action["target_storey_id"]),
                action.get("object_ids"),
                work_path,
            )
        else:
            raise ValueError(f"Unsupported action type: {kind}")
        current_path = work_path
    return work_path


# ----------------------------------------------------------------------------
# Proxy / Type mapper (from app (3).py)
# ----------------------------------------------------------------------------

FALLBACK_ENUM_LIBRARY = {
    "IfcWasteTerminalTypeEnum": {
        "FLOORTRAP",
        "FLOORWASTE",
        "GULLYSUMP",
        "GULLYTRAP",
        "GREASEINTERCEPTOR",
        "OILINTERCEPTOR",
        "PETROLINTERCEPTOR",
        "ROOFDRAIN",
        "WASTEDISPOSALUNIT",
        "WASTETRAP",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcPipeSegmentTypeEnum": {
        "CULVERT",
        "FLEXIBLESEGMENT",
        "GUTTER",
        "RIGIDSEGMENT",
        "SPOOL",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcDistributionChamberElementTypeEnum": {
        "FORMEDDUCT",
        "INSPECTIONCHAMBER",
        "INSPECTIONPIT",
        "MANHOLE",
        "METERCHAMBER",
        "SUMP",
        "TRENCH",
        "VALVECHAMBER",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcTankTypeEnum": {
        "PREFORMED",
        "SECTIONAL",
        "EXPANSION",
        "PRESSUREVESSEL",
        "FEEDANDEXPANSION",
        "USERDEFINED",
        "NOTDEFINED",
    },
}

LEGACY_OCCURRENCE_FALLBACK = {
    "IFCWASTETERMINALTYPE": "IFCFLOWTERMINAL",
    "IFCPIPESEGMENTTYPE": "IFCFLOWSEGMENT",
    "IFCTANKTYPE": "IFCFLOWSTORAGEDEVICE",
}


def normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def split_meaningful_tokens(type_name: str) -> List[str]:
    return [part.strip() for part in (type_name or "").split("_") if part.strip()]


def parse_name_parts(type_name: str) -> Dict[str, Any]:
    raw_tokens = split_meaningful_tokens(type_name)
    ordinal_index = None
    for idx in range(len(raw_tokens) - 1, -1, -1):
        if re.fullmatch(r"type\s*\d*", raw_tokens[idx], re.IGNORECASE):
            ordinal_index = idx
            break

    tokens_without_ordinal = raw_tokens[:ordinal_index] if ordinal_index is not None else raw_tokens
    classish_raw = tokens_without_ordinal[0] if tokens_without_ordinal else ""
    predef_candidate_raw = ""
    if len(tokens_without_ordinal) > 1:
        predef_candidate_raw = "_".join(tokens_without_ordinal[1:])

    return {
        "raw_tokens": raw_tokens,
        "tokens_without_ordinal": tokens_without_ordinal,
        "classish_raw": classish_raw,
        "predef_candidate_raw": predef_candidate_raw,
        "ordinal_raw": raw_tokens[ordinal_index] if ordinal_index is not None else "",
    }


def _schema_definition(schema_name: str):
    try:
        return ifcopenshell.ifcopenshell_wrapper.schema_by_name(schema_name)
    except Exception:
        return None


def _entity_names(schema_def) -> set:
    if schema_def is None:
        return set()
    out = set()
    try:
        for decl in schema_def.declarations():
            try:
                if decl.as_entity() is not None:
                    out.add(decl.name())
            except Exception:
                continue
    except Exception:
        pass
    return out


def build_type_class_lookup(schema_name: str) -> Dict[str, str]:
    schema_def = _schema_definition(schema_name)
    lookup = {}
    if schema_def is None:
        return lookup
    for entity_name in _entity_names(schema_def):
        if not entity_name.startswith("Ifc") or not entity_name.endswith("Type"):
            continue
        key = normalize_token(entity_name[3:-4])
        if key:
            lookup[key] = entity_name
    return lookup


def resolve_type_class_from_name(type_name: str, type_lookup: Dict[str, str]) -> Dict[str, Any]:
    parsed = parse_name_parts(type_name)
    raw_tokens = parsed["raw_tokens"]
    tokens_without_ordinal = parsed["tokens_without_ordinal"]

    classish_tokens_used = 0
    resolved_type_class = None
    parsed_classish = parsed["classish_raw"]
    parsed_predef = parsed["predef_candidate_raw"]

    if parsed_classish:
        direct_key = normalize_token(parsed_classish)
        resolved_type_class = type_lookup.get(direct_key)
        if resolved_type_class:
            classish_tokens_used = 1

    if not resolved_type_class:
        for i in range(1, len(tokens_without_ordinal) + 1):
            candidate = "".join(tokens_without_ordinal[:i])
            resolved = type_lookup.get(normalize_token(candidate))
            if resolved:
                resolved_type_class = resolved
                classish_tokens_used = i
                parsed_classish = "_".join(tokens_without_ordinal[:i])
                parsed_predef = "_".join(tokens_without_ordinal[i:])
                break

    return {
        **parsed,
        "parsed_classish": parsed_classish,
        "parsed_predef": parsed_predef,
        "resolved_type_class": resolved_type_class,
        "classish_tokens_used": classish_tokens_used,
        "raw_tokens": raw_tokens,
    }


def _predefined_type_info(schema_name: str, entity_class: str) -> Dict[str, Any]:
    schema_def = _schema_definition(schema_name)
    if schema_def is None:
        return {"has_predefined": False, "enum_name": None, "enum_items": []}
    try:
        decl = schema_def.declaration_by_name(entity_class)
    except Exception:
        return {"has_predefined": False, "enum_name": None, "enum_items": []}

    attr = None
    try:
        for a in decl.all_attributes():
            if a.name().lower() == "predefinedtype":
                attr = a
                break
    except Exception:
        attr = None
    if attr is None:
        return {"has_predefined": False, "enum_name": None, "enum_items": []}

    enum_name = None
    enum_items: List[str] = []
    try:
        attr_type = attr.type_of_attribute()
        declared = attr_type.declared_type() if hasattr(attr_type, "declared_type") else None
        if declared is not None and hasattr(declared, "enumeration_items"):
            enum_name = declared.name()
            enum_items = [str(item) for item in declared.enumeration_items()]
    except Exception:
        pass
    return {"has_predefined": True, "enum_name": enum_name, "enum_items": enum_items}


def resolve_predefined_literal(predef_candidate_raw: str, enum_items: List[str]) -> Dict[str, str]:
    enum_lookup = {normalize_token(item): item for item in enum_items}
    normalized_candidate = normalize_token(predef_candidate_raw)
    if normalized_candidate and normalized_candidate in enum_lookup:
        return {"value": enum_lookup[normalized_candidate], "reason": "enum matched"}
    if "USERDEFINED" in enum_items:
        return {"value": "USERDEFINED", "reason": "no enum match → USERDEFINED"}
    return {"value": "", "reason": "no enum match"}


def resolve_occurrence_from_type_class(schema_name: str, type_class: Optional[str]) -> Optional[str]:
    if not type_class:
        return None
    if not type_class.upper().endswith("TYPE"):
        return None
    base = type_class[:-4]
    entity_names = _entity_names(_schema_definition(schema_name))
    if base in entity_names:
        return base
    return LEGACY_OCCURRENCE_FALLBACK.get(type_class.upper())

FORCED_PREDEFINED = {
    "ifcpipesegmenttype": "RIGIDSEGMENT",
}


def build_enum_library(model):
    enums = {}
    if model is None:
        return enums

    try:
        schema = model.wrapped_data.schema
    except Exception:
        try:
            schema = model.wrapped_data.schema()
        except Exception:
            return enums

    try:
        for t in schema.types():
            try:
                is_enum = t.is_enumeration_type()
            except AttributeError:
                is_enum = getattr(t, "is_enumeration", lambda: False)()
            if not is_enum:
                continue

            name = t.name()
            values = set()
            try:
                for it in t.enumeration_items():
                    values.add(it.name())
            except Exception:
                pass
            if values:
                enums[name] = values
    except Exception:
        pass

    return enums


def resolve_type_and_predefined_for_name(type_name: str, schema_name: str) -> Dict[str, Any]:
    type_lookup = build_type_class_lookup(schema_name)
    resolved = resolve_type_class_from_name(type_name, type_lookup)
    resolved_type_class = resolved.get("resolved_type_class")
    predef_info = _predefined_type_info(schema_name, resolved_type_class) if resolved_type_class else {
        "has_predefined": False,
        "enum_name": None,
        "enum_items": [],
    }
    predef_resolution = resolve_predefined_literal(resolved.get("parsed_predef", ""), predef_info.get("enum_items", []))
    return {
        **resolved,
        "resolved_predefined_type": predef_resolution.get("value", ""),
        "resolved_predefined_reason": predef_resolution.get("reason", ""),
        "predef_info": predef_info,
    }


def rewrite_proxy_types(in_path: str, out_path: str) -> Tuple[str, str]:
    with open(in_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    model = None
    schema_name = "IFC4"
    try:
        model = ifcopenshell.open(in_path)
        schema_name = model.schema
    except Exception:
        schema_name = "IFC4"

    stats = {
        "proxy_types_total": 0,
        "building_types_total": 0,
        "proxy_types_converted": 0,
        "building_types_converted": 0,
        "left_as_proxy_type": 0,
        "left_as_building_type": 0,
        "occurrences_converted": 0,
    }

    typeid_to_occ_entity = {}

    proxy_type_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTPROXYTYPE"
        r"\('(?P<guid>[^']*)',"
        r"(?P<own>[^,]*),"
        r"'(?P<name>[^']*)',"
        r"(?P<mid>.*),"
        r"\.(?P<enum>\w+)\.\);",
        re.IGNORECASE,
    )

    building_type_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTTYPE"
        r"\('(?P<guid>[^']*)',"
        r"(?P<own>[^,]*),"
        r"'(?P<name>[^']*)',"
        r"(?P<mid>.*)\);",
        re.IGNORECASE,
    )

    updated_lines = []

    for line in lines:
        m_proxy = proxy_type_re.match(line)
        if m_proxy:
            stats["proxy_types_total"] += 1
            g = m_proxy.groupdict()
            ws = g["ws"]
            type_id = g["id"]
            guid = g["guid"]
            owner = g["own"]
            type_name = g["name"]
            mid = g["mid"]

            resolved = resolve_type_and_predefined_for_name(type_name, schema_name)
            target_type = resolved.get("resolved_type_class")
            if not target_type:
                stats["left_as_proxy_type"] += 1
                updated_lines.append(line)
                continue
            enum_val = resolved.get("resolved_predefined_type") or "USERDEFINED"

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["proxy_types_converted"] += 1

            occ_entity = resolve_occurrence_from_type_class(schema_name, target_type) or "IFCBUILDINGELEMENTPROXY"
            typeid_to_occ_entity[type_id] = occ_entity.upper()
            continue

        m_build = building_type_re.match(line)
        if m_build:
            stats["building_types_total"] += 1
            g = m_build.groupdict()
            ws = g["ws"]
            type_id = g["id"]
            guid = g["guid"]
            owner = g["own"]
            type_name = g["name"]
            mid = g["mid"]

            resolved = resolve_type_and_predefined_for_name(type_name, schema_name)
            target_type = resolved.get("resolved_type_class")
            if not target_type:
                stats["left_as_building_type"] += 1
                updated_lines.append(line)
                continue
            enum_val = resolved.get("resolved_predefined_type") or "USERDEFINED"

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["building_types_converted"] += 1

            occ_entity = resolve_occurrence_from_type_class(schema_name, target_type) or "IFCBUILDINGELEMENTPROXY"
            typeid_to_occ_entity[type_id] = occ_entity.upper()
            continue

        updated_lines.append(line)

    rel_def_type_re = re.compile(
        r"^(?P<ws>\s*)#(?P<relid>\d+)=IFCRELDEFINESBYTYPE\("
        r"'(?P<guid>[^']*)',"
        r"(?P<owner>[^,]*),"
        r"(?P<name>[^,]*),"
        r"(?P<desc>[^,]*),"
        r"\((?P<objs>[^)]*)\),"
        r"(?P<typeid>#\d+)\);",
        re.IGNORECASE,
    )

    occid_to_entity = {}
    for line in updated_lines:
        m = rel_def_type_re.match(line)
        if not m:
            continue
        d = m.groupdict()
        type_id = d["typeid"]
        if type_id not in typeid_to_occ_entity:
            continue
        occ_entity = typeid_to_occ_entity[type_id]
        objs_raw = d["objs"]
        obj_ids = [o.strip() for o in objs_raw.split(",") if o.strip()]
        for oid in obj_ids:
            occid_to_entity[oid] = occ_entity

    final_lines = []

    occ_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTPROXY\(",
        re.IGNORECASE,
    )

    for line in updated_lines:
        m = occ_re.match(line)
        if not m:
            final_lines.append(line)
            continue

        ws = m.group("ws")
        occ_id = m.group("id")
        if occ_id not in occid_to_entity:
            final_lines.append(line)
            continue

        new_entity = occid_to_entity[occ_id]
        rest = line[m.end():]
        new_line = f"{ws}{occ_id}={new_entity}({rest}"
        final_lines.append(new_line)
        stats["occurrences_converted"] += 1

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(final_lines)

    base = os.path.basename(in_path)
    summary = (
        f"Input file:  {base}\n"
        f"Output file: {os.path.basename(out_path)}\n\n"
        f"Proxy types (IFCBUILDINGELEMENTPROXYTYPE) found: {stats['proxy_types_total']}\n"
        f"  → converted to specific IFC types: {stats['proxy_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTPROXYTYPE: {stats['left_as_proxy_type']}\n\n"
        f"Building types (IFCBUILDINGELEMENTTYPE) found: {stats['building_types_total']}\n"
        f"  → converted to specific IFC types: {stats['building_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTTYPE: {stats['left_as_building_type']}\n\n"
        f"Occurrences converted from IfcBuildingElementProxy to typed entities: "
        f"{stats['occurrences_converted']}\n\n"
        "Occurrences are retyped only when an IfcRelDefinesByType exists and the "
        "referenced type could be mapped. Mapping is IFC2x3-compliant: waste terminals "
        "→ IfcFlowTerminal/IfcWasteTerminalType, pipe segments → "
        "IfcFlowSegment/IfcPipeSegmentType, tanks → "
        "IfcFlowStorageDevice/IfcTankType, distribution chambers → "
        "IfcDistributionChamberElement/IfcDistributionChamberElementType.\n"
    )

    return out_path, summary


# ----------------------------------------------------------------------------
# Presentation layer purge
# ----------------------------------------------------------------------------

DEEP_LAYER_RE = re.compile(r"^[A-Z]-Ss\d{6,}--")
SHALLOW_BASE_RE = re.compile(r"^[A-Z]-Ss\d{4}0--")
ALLOWED_LAYER_RE = re.compile(r"[A-Z]-Ss\d{4}0--[A-Za-z0-9-]*")
DEFAULT_ALLOWED_LAYERS = {
    "I-Ss4015--GeneralFittingsFurnishingsAndEquipment(FF&E)Systems",
    "L-SL0000--HabitatSupervisedAreas",
    "L-SL0000--HardInformalAndSocialArea",
    "L-SL0000--HardOutdoorPE",
    "L-SL0000--NonNetSiteArea",
    "L-SL0000--SoftInformalAndSocialArea",
    "L-SL0000--SoftOutdoorPE",
    "L-Ss2514--GeneralPatternWireMeshFencingSystems",
    "L-Ss2514--WoodCloseBoardedFencingSystems",
    "L-Ss2532--GateSystems",
    "L-Ss3014--AcrylicAndResinBoundAggregatePavingSystems",
    "L-Ss3014--AsphaltConcretePavingSystems",
    "L-Ss3014--ConcreteRoadAndPavingSystems",
    "L-Ss3014--HogginPavingSystems",
    "L-Ss3014--PermeableSmallUnitPavingSystems",
    "L-Ss3014--PorousAsphaltConcreteSportsPavingSystems",
    "L-Ss4535--AmenityAndOrnamentalPlantingSystems",
    "L-Ss4535--BiodiversityAndEnvironmentalConservationSystems",
    "L-Ss4535--GrassSeedingSystems",
    "L-Ss4535--PitPlantedLargeTreeSystems",
    "L-Ss4535--PitPlantedSmallTreeAndShrubSystems",
    "O-Ss2530--RollerShutterDoorsetSystems",
    "O-Ss4015--CommercialCateringFF&ESystems",
    "O-Ss4015--JanitorialUnitSystems",
    "O-Ss4015--Shelving,StorageAndEnclosuresSystems",
    "O-Ss4015--SinkSystems",
    "O-Ss4510--InsectControlSystems",
    "O-Ss5040--WasteCollectionSystems",
    "O-Ss5515--WaterTreatmentFiltrationSystems",
    "O-Ss6060--RefrigerationSystems",
    "O-Ss6540--KitchenExtractVentilationSystems",
    "S-EF2005--Substructure",
    "S-EF2510--Walls",
    "S-EF3020--Floors",
    "Z-Ss4070--Equipment",
    "Z-Ss5035--SurfaceAndWastewaterDrainageCollectionSystems",
    "Z-Ss6050--WaterTanksAndCisterns",
    "Z-Ss6070--DistributionBoxesAndSwitchboards",
    "Z-Ss6070--LowVoltageSwitchgear",
    "Z-Ss6070--PowerSupplyProducts",
    "Z-Ss6075--CommunicationsSourceProducts",
    "Z-Ss6552--PipesAndFittings",
    "Z-Ss6553--PumpProducts",
    "Z-Ss6554--ValveProducts",
    "Z-Ss6565--DuctDampers",
    "Z-Ss6565--DuctworkAndFittings",
    "Z-Ss6567--Fans",
    "Z-Ss6567--SoundAttenuators",
    "Z-Ss6570--CablesConductorsAndFittingsProducts",
    "Z-Ss6572--ElectricalProtectiveDevices",
    "Z-Ss7060--HeatEmitters",
    "Z-Ss7065--AirTerminalsAndDiffusers",
    "Z-Ss7080--LightingSystems",
    "Z-Ss7550--MechanicalAndElectricalServicesControlProducts",
    "Z-Ss8010--CableTransportSystems",
    "Z-Ss8077--EquipmentEnclosuresCabinetsBoxesAndHousings",
}


def parse_allowed_layers(text_or_file: Optional[str]) -> set:
    if not text_or_file:
        return set()
    tokens = set()
    for token in ALLOWED_LAYER_RE.findall(text_or_file):
        tokens.add(token.strip())
    return tokens


def compute_shallow_layer(layer_value: str) -> Optional[str]:
    if not layer_value:
        return None
    match = re.match(r"^([A-Z]-Ss)(\d{4})\d+", layer_value)
    if not match:
        return None
    prefix, digits = match.groups()
    return f"{prefix}{digits}0--"


def propose_layer_mapping(
    current_value: str,
    allowed_set: set,
    explicit_map: Dict[str, str],
    auto_shallow: bool,
) -> Optional[Dict[str, str]]:
    if not current_value:
        return None
    if current_value in explicit_map:
        target = explicit_map[current_value]
        allowed_status = "unknown"
        if allowed_set:
            allowed_status = "yes" if target in allowed_set else "no"
        return {"target": target, "reason": "Explicit", "allowed": allowed_status}
    if not DEEP_LAYER_RE.match(current_value):
        return None
    if not auto_shallow:
        allowed_status = "unknown" if not allowed_set else "no"
        return {"target": "", "reason": "Manual", "allowed": allowed_status}
    base = compute_shallow_layer(current_value)
    if not base:
        return None
    if allowed_set:
        if base in allowed_set:
            return {"target": base, "reason": "Shallow", "allowed": "yes"}
        return {"target": base, "reason": "Manual", "allowed": "no"}
    return {"target": base, "reason": "Shallow", "allowed": "unknown"}


def _extract_property_value(prop: ifcopenshell.entity_instance) -> Optional[str]:
    if not prop or not prop.is_a("IfcPropertySingleValue"):
        return None
    nominal = getattr(prop, "NominalValue", None)
    if nominal is None:
        return None
    return getattr(nominal, "wrappedValue", nominal)


def find_layer_properties(element: ifcopenshell.entity_instance) -> List[Dict[str, Any]]:
    layer_props = []
    ifc_file = element.file

    def collect_from_definition(definition, source: str):
        if not definition or not definition.is_a("IfcPropertySet"):
            return
        for prop in definition.HasProperties or []:
            if not prop.is_a("IfcPropertySingleValue"):
                continue
            name = getattr(prop, "Name", "") or ""
            if name.lower() != "layer":
                continue
            value = _extract_property_value(prop)
            layer_props.append({"id": prop.id(), "value": value, "source": source})

    for rel in getattr(element, "IsDefinedBy", None) or []:
        if rel.is_a("IfcRelDefinesByProperties"):
            collect_from_definition(rel.RelatingPropertyDefinition, "occurrence")

    element_type = ifcopenshell.util.element.get_type(element)
    if element_type:
        for definition in getattr(element_type, "HasPropertySets", None) or []:
            collect_from_definition(definition, "type")

    return layer_props


def scan_layers(
    ifc_path: str,
    allowed_set: set,
    explicit_map: Dict[str, str],
    options: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    model = ifcopenshell.open(ifc_path)
    auto_shallow = bool(options.get("auto_shallow", True))
    rows = []
    elements = [e for e in model.by_type("IfcProduct") if getattr(e, "GlobalId", None)]
    for element in elements:
        presentation_layers = []
        for layer in ifcopenshell.util.element.get_layers(model, element):
            name = getattr(layer, "Name", None)
            if name:
                presentation_layers.append({"id": layer.id(), "name": name})
        property_layers = find_layer_properties(element)
        property_values = [p["value"] for p in property_layers if p.get("value")]
        property_display = "; ".join([v for v in property_values if v])
        if presentation_layers:
            for layer_info in presentation_layers:
                mapping = propose_layer_mapping(layer_info["name"], allowed_set, explicit_map, auto_shallow)
                if not mapping:
                    continue
                row = {
                    "row_id": uuid.uuid4().hex,
                    "globalid": element.GlobalId,
                    "ifc_class": element.is_a(),
                    "source": "presentation",
                    "presentation_layer": layer_info["name"],
                    "property_layer": property_display,
                    "presentation_layer_id": layer_info["id"],
                    "property_layer_id": None,
                    "presentation_layers": presentation_layers,
                    "property_layers": property_layers,
                    "target_layer": mapping["target"],
                    "mapping_reason": mapping["reason"],
                    "allowed_status": mapping["allowed"],
                    "apply_default": mapping["allowed"] == "yes",
                }
                rows.append(row)
        else:
            for prop_info in property_layers:
                mapping = propose_layer_mapping(prop_info.get("value"), allowed_set, explicit_map, auto_shallow)
                if not mapping:
                    continue
                row = {
                    "row_id": uuid.uuid4().hex,
                    "globalid": element.GlobalId,
                    "ifc_class": element.is_a(),
                    "source": "property",
                    "presentation_layer": "",
                    "property_layer": prop_info.get("value") or "",
                    "presentation_layer_id": None,
                    "property_layer_id": prop_info.get("id"),
                    "presentation_layers": presentation_layers,
                    "property_layers": property_layers,
                    "target_layer": mapping["target"],
                    "mapping_reason": mapping["reason"],
                    "allowed_status": mapping["allowed"],
                    "apply_default": mapping["allowed"] == "yes",
                }
                rows.append(row)

    stats = {
        "schema": model.schema,
        "elements": len(elements),
        "presentation_layers": sum(len(ifcopenshell.util.element.get_layers(model, e)) for e in elements),
        "property_layers": sum(len(find_layer_properties(e)) for e in elements),
        "rows": len(rows),
    }
    return stats, rows


def _update_property_value(model: ifcopenshell.file, prop: ifcopenshell.entity_instance, new_value: str) -> None:
    nominal = getattr(prop, "NominalValue", None)
    value_type = "IfcLabel"
    if nominal is not None and hasattr(nominal, "is_a"):
        value_type = nominal.is_a()
    prop.NominalValue = model.create_entity(value_type, new_value)


def apply_layer_changes(
    ifc_path: str,
    rows_to_apply: List[Dict[str, Any]],
    options: Dict[str, Any],
) -> Tuple[str, str, str]:
    model = ifcopenshell.open(ifc_path)
    update_both = bool(options.get("update_both", False))
    updated_layers = set()
    updated_props = set()
    change_log = []
    now = datetime.datetime.utcnow().isoformat() + "Z"

    for row in rows_to_apply:
        target = row.get("target_layer") or ""
        if not target:
            continue
        mapping_reason = row.get("mapping_reason", "")
        allowed_status = row.get("allowed_status", "")
        if row.get("source") == "presentation" or update_both:
            presentation_layers = row.get("presentation_layers", [])
            if row.get("source") == "presentation" and not update_both:
                presentation_layers = [
                    info
                    for info in presentation_layers
                    if info.get("id") == row.get("presentation_layer_id")
                ]
            for layer_info in presentation_layers:
                layer_id = layer_info.get("id")
                if layer_id in updated_layers:
                    continue
                layer = model.by_id(layer_id) if layer_id else None
                if not layer or not layer.is_a("IfcPresentationLayerAssignment"):
                    continue
                old_value = getattr(layer, "Name", None)
                if old_value != layer_info.get("name"):
                    continue
                if old_value == target:
                    updated_layers.add(layer_id)
                    continue
                layer.Name = target
                updated_layers.add(layer_id)
                change_log.append(
                    {
                        "globalid": row.get("globalid"),
                        "ifc_class": row.get("ifc_class"),
                        "target": "presentation_layer",
                        "old_value": old_value,
                        "new_value": target,
                        "mapping_reason": mapping_reason,
                        "allowed_status": allowed_status,
                        "timestamp": now,
                    }
                )
        if row.get("source") == "property" or update_both:
            property_layers = row.get("property_layers", [])
            if row.get("source") == "property" and not update_both:
                property_layers = [
                    info for info in property_layers if info.get("id") == row.get("property_layer_id")
                ]
            for prop_info in property_layers:
                prop_id = prop_info.get("id")
                if prop_id in updated_props:
                    continue
                prop = model.by_id(prop_id) if prop_id else None
                if not prop or not prop.is_a("IfcPropertySingleValue"):
                    continue
                old_value = _extract_property_value(prop)
                if old_value != prop_info.get("value"):
                    continue
                if old_value == target:
                    updated_props.add(prop_id)
                    continue
                _update_property_value(model, prop, target)
                updated_props.add(prop_id)
                change_log.append(
                    {
                        "globalid": row.get("globalid"),
                        "ifc_class": row.get("ifc_class"),
                        "target": "property_layer",
                        "old_value": old_value,
                        "new_value": target,
                        "mapping_reason": mapping_reason,
                        "allowed_status": allowed_status,
                        "timestamp": now,
                    }
                )

    base_dir = os.path.dirname(ifc_path)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(ifc_path))[0]
    out_path = os.path.join(base_dir, f"{base}_layer_purged_{ts}.ifc")
    model.write(out_path)

    json_path = os.path.join(base_dir, f"{base}_layer_purge_log_{ts}.json")
    csv_path = os.path.join(base_dir, f"{base}_layer_purge_log_{ts}.csv")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(change_log, f, indent=2)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "globalid",
                "ifc_class",
                "target",
                "target_source",
                "target_globalid",
                "target_ifc_id",
                "old_value",
                "new_value",
                "mapping_reason",
                "target_class",
                "allowed_status",
                "timestamp",
            ],
        )
        writer.writeheader()
        writer.writerows(change_log)

    return out_path, json_path, csv_path


def match_type_name_for_proxy(type_name: str) -> bool:
    if not type_name:
        return False
    return bool(resolve_type_and_predefined_for_name(type_name, "IFC4").get("resolved_type_class"))


def list_instance_classes(ifc_path: str) -> List[str]:
    model = ifcopenshell.open(ifc_path)
    classes = sorted({e.is_a() for e in model.by_type("IfcObject") if getattr(e, "GlobalId", None)})
    return classes


def scan_predefined_types(
    ifc_path: str,
    class_filter: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    model = ifcopenshell.open(ifc_path)
    schema_name = model.schema
    class_set = {c for c in (class_filter or []) if c}
    elements = [e for e in model.by_type("IfcObject") if getattr(e, "GlobalId", None)]
    if class_set:
        elements = [e for e in elements if e.is_a() in class_set]
    rows = []
    for element in elements:
        element_type = ifcopenshell.util.element.get_type(element)
        type_name = getattr(element_type, "Name", "") if element_type else ""
        resolved = resolve_type_and_predefined_for_name(type_name, schema_name)
        match_found = bool(resolved.get("resolved_type_class"))

        predef_target = None
        predef_target_source = "none"
        predef_reason = ""
        predef_target_info = {"has_predefined": False, "enum_name": None, "enum_items": []}

        if element_type is not None:
            type_info = _predefined_type_info(schema_name, element_type.is_a())
            if type_info["has_predefined"]:
                predef_target = element_type
                predef_target_source = "type"
                predef_target_info = type_info
                predef_reason = "target type has PredefinedType"

        if predef_target is None:
            occ_info = _predefined_type_info(schema_name, element.is_a())
            if occ_info["has_predefined"]:
                predef_target = element
                predef_target_source = "occurrence"
                predef_target_info = occ_info
                predef_reason = "target occurrence has PredefinedType"

        proposed = "USERDEFINED"
        resolved_predef = resolved.get("resolved_predefined_type") or ""
        resolved_reason = resolved.get("resolved_predefined_reason") or ""
        resolved_type_class = resolved.get("resolved_type_class")
        resolved_type_info = (
            _predefined_type_info(schema_name, resolved_type_class) if resolved_type_class else {"has_predefined": False}
        )
        if not match_found:
            predef_reason = "classish not resolved from type name"
        elif not resolved_type_info.get("has_predefined"):
            predef_reason = "PredefinedType not supported for this class in schema"
        elif predef_target is None:
            predef_reason = "PredefinedType not writable (no attr on type or occurrence)"
        elif predef_target_info.get("enum_items"):
            enum_items = predef_target_info.get("enum_items")
            predef_choice = resolve_predefined_literal(resolved.get("parsed_predef", ""), enum_items)
            proposed = predef_choice["value"] or "USERDEFINED"
            predef_reason = predef_choice["reason"]
        elif predef_target_info.get("has_predefined"):
            proposed = resolved_predef or "USERDEFINED"
            predef_reason = resolved_reason or "PredefinedType supported without enum metadata"
        else:
            proposed = "USERDEFINED"
            predef_reason = "PredefinedType not supported for this class in schema"

        rows.append(
            {
                "row_id": uuid.uuid4().hex,
                "globalid": element.GlobalId,
                "ifc_class": element.is_a(),
                "type_name": type_name or "",
                "match_found": match_found,
                "proposed_predefined_type": proposed,
                "apply_default": predef_target is not None and proposed not in ("", "N/A"),
                "predef_target_source": predef_target_source,
                "predef_target_globalid": getattr(predef_target, "GlobalId", None) if predef_target else None,
                "predef_target_id": int(predef_target.id()) if predef_target else None,
                "predef_target_class": predef_target.is_a() if predef_target else None,
                "parsed_classish": resolved.get("parsed_classish", ""),
                "resolved_type_class": resolved.get("resolved_type_class"),
                "parsed_predef_token": resolved.get("parsed_predef", ""),
                "resolved_predefined_type": proposed,
                "target_source": predef_target_source,
                "target_globalid": getattr(predef_target, "GlobalId", None) if predef_target else None,
                "target_ifc_id": int(predef_target.id()) if predef_target else None,
                "target_class": predef_target.is_a() if predef_target else None,
                "predef_supported": bool(predef_target_info.get("has_predefined")),
                "predef_reason": predef_reason,
                "schema": schema_name,
            }
        )
    stats = {"schema": model.schema, "elements": len(elements), "rows": len(rows)}
    return stats, rows


def apply_predefined_type_changes(
    ifc_path: str,
    rows_to_apply: List[Dict[str, Any]],
) -> Tuple[str, str, str]:
    model = ifcopenshell.open(ifc_path)
    change_log = []
    now = datetime.datetime.utcnow().isoformat() + "Z"
    updated = 0
    for row in rows_to_apply:
        target = row.get("proposed_predefined_type")
        if target in (None, ""):
            continue
        target_entity = None
        target_gid = row.get("target_globalid") or row.get("predef_target_globalid")
        if target_gid:
            target_entity = model.by_guid(target_gid)

        if target_entity is None:
            target_id = row.get("target_ifc_id") or row.get("predef_target_id")
            if target_id is not None:
                try:
                    candidate = model.by_id(int(target_id))
                except Exception:
                    candidate = None
                if candidate is not None and hasattr(candidate, "PredefinedType"):
                    target_entity = candidate

        if not target_entity or not hasattr(target_entity, "PredefinedType"):
            continue

        old_value = getattr(target_entity, "PredefinedType", None)
        if old_value == target:
            continue

        target_entity.PredefinedType = target
        updated += 1
        change_log.append(
            {
                "globalid": row.get("globalid"),
                "ifc_class": row.get("ifc_class"),
                "target": f"predefined_type:{row.get('target_source') or row.get('predef_target_source', 'none')}",
                "target_source": row.get("target_source") or row.get("predef_target_source", "none"),
                "target_globalid": row.get("target_globalid") or row.get("predef_target_globalid"),
                "target_ifc_id": row.get("target_ifc_id") or row.get("predef_target_id"),
                "old_value": old_value,
                "new_value": target,
                "mapping_reason": row.get("predef_reason") or ("Type name match" if row.get("match_found") else "No match"),
                "target_class": row.get("target_class") or row.get("predef_target_class") or (target_entity.is_a() if target_entity else None),
                "allowed_status": "n/a",
                "timestamp": now,
            }
        )

    base_dir = os.path.dirname(ifc_path)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(ifc_path))[0]
    out_path = os.path.join(base_dir, f"{base}_predefined_{ts}.ifc")
    model.write(out_path)

    json_path = os.path.join(base_dir, f"{base}_predefined_log_{ts}.json")
    csv_path = os.path.join(base_dir, f"{base}_predefined_log_{ts}.csv")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(change_log, f, indent=2)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "globalid",
                "ifc_class",
                "target",
                "target_source",
                "target_globalid",
                "target_ifc_id",
                "old_value",
                "new_value",
                "mapping_reason",
                "target_class",
                "allowed_status",
                "timestamp",
            ],
        )
        writer.writeheader()
        writer.writerows(change_log)

    return out_path, json_path, csv_path


# ----------------------------------------------------------------------------
# Model checking helpers
# ----------------------------------------------------------------------------

CHECK_CACHE: Dict[str, Any] = {"definitions": None, "summary": {}}


def _expression_lookup() -> Dict[str, str]:
    expr_cfg = load_expression_config()
    merged: Dict[str, str] = {}
    merged.update(expr_cfg.get("by_classification_system", {}))
    merged.update(expr_cfg.get("by_check_id", {}))
    return merged


def load_definitions() -> List[Any]:
    mapping_cfg = load_mapping_config()
    definitions = load_check_definitions(mapping_cfg, _expression_lookup())
    CHECK_CACHE["definitions"] = definitions
    CHECK_CACHE["summary"] = summarize_sections(definitions)
    return definitions


def _ensure_definitions() -> List[Any]:
    return CHECK_CACHE["definitions"] or load_definitions()


def serialize_field_descriptor(fd: FieldDescriptor) -> Dict[str, Any]:
    return {
        "kind": fd.kind.value,
        "attribute": fd.attribute_name,
        "property": fd.property_name,
        "pset": fd.pset_name,
        "quantity": fd.quantity_name,
        "qto": fd.qto_name,
        "classification_system": fd.classification_system,
        "expression": fd.expression,
    }


def serialize_definition(defn) -> Dict[str, Any]:
    return {
        "check_id": defn.check_id,
        "description": defn.description,
        "entity_scope": defn.entity_scope,
        "info": defn.info_to_check,
        "applicable_models": defn.applicable_models,
        "milestones": defn.milestones,
        "section": defn.section,
        "mapping_status": defn.mapping_status,
        "field": serialize_field_descriptor(defn.field) if defn.field else None,
    }


def _to_serializable(val: Any):
    if isinstance(val, (int, float, str)) or val is None:
        return val
    try:
        return float(val)
    except Exception:
        try:
            return str(val)
        except Exception:
            return None


def _filter_defs(defs: List[Any], section: Optional[str], riba_stage: Optional[str]) -> List[Any]:
    filtered = []
    for d in defs:
        if section and d.section != section:
            continue
        if riba_stage and d.milestones and riba_stage not in d.milestones:
            continue
        filtered.append(d)
    return filtered


def _collect_targets(model, defs: List[Any], entity_filter: Optional[str], entity_filters: Optional[List[str]]) -> List[Any]:
    targets = []
    entity_names = set()
    filters = entity_filters or []
    if entity_filter:
        filters.append(entity_filter)
    if filters:
        entity_names.update(filters)
    else:
        for d in defs:
            for ent in d.entity_scope:
                entity_names.add(ent)
    for ent in sorted(entity_names):
        try:
            for obj in model.by_type(ent):
                targets.append(obj)
        except Exception:
            continue
    return targets


def _row_id(obj) -> str:
    gid = getattr(obj, "GlobalId", None) or getattr(obj, "GlobalID", None)
    return f"{gid or obj.id()}"


def build_table_data(model, section: str, riba_stage: Optional[str], entity_filter: Optional[str], entity_filters: Optional[List[str]]) -> Dict[str, Any]:
    defs = _filter_defs(_ensure_definitions(), section, riba_stage)
    targets = _collect_targets(model, defs, entity_filter, entity_filters)
    expr_engine = ExpressionEngine(model)
    columns = [serialize_definition(d) for d in defs]
    rows = []
    for obj in targets:
        row_values: Dict[str, Dict[str, Any]] = {}
        issues_count = 0
        for d in defs:
            if d.field is None:
                continue
            val = get_value(obj, d.field)
            generated = expr_engine.evaluate(d.field.expression, obj) if d.field.expression else None
            validation = validate_value(model, obj, d.field, val, check_id=d.check_id)
            issues_count += len(validation)
            row_values[d.check_id] = {
                "value": _to_serializable(val),
                "generated": generated,
                "issues": [vars(v) for v in validation],
                "descriptor": serialize_field_descriptor(d.field),
            }
        rows.append(
            {
                "id": obj.id(),
                "global_id": getattr(obj, "GlobalId", None),
                "name": getattr(obj, "Name", None) or getattr(obj, "LongName", None),
                "type": obj.is_a(),
                "issues": issues_count,
                "values": row_values,
            }
        )
    return {
        "columns": columns,
        "rows": rows,
        "summary": {
            "rows": len(rows),
            "columns": len(columns),
            "issues": sum(r["issues"] for r in rows),
        },
    }


def _change_log_path(session_id: str) -> Path:
    return Path(SESSION_STORE.session_path(session_id)) / "change_log.json"


def read_change_log(session_id: str) -> List[Dict[str, Any]]:
    path = _change_log_path(session_id)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def append_change_log(session_id: str, entries: List[Dict[str, Any]]):
    path = _change_log_path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_change_log(session_id)
    existing.extend(entries)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


def update_step2ifc_job(job_id: str, **updates: Any) -> None:
    job = STEP2IFC_JOBS.get(job_id)
    if not job:
        return
    job.update(updates)


def run_step2ifc_auto_job(job_id: str, session_id: str, input_path: Path, output_path: Path) -> None:
    update_step2ifc_job(job_id, status="running", progress=5, message="Starting auto conversion")

    def progress_cb(percent: int, message: str) -> None:
        update_step2ifc_job(job_id, progress=percent, message=message)

    if not STEP2IFC_AVAILABLE:
        update_step2ifc_job(
            job_id,
            status="error",
            progress=100,
            message=STEP2IFC_IMPORT_ERROR or "step2ifc dependencies unavailable",
            error=True,
            done=True,
        )
        APP_LOGGER.error("STEP2IFC auto conversion unavailable: %s", STEP2IFC_IMPORT_ERROR)
        return

    if STEP2IFC_RUN_CONVERT is None:
        update_step2ifc_job(
            job_id,
            status="error",
            progress=100,
            message="step2ifc converter unavailable",
            error=True,
            done=True,
        )
        APP_LOGGER.error("STEP2IFC converter unavailable")
        return

    mapping_path = STEP2IFC_JOBS.get(job_id, {}).get("mapping_path")
    if mapping_path:
        update_step2ifc_job(job_id, status="running", progress=15, message="Running mapped conversion")
        try:
            STEP2IFC_RUN_CONVERT(
                argparse.Namespace(
                    input_path=str(input_path),
                    output_path=str(output_path),
                    schema="IFC4",
                    units="mm",
                    project="Project",
                    site="Site",
                    building="Building",
                    storey="Storey",
                    geom="brep",
                    mesh_deflection=0.5,
                    mesh_angle=0.5,
                    merge_by_name=False,
                    split_by_assembly=False,
                    default_type="IfcBuildingElementProxy",
                    class_map=mapping_path,
                    log_path=None,
                )
            )
        except Exception as exc:  # pragma: no cover - background task guard
            APP_LOGGER.exception("STEP2IFC mapped conversion failed")
            update_step2ifc_job(
                job_id,
                status="error",
                progress=100,
                message=str(exc),
                error=True,
                done=True,
            )
            return
        update_step2ifc_job(job_id, progress=85, message="Mapped conversion complete; gathering outputs")
    else:
        try:
            auto_convert(input_path, output_path, progress_cb=progress_cb)
        except Exception as exc:  # pragma: no cover - background task guard
            APP_LOGGER.exception("STEP2IFC auto conversion failed")
            update_step2ifc_job(
                job_id,
                status="error",
                progress=100,
                message=str(exc),
                error=True,
                done=True,
            )
            return

    outputs = []
    for path in [
        output_path,
        output_path.with_suffix(".qc.json"),
        output_path.with_suffix(".qc.txt"),
        output_path.with_suffix(".log.jsonl"),
        output_path.parent / "classmap.autogen.yaml",
    ]:
        if path.exists():
            outputs.append({"name": path.name, "url": f"/api/session/{session_id}/download?name={path.name}"})

    update_step2ifc_job(
        job_id,
        status="done",
        progress=100,
        message="Auto conversion complete",
        done=True,
        error=False,
        outputs=outputs,
    )


def update_data_extract_job(job_id: str, **updates: Any) -> None:
    job = DATA_EXTRACT_JOBS.get(job_id)
    if not job:
        return
    job.update(updates)


def _write_csv_rows(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            writer.writerow([_clean_value(v) for v in row])


def _write_model_table(model: ifcopenshell.file, path: Path, source_file: str) -> None:
    header = ["Source_File", "IFC_Schema", "IFC_Description", "Entity_Count"]
    rows = [
        [
            source_file,
            getattr(model, "schema", ""),
            _clean_value(getattr(model, "description", "")),
            len(model),
        ]
    ]
    _write_csv_rows(path, header, rows)


def _format_child_entities(entity: Any) -> str:
    try:
        children = ifcopenshell.util.element.get_decomposition(entity) or []
    except Exception:
        children = []
    parts = []
    for child in children:
        name = getattr(child, "Name", "") or getattr(child, "LongName", "") or ""
        parts.append(f"{_line_ref(child)}{child.is_a()}({name})")
    return "; ".join(parts)


def _write_project_table(model: ifcopenshell.file, path: Path, source_file: str) -> None:
    header = [
        "IFC_Line_Ref",
        "Source_File",
        "IFC_Name",
        "IFC_GlobalId",
        "IFC_Entity",
        "IFC_Description",
        "IFC_LongName",
        "Child_Entities",
    ]
    rows: List[List[Any]] = []
    for ifc_type in ["IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey"]:
        for entity in model.by_type(ifc_type):
            rows.append(
                [
                    _line_ref(entity),
                    source_file,
                    getattr(entity, "Name", "") or "",
                    getattr(entity, "GlobalId", "") or "",
                    entity.is_a(),
                    getattr(entity, "Description", "") or "",
                    getattr(entity, "LongName", "") or "",
                    _format_child_entities(entity),
                ]
            )
    _write_csv_rows(path, header, rows)


def _write_object_table(
    objects: List[Any],
    path: Path,
    source_file: str,
    regexes: Dict[str, str],
) -> Dict[str, Any]:
    header = [
        "IFC_Line_Ref",
        "Source_File",
        "IFC_Name",
        "IFC_GlobalId",
        "IFC_Entity",
        "IFC_Description",
        "IFC_ObjectType",
        "IFC_Type",
        "IFC_Type_Name",
        "IFC_Predefined_Type",
        "IFC_Layer",
        "IFC_Tag",
        "IFC_Name_Syntax_Check",
        "IFC_Name_Short_Code",
        "IFC_Type_Syntax_Check",
        "IFC_Name_Type_Code",
        "IFC_Layer_Syntax_Check",
        "IFC_Name_Duplicate",
        "IFC_LongName",
        "IFC_Type_Line_Ref",
        "IFC_Type_Description",
        "IFC_TypeId",
        "Coordinates_xyz",
    ]
    name_counts: Dict[str, int] = {}
    for obj in objects:
        name = getattr(obj, "Name", "") or ""
        if name:
            name_counts[name] = name_counts.get(name, 0) + 1
    rows: List[List[Any]] = []
    for obj in objects:
        name = getattr(obj, "Name", "") or ""
        type_obj = ifcopenshell.util.element.get_type(obj)
        type_name = getattr(type_obj, "Name", "") if type_obj else ""
        layer_name = _get_layers_name(obj)
        rows.append(
            [
                _line_ref(obj),
                source_file,
                name,
                getattr(obj, "GlobalId", "") or "",
                obj.is_a(),
                getattr(obj, "Description", "") or "",
                getattr(obj, "ObjectType", "") or "",
                type_obj.is_a() if type_obj else "",
                type_name or "",
                getattr(obj, "PredefinedType", "") or "",
                layer_name,
                getattr(obj, "Tag", "") or "",
                _regex_check(regexes.get("regex_ifc_name", ""), name),
                _regex_extract(regexes.get("regex_ifc_name_code", ""), name),
                _regex_check(regexes.get("regex_ifc_type", ""), type_name),
                _regex_extract(regexes.get("regex_ifc_type_code", ""), type_name),
                _regex_check(regexes.get("regex_ifc_layer", ""), layer_name),
                "True" if name and name_counts.get(name, 0) > 1 else "False",
                getattr(obj, "LongName", "") or "",
                _line_ref(type_obj),
                getattr(type_obj, "Description", "") if type_obj else "",
                getattr(type_obj, "GlobalId", "") if type_obj else "",
                _get_object_xyz(obj),
            ]
        )
    _write_csv_rows(path, header, rows)
    return {"objects": objects, "name_counts": name_counts}


def _write_classification_table(
    model: ifcopenshell.file,
    path: Path,
    source_file: str,
    include_ids: Optional[set] = None,
) -> None:
    header = [
        "Source_File",
        "IFC_GlobalId",
        "IFC_Entity",
        "Classification_System",
        "Classification_Name",
        "Classification_Code",
        "Classification_Description",
    ]
    rows: List[List[Any]] = []
    for rel in model.by_type("IfcRelAssociatesClassification"):
        related = getattr(rel, "RelatedObjects", []) or []
        classification = getattr(rel, "RelatingClassification", None)
        if not classification:
            continue
        sys_name = getattr(classification, "Name", "") or ""
        code = getattr(classification, "Identification", "") or ""
        desc = getattr(classification, "Description", "") or ""
        if classification.is_a("IfcClassificationReference"):
            sys_name = getattr(classification, "ReferencedSource", None) and getattr(
                classification.ReferencedSource, "Name", ""
            )
            sys_name = sys_name or getattr(classification, "Name", "") or ""
            code = getattr(classification, "Identification", "") or getattr(classification, "ItemReference", "") or ""
            desc = getattr(classification, "Description", "") or ""
        for obj in related:
            if include_ids is not None and obj.id() not in include_ids:
                continue
            rows.append(
                [
                    source_file,
                    getattr(obj, "GlobalId", "") or "",
                    obj.is_a(),
                    sys_name or "",
                    getattr(classification, "Name", "") or "",
                    code or "",
                    desc or "",
                ]
            )
    _write_csv_rows(path, header, rows)


def _write_system_table(
    model: ifcopenshell.file,
    path: Path,
    source_file: str,
    include_ids: Optional[set] = None,
) -> None:
    header = [
        "Source_File",
        "IFC_GlobalId",
        "IFC_Entity",
        "System_Name",
        "System_GlobalId",
        "System_Description",
    ]
    rows: List[List[Any]] = []
    for rel in model.by_type("IfcRelAssignsToGroup"):
        group = getattr(rel, "RelatingGroup", None)
        if not group or not group.is_a("IfcSystem"):
            continue
        for obj in getattr(rel, "RelatedObjects", []) or []:
            if include_ids is not None and obj.id() not in include_ids:
                continue
            rows.append(
                [
                    source_file,
                    getattr(obj, "GlobalId", "") or "",
                    obj.is_a(),
                    getattr(group, "Name", "") or "",
                    getattr(group, "GlobalId", "") or "",
                    getattr(group, "Description", "") or "",
                ]
            )
    _write_csv_rows(path, header, rows)


def _write_spatial_table(
    model: ifcopenshell.file,
    path: Path,
    source_file: str,
    objects: List[Any],
) -> None:
    header = [
        "Source_File",
        "IFC_GlobalId",
        "IFC_Entity",
        "Container_Space",
        "Container_Storey",
        "Container_Building",
        "Container_Site",
        "Container_Project",
    ]
    rows: List[List[Any]] = []
    for obj in objects:
        container = ifcopenshell.util.element.get_container(obj)
        space = storey = building = site = project = None
        current = container
        while current:
            if current.is_a("IfcSpace"):
                space = current
            elif current.is_a("IfcBuildingStorey"):
                storey = current
            elif current.is_a("IfcBuilding"):
                building = current
            elif current.is_a("IfcSite"):
                site = current
            elif current.is_a("IfcProject"):
                project = current
            current = ifcopenshell.util.element.get_container(current)
        rows.append(
            [
                source_file,
                getattr(obj, "GlobalId", "") or "",
                obj.is_a(),
                _line_ref(space),
                _line_ref(storey),
                _line_ref(building),
                _line_ref(site),
                _line_ref(project),
            ]
        )
    _write_csv_rows(path, header, rows)


def _write_pset_template_table(
    path: Path,
    source_file: str,
    template_map: Dict[str, List[str]],
    object_type_counts: Dict[str, int],
) -> None:
    header = ["Source_File", "IFC_Entity_Occurrence_Type", "Pset_Name", "Applied_Count"]
    rows: List[List[Any]] = []
    for entity_type, psets in template_map.items():
        if not psets:
            rows.append([source_file, entity_type, "", object_type_counts.get(entity_type, 0)])
        for pset in psets:
            rows.append([source_file, entity_type, pset, object_type_counts.get(entity_type, 0)])
    _write_csv_rows(path, header, rows)


def _write_property_table(
    model: ifcopenshell.file,
    path: Path,
    source_file: str,
    objects: List[Any],
    template_map: Dict[str, List[str]],
) -> None:
    header = [
        "Source_File",
        "IFC_GlobalId",
        "IFC_Entity",
        "Pset_Name",
        "Property_Name",
        "Property_Value",
        "Property_Value_Type",
        "Unit",
        "IFC_TypeId",
    ]
    rows: List[List[Any]] = []
    for obj in objects:
        type_obj = ifcopenshell.util.element.get_type(obj)
        obj_type = obj.is_a()
        type_name = type_obj.is_a() if type_obj else ""
        allowed = None
        if obj_type in template_map:
            allowed = template_map[obj_type]
        elif type_name in template_map:
            allowed = template_map[type_name]
        psets = _safe_get_psets(obj)
        if allowed is None:
            continue
        if not allowed:
            allowed = list(psets.keys())
        for pset_name in allowed:
            props = psets.get(pset_name) or {}
            for prop_name, value in props.items():
                rows.append(
                    [
                        source_file,
                        getattr(obj, "GlobalId", "") or "",
                        obj.is_a(),
                        pset_name,
                        prop_name,
                        _clean_value(value),
                        type(value).__name__,
                        "",
                        getattr(type_obj, "GlobalId", "") if type_obj else "",
                    ]
                )
    _write_csv_rows(path, header, rows)


def run_data_extractor_job(
    job_id: str,
    session_id: str,
    ifc_files: List[str],
    exclude_filter: Optional[str],
    pset_template: Optional[str],
    tables: List[str],
    regexes: Dict[str, str],
) -> None:
    session_root = Path(SESSION_STORE.ensure(session_id))
    work_dir = Path(tempfile.mkdtemp(prefix="data_extractor_", dir=session_root))
    log_lines: List[str] = []

    def log(message: str) -> None:
        log_lines.append(message)
        update_data_extract_job(job_id, logs=log_lines)

    total_tables = max(len(tables), 1)
    update_data_extract_job(job_id, status="running", progress=2, message="Starting extraction", logs=log_lines)

    exclude_path = Path(exclude_filter) if exclude_filter else RESOURCE_DIR / "Exclude_Filter_Template.csv"
    pset_path = Path(pset_template) if pset_template else RESOURCE_DIR / "GPA_Pset_Template.csv"
    exclude_terms = _read_csv_first_column(exclude_path)
    template_map = _load_pset_template(pset_path)

    outputs: List[Dict[str, Any]] = []
    preview_payload: Optional[Dict[str, Any]] = None

    for file_index, file_name in enumerate(ifc_files, start=1):
        safe_name = sanitize_filename(file_name)
        input_path = session_root / safe_name
        if not input_path.exists():
            log(f"[{safe_name}] Missing IFC file.")
            continue
        base_name = os.path.splitext(safe_name)[0]
        file_dir = work_dir / base_name
        file_dir.mkdir(parents=True, exist_ok=True)
        log(f"[{safe_name}] Opening IFC...")
        try:
            model = ifcopenshell.open(str(input_path))
        except Exception as exc:
            log(f"[{safe_name}] ERROR opening IFC: {exc}")
            continue

        progress_base = int((file_index - 1) / max(len(ifc_files), 1) * 100)
        per_file_step = int(100 / max(len(ifc_files), 1))

        all_objects = _iter_object_elements(model)
        objects = [o for o in all_objects if not any(t in (getattr(o, "Name", "") or "") for t in exclude_terms)]
        include_ids = {obj.id() for obj in objects}
        object_type_counts: Dict[str, int] = {}
        for obj in objects:
            object_type_counts[obj.is_a()] = object_type_counts.get(obj.is_a(), 0) + 1

        for table_index, table_name in enumerate(tables, start=1):
            update_data_extract_job(
                job_id,
                progress=min(progress_base + int((table_index / total_tables) * per_file_step), 99),
                message=f"{safe_name}: {table_name}",
            )
            try:
                if table_name == "Model Data Table":
                    out_path = file_dir / f"IFC MODEL - {base_name}.csv"
                    _write_model_table(model, out_path, safe_name)
                elif table_name == "Project Data Table":
                    out_path = file_dir / f"IFC PROJECT - {base_name}.csv"
                    _write_project_table(model, out_path, safe_name)
                elif table_name == "Object Data Table":
                    out_path = file_dir / f"IFC OBJECT TYPE - {base_name}.csv"
                    _write_object_table(objects, out_path, safe_name, regexes)
                elif table_name == "Property Data Table":
                    out_path = file_dir / f"IFC PROPERTY - {base_name}.csv"
                    _write_property_table(model, out_path, safe_name, objects, template_map)
                elif table_name == "Classification Data Table":
                    out_path = file_dir / f"IFC CLASSIFICATION - {base_name}.csv"
                    _write_classification_table(model, out_path, safe_name, include_ids if include_ids else None)
                elif table_name == "Spatial Structure Data Table":
                    out_path = file_dir / f"IFC SPATIAL STRUCTURE - {base_name}.csv"
                    _write_spatial_table(model, out_path, safe_name, objects)
                elif table_name == "System Data Table":
                    out_path = file_dir / f"IFC SYSTEM - {base_name}.csv"
                    _write_system_table(model, out_path, safe_name, include_ids if include_ids else None)
                elif table_name == "Pset Template Data Table":
                    out_path = file_dir / f"IFC PSET TEMPLATE - {base_name}.csv"
                    _write_pset_template_table(out_path, safe_name, template_map, object_type_counts)
                else:
                    continue
            except Exception as exc:
                log(f"[{safe_name}] ERROR writing {table_name}: {exc}")

        log(f"[{safe_name}] Extraction complete.")

        if preview_payload is None:
            preview_candidates = [
                file_dir / f"IFC OBJECT TYPE - {base_name}.csv",
                file_dir / f"IFC PROJECT - {base_name}.csv",
                file_dir / f"IFC MODEL - {base_name}.csv",
            ]
            for candidate in preview_candidates:
                if candidate.exists():
                    df = pd.read_csv(candidate, nrows=200)
                    preview_payload = {"columns": list(df.columns), "rows": df.fillna("").values.tolist()}
                    break

    zip_name = f"ifc_data_extract_{uuid.uuid4().hex}.zip"
    zip_path = session_root / zip_name
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for file_path in work_dir.rglob("*.csv"):
            arcname = file_path.relative_to(work_dir)
            zipf.write(file_path, arcname.as_posix())
    outputs.append({"name": zip_name, "url": f"/api/session/{session_id}/download?name={zip_name}"})

    update_data_extract_job(
        job_id,
        status="done",
        progress=100,
        message="Extraction complete",
        done=True,
        error=False,
        outputs=outputs,
        preview=preview_payload,
    )


def update_ifc_qa_job(job_id: str, **updates: Any) -> None:
    job = IFC_QA_JOBS.get(job_id)
    if not job:
        return
    job.update(updates)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _evaluate_qa_rules(
    tables: Dict[str, List[Dict[str, str]]],
    rules: List[Dict[str, str]],
    property_requirements: List[Dict[str, str]],
    unacceptable_values: List[Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]], int]:
    failures: List[Dict[str, Any]] = []
    totals_by_page: Dict[str, Dict[str, int]] = {}
    total_checks = 0

    def bump(page: str, checks: int = 0, fails: int = 0) -> None:
        totals_by_page.setdefault(page, {"checks": 0, "fails": 0})
        totals_by_page[page]["checks"] += checks
        totals_by_page[page]["fails"] += fails

    for rule in rules:
        page = (rule.get("page") or "qa_summary").strip()
        table_name = (rule.get("table") or "").strip()
        field = (rule.get("field") or "").strip()
        check_type = (rule.get("check_type") or "required").strip().lower()
        pattern = rule.get("pattern") or ""
        severity = rule.get("severity") or "medium"
        message = rule.get("message") or "Rule failed"
        rows = tables.get(table_name, [])
        for row in rows:
            total_checks += 1
            value = row.get(field, "")
            passed = True
            if check_type == "required":
                passed = bool(value)
            elif check_type == "regex":
                try:
                    passed = bool(re.search(pattern, value or ""))
                except re.error:
                    passed = False
            elif check_type == "equals":
                passed = value == pattern
            elif check_type == "not_equals":
                passed = value != pattern
            elif check_type == "contains":
                passed = pattern in (value or "")
            if not passed:
                failures.append(
                    {
                        "page": page,
                        "rule_id": rule.get("rule_id") or "",
                        "severity": severity,
                        "source_file": row.get("Source_File", ""),
                        "ifc_globalid": row.get("IFC_GlobalId", ""),
                        "table_name": table_name,
                        "field": field,
                        "actual_value": value,
                        "message": message,
                    }
                )
                bump(page, checks=1, fails=1)
            else:
                bump(page, checks=1, fails=0)

    prop_rows = tables.get("IFC PROPERTY", [])
    prop_index = {}
    for row in prop_rows:
        key = (
            row.get("IFC_GlobalId", ""),
            row.get("IFC_Entity", ""),
            row.get("Pset_Name", ""),
            row.get("Property_Name", ""),
        )
        prop_index.setdefault(key, []).append(row)
    for req in property_requirements:
        if not (req.get("required") or "").strip().lower() == "true":
            continue
        total_checks += 1
        target_entity = req.get("ifc_entity", "")
        pset_name = req.get("pset_name", "")
        prop_name = req.get("property_name", "")
        severity = req.get("severity", "medium")
        message = req.get("message", "Required property missing")
        page = "property_values"
        matched = any(
            key[1] == target_entity and key[2] == pset_name and key[3] == prop_name for key in prop_index.keys()
        )
        if not matched:
            failures.append(
                {
                    "page": page,
                    "rule_id": req.get("rule_id", ""),
                    "severity": severity,
                    "source_file": "",
                    "ifc_globalid": "",
                    "table_name": "IFC PROPERTY",
                    "field": prop_name,
                    "actual_value": "",
                    "message": message,
                }
            )
            bump(page, checks=1, fails=1)
        else:
            bump(page, checks=1, fails=0)

    for bad in unacceptable_values:
        field = (bad.get("field") or "").strip()
        bad_value = bad.get("unacceptable_value", "")
        severity = bad.get("severity", "medium")
        message = bad.get("message", "Unacceptable value")
        for table_name, rows in tables.items():
            for row in rows:
                if field not in row:
                    continue
                total_checks += 1
                value = row.get(field, "")
                if value == bad_value:
                    page = "property_values" if table_name == "IFC PROPERTY" else "occurrence_naming"
                    failures.append(
                        {
                            "page": page,
                            "rule_id": bad.get("rule_id", ""),
                            "severity": severity,
                            "source_file": row.get("Source_File", ""),
                            "ifc_globalid": row.get("IFC_GlobalId", ""),
                            "table_name": table_name,
                            "field": field,
                            "actual_value": value,
                            "message": message,
                        }
                    )
                    bump(page, checks=1, fails=1)
                else:
                    bump("qa_summary", checks=1, fails=0)

    return failures, totals_by_page, total_checks


def run_ifc_qa_job(
    job_id: str,
    ifc_paths: List[Path],
    override_paths: Dict[str, Optional[Path]],
    session_id: Optional[str],
) -> None:
    job_dir = Path(tempfile.mkdtemp(prefix="ifc_qa_"))
    output_dir = job_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    logs: List[str] = []

    def log(msg: str) -> None:
        logs.append(msg)
        update_ifc_qa_job(job_id, logs=logs)

    try:
        update_ifc_qa_job(job_id, status="running", percent=2, currentStep="Initializing", logs=logs)

        regex_path = _qa_config_path(session_id, "regex_patterns", override_paths.get("regex_patterns"))
        exclude_path = _qa_config_path(session_id, "exclude_filter", override_paths.get("exclude_filter"))
        pset_path = _qa_config_path(session_id, "pset_template", override_paths.get("pset_template"))
        rules_path = _qa_config_path(session_id, "qa_rules", override_paths.get("qa_rules"))
        props_path = _qa_config_path(
            session_id, "qa_property_requirements", override_paths.get("qa_property_requirements")
        )
        bads_path = _qa_config_path(session_id, "qa_unacceptable_values", override_paths.get("qa_unacceptable_values"))

        regex_patterns = _load_regex_patterns(regex_path)
        regexes = {
            "regex_ifc_name": regex_patterns.get("regex_ifc_name", {}).get("pattern", ""),
            "regex_ifc_type": regex_patterns.get("regex_ifc_type", {}).get("pattern", ""),
            "regex_ifc_system": regex_patterns.get("regex_ifc_system", {}).get("pattern", ""),
            "regex_ifc_layer": regex_patterns.get("regex_ifc_layer", {}).get("pattern", ""),
            "regex_ifc_name_code": regex_patterns.get("regex_ifc_name_code", {}).get("pattern", ""),
            "regex_ifc_type_code": regex_patterns.get("regex_ifc_type_code", {}).get("pattern", ""),
            "regex_ifc_system_code": regex_patterns.get("regex_ifc_system_code", {}).get("pattern", ""),
        }
        for key, meta in regex_patterns.items():
            if meta.get("enabled") == "false":
                regexes[key] = ""

        exclude_terms = _read_csv_first_column(exclude_path)
        pset_template = _load_pset_template(pset_path)

        qa_rules = _read_csv_rows(rules_path)
        qa_property_requirements = _read_csv_rows(props_path)
        qa_unacceptable = _read_csv_rows(bads_path)

        for index, ifc_path in enumerate(ifc_paths, start=1):
            safe_name = sanitize_filename(ifc_path.name)
            base_name = ifc_path.stem
            file_out_dir = output_dir / base_name
            file_out_dir.mkdir(parents=True, exist_ok=True)
            update_ifc_qa_job(
                job_id,
                currentFile=safe_name,
                currentStep="Opening IFC",
                percent=min(int(index / max(len(ifc_paths), 1) * 100), 95),
            )
            log(f"[{safe_name}] Opening IFC...")
            try:
                model = ifcopenshell.open(str(ifc_path))
            except Exception as exc:
                log(f"[{safe_name}] ERROR opening IFC: {exc}")
                continue

            all_objects = _iter_object_elements(model)
            objects = [
                o for o in all_objects if not any(t in (getattr(o, "Name", "") or "") for t in exclude_terms)
            ]
            include_ids = {obj.id() for obj in objects}
            object_type_counts: Dict[str, int] = {}
            for obj in objects:
                object_type_counts[obj.is_a()] = object_type_counts.get(obj.is_a(), 0) + 1

            update_ifc_qa_job(job_id, currentStep="Extracting IFC PROJECT")
            _write_project_table(model, file_out_dir / f"IFC PROJECT - {base_name}.csv", safe_name)

            update_ifc_qa_job(job_id, currentStep="Extracting IFC OBJECT TYPE")
            _write_object_table(objects, file_out_dir / f"IFC OBJECT TYPE - {base_name}.csv", safe_name, regexes)

            update_ifc_qa_job(job_id, currentStep="Extracting IFC CLASSIFICATION")
            _write_classification_table(
                model,
                file_out_dir / f"IFC CLASSIFICATION - {base_name}.csv",
                safe_name,
                include_ids if include_ids else None,
            )

            update_ifc_qa_job(job_id, currentStep="Extracting IFC SPATIAL STRUCTURE")
            _write_spatial_table(
                model,
                file_out_dir / f"IFC SPATIAL STRUCTURE - {base_name}.csv",
                safe_name,
                objects,
            )

            update_ifc_qa_job(job_id, currentStep="Extracting IFC SYSTEM")
            _write_system_table(
                model,
                file_out_dir / f"IFC SYSTEM - {base_name}.csv",
                safe_name,
                include_ids if include_ids else None,
            )

            update_ifc_qa_job(job_id, currentStep="Extracting IFC PSET TEMPLATE")
            _write_pset_template_table(
                file_out_dir / f"IFC PSET TEMPLATE - {base_name}.csv",
                safe_name,
                pset_template,
                object_type_counts,
            )

            update_ifc_qa_job(job_id, currentStep="Extracting IFC PROPERTY")
            _write_property_table(
                model,
                file_out_dir / f"IFC PROPERTY - {base_name}.csv",
                safe_name,
                objects,
                pset_template,
            )
            log(f"[{safe_name}] Extraction complete.")

        tables: Dict[str, List[Dict[str, str]]] = {}
        for csv_path in output_dir.rglob("*.csv"):
            stem = csv_path.name.split(" - ")[0].strip()
            tables.setdefault(stem, [])
            tables[stem].extend(_read_csv_rows(csv_path))

        failures, totals_by_page, total_checks = _evaluate_qa_rules(
            tables,
            qa_rules,
            qa_property_requirements,
            qa_unacceptable,
        )
        objects_checked = len(tables.get("IFC OBJECT TYPE", []))
        total_failures = len(failures)
        pass_percent = 100.0 if total_checks == 0 else round(100.0 * (total_checks - total_failures) / total_checks, 2)

        pages = [
            "project_naming",
            "occurrence_naming",
            "type_naming",
            "classification_template",
            "classification_values",
            "pset_template",
            "property_values",
            "system",
            "zone",
        ]
        per_page = {}
        for page in pages:
            stats = totals_by_page.get(page, {"checks": 0, "fails": 0})
            checks = stats["checks"]
            fails = stats["fails"]
            per_page[page] = {
                "checks": checks,
                "fails": fails,
                "pass_percent": 100.0 if checks == 0 else round(100.0 * (checks - fails) / checks, 2),
            }

        qa_summary = {
            "overall": {
                "files_checked": len(ifc_paths),
                "objects_checked": objects_checked,
                "total_checks": total_checks,
                "total_failures": total_failures,
                "pass_percent": pass_percent,
            },
            "per_page": per_page,
            "failures": failures,
        }
        summary_path = job_dir / "qa_summary.json"
        with open(summary_path, "w", encoding="utf-8") as handle:
            json.dump(qa_summary, handle, indent=2)

        zip_path = job_dir / f"ifc_qa_{job_id}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            for file_path in output_dir.rglob("*.csv"):
                arcname = file_path.relative_to(output_dir)
                zipf.write(file_path, arcname.as_posix())
            zipf.write(summary_path, summary_path.name)

        update_ifc_qa_job(
            job_id,
            status="complete",
            percent=100,
            currentStep="Complete",
            result_path=str(zip_path),
            summary=qa_summary,
        )
    except Exception as exc:  # pragma: no cover - job-level guard
        log(f"ERROR: {exc}")
        update_ifc_qa_job(job_id, status="failed", percent=100, currentStep="Failed")


def apply_edits(session_id: str, in_path: str, edits: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    if not edits:
        raise HTTPException(status_code=400, detail="No edits supplied")
    model = ifcopenshell.open(in_path)
    defs = _ensure_definitions()
    def_by_id = {d.check_id: d for d in defs}
    expr_engine = ExpressionEngine(model)
    audits = []
    for e in edits:
        check_id = e.get("check_id")
        obj_id = e.get("object_id")
        if check_id not in def_by_id:
            continue
        desc = def_by_id[check_id].field
        if desc is None:
            continue
        target = model.by_id(int(obj_id))
        if target is None:
            continue
        mode = e.get("mode", "manual")
        value = e.get("value")
        if mode == "generated" and desc.expression:
            value = expr_engine.evaluate(desc.expression, target)
        old_val, new_val = set_value(model, target, desc, value)
        audits.append(
            {
                "object_id": target.id(),
                "global_id": getattr(target, "GlobalId", None),
                "check_id": check_id,
                "field": desc.path_label(),
                "old": _to_serializable(old_val),
                "new": _to_serializable(new_val),
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            }
        )
    base, ext = os.path.splitext(os.path.basename(in_path))
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_name = f"{base}_checked_{ts}{ext or '.ifc'}"
    out_path = os.path.join(os.path.dirname(in_path), out_name)
    model.write(out_path)
    append_change_log(session_id, audits)
    return out_name, audits



def _tail_text(value: str, max_chars: int = 4000) -> str:
    return (value or "")[-max_chars:]


def _run_cobieqc_job(job_id: str) -> None:
    try:
        job = COBIEQC_JOB_STORE.update_job(
            job_id,
            status=STATUS_RUNNING,
            progress=0.1,
            started_at=datetime.datetime.utcnow().isoformat() + "Z",
            message="Running COBieQC reporter",
        )
        job_dir = COBIEQC_JOB_STORE.get_job_dir(job_id)
        input_path = job_dir / job.get("input_filename", "input.xlsx")
        stage = str(job.get("stage", "D")).upper()
        COBIEQC_JOB_STORE.append_log(job_id, f"Running stage {stage} for {input_path.name}")

        result = run_cobieqc(str(input_path), stage, str(job_dir))
        COBIEQC_JOB_STORE.append_log(job_id, "--- STDOUT ---")
        COBIEQC_JOB_STORE.append_log(job_id, result.get("stdout", ""))
        COBIEQC_JOB_STORE.append_log(job_id, "--- STDERR ---")
        COBIEQC_JOB_STORE.append_log(job_id, result.get("stderr", ""))

        if result.get("ok"):
            COBIEQC_JOB_STORE.update_job(
                job_id,
                status=STATUS_DONE,
                progress=1.0,
                message="Report generated",
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                output_filename=result.get("output_filename", "report.html"),
            )
        else:
            COBIEQC_JOB_STORE.update_job(
                job_id,
                status=STATUS_ERROR,
                progress=1.0,
                message=result.get("error") or "COBieQC failed",
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
            )
    except Exception as exc:
        COBIEQC_JOB_STORE.append_log(job_id, f"Unhandled error: {exc}")
        COBIEQC_JOB_STORE.update_job(
            job_id,
            status=STATUS_ERROR,
            progress=1.0,
            message=str(exc),
            finished_at=datetime.datetime.utcnow().isoformat() + "Z",
        )


# ----------------------------------------------------------------------------
# FastAPI app + routes
# ----------------------------------------------------------------------------

app = FastAPI(title="IFC Toolkit Hub")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def startup_cleanup():
    SESSION_STORE.cleanup_stale()
    COBIEQC_JOB_STORE.cleanup_old_jobs()
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False, timeout=10)
        if proc.returncode == 0:
            APP_LOGGER.info("COBieQC Java runtime detected")
        else:
            APP_LOGGER.warning("COBieQC Java runtime check failed: %s", (proc.stderr or proc.stdout or "").strip())
    except Exception as exc:
        APP_LOGGER.warning("COBieQC Java runtime unavailable: %s", exc)


@app.on_event("shutdown")
def shutdown_cleanup():
    for sid in list(SESSION_STORE.sessions.keys()):
        SESSION_STORE.drop(sid)


@app.get("/", response_class=HTMLResponse)
def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request, "active": "upload"})


@app.get("/cleaner", response_class=HTMLResponse)
def cleaner_page(request: Request):
    return templates.TemplateResponse("cleaner.html", {"request": request, "active": "cleaner"})


@app.get("/excel", response_class=HTMLResponse)
def excel_page(request: Request):
    return templates.TemplateResponse("excel.html", {"request": request, "active": "excel"})


@app.get("/ifc-qa/extractor", response_class=HTMLResponse)
def ifc_qa_extractor_page(request: Request):
    return templates.TemplateResponse(
        "ifc_qa.html",
        {"request": request, "active": "ifc-qa", "qa_page": "extractor"},
    )


@app.get("/ifc-qa/dashboard", response_class=HTMLResponse)
def ifc_qa_dashboard_page(request: Request):
    return templates.TemplateResponse(
        "ifc_qa.html",
        {"request": request, "active": "ifc-qa", "qa_page": "dashboard"},
    )


@app.get("/ifc-qa/config", response_class=HTMLResponse)
def ifc_qa_config_page(request: Request):
    return templates.TemplateResponse(
        "ifc_qa.html",
        {"request": request, "active": "ifc-qa", "qa_page": "config"},
    )


@app.get("/data-extractor", response_class=HTMLResponse)
def data_extractor_page(request: Request):
    return templates.TemplateResponse(
        "data_extractor.html",
        {"request": request, "active": "data-extractor", "regex_defaults": load_default_config()},
    )


@app.get("/storeys", response_class=HTMLResponse)
def storeys_page(request: Request):
    return templates.TemplateResponse("storeys.html", {"request": request, "active": "storeys"})


@app.get("/proxy", response_class=HTMLResponse)
def proxy_page(request: Request):
    return templates.TemplateResponse("proxy.html", {"request": request, "active": "proxy"})


@app.get("/presentation-layer", response_class=HTMLResponse)
def presentation_layer_page(request: Request):
    return templates.TemplateResponse(
        "presentation_layer.html",
        {"request": request, "active": "presentation-layer"},
    )


@app.get("/step2ifc", response_class=HTMLResponse)
def step2ifc_page(request: Request):
    return templates.TemplateResponse("step2ifc.html", {"request": request, "active": "step2ifc"})


@app.get("/tools/cobieqc", response_class=HTMLResponse)
def cobieqc_page(request: Request):
    return templates.TemplateResponse("cobieqc.html", {"request": request, "active": "cobieqc"})


@app.get("/files", response_class=HTMLResponse)
def files_page(request: Request):
    return templates.TemplateResponse("files.html", {"request": request, "active": "files"})


@app.get("/levels", response_class=HTMLResponse)
def levels_page(request: Request):
    return templates.TemplateResponse("levels.html", {"request": request, "active": "levels"})


@app.get("/viewer", response_class=HTMLResponse)
def viewer_page(request: Request):
    return HTMLResponse(
        "<html><body><h2>IFC Viewer temporarily disabled</h2><p>The viewer is not included in this build.</p></body></html>",
        status_code=503,
    )


@app.get("/model-checking", response_class=HTMLResponse)
def model_checking_page(request: Request):
    return templates.TemplateResponse("model_checking.html", {"request": request, "active": "model-checking"})


@app.get("/admin/mappings", response_class=HTMLResponse)
def admin_mappings_page(request: Request):
    return templates.TemplateResponse("mappings.html", {"request": request, "active": "mappings"})


@app.post("/api/session")
def create_session(payload: Dict[str, Any] = Body(default=None)):
    SESSION_STORE.cleanup_stale()
    incoming = payload.get("session_id") if payload else None
    if incoming and SESSION_STORE.exists(incoming):
        SESSION_STORE.touch(incoming)
        session_id = incoming
    else:
        session_id = SESSION_STORE.create()
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=SESSION_STORE.ttl_hours)
    return {"session_id": session_id, "expires_at": expiry.isoformat() + "Z"}


@app.delete("/api/session/{session_id}")
def end_session(session_id: str):
    SESSION_STORE.drop(session_id)
    return {"status": "deleted"}


@app.get("/api/session/{session_id}/files")
def list_files(session_id: str):
    root = SESSION_STORE.ensure(session_id)
    files = []
    for fname in sorted(os.listdir(root)):
        fpath = os.path.join(root, fname)
        if os.path.isfile(fpath):
            stat = os.stat(fpath)
            files.append({
                "name": fname,
                "size": stat.st_size,
                "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
    return {"files": files}


@app.get("/api/tools/cobieqc/health")
def cobieqc_health():
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False, timeout=10)
    except Exception as exc:
        return {"ok": False, "java_available": False, "detail": str(exc)}
    return {
        "ok": proc.returncode == 0,
        "java_available": proc.returncode == 0,
        "detail": (proc.stderr or proc.stdout or "").strip(),
    }


@app.post("/api/tools/cobieqc/run")
async def cobieqc_run(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    stage: str = Form("D"),
):
    stage = (stage or "D").upper()
    if stage not in {"D", "C"}:
        raise HTTPException(status_code=400, detail="Stage must be D or C")

    data = await file.read()
    safe_name = sanitize_upload_filename(file.filename or "input.xlsx")
    try:
        validate_upload(safe_name, len(data))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job = COBIEQC_JOB_STORE.create_job(stage=stage, original_filename=safe_name)
    job_dir = COBIEQC_JOB_STORE.get_job_dir(job["job_id"])
    input_path = job_dir / "input.xlsx"
    input_path.write_bytes(data)
    COBIEQC_JOB_STORE.append_log(job["job_id"], f"Saved input file {safe_name} ({len(data)} bytes)")

    background_tasks.add_task(_run_cobieqc_job, job["job_id"])
    return {"job_id": job["job_id"], "status": "queued"}


@app.get("/api/tools/cobieqc/jobs/{job_id}")
def cobieqc_job_status(job_id: str):
    if not COBIEQC_JOB_STORE.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    job = COBIEQC_JOB_STORE.read_job(job_id)
    return {
        "job_id": job["job_id"],
        "status": job.get("status"),
        "stage": job.get("stage"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "message": job.get("message"),
        "progress": job.get("progress", 0.0),
        "logs_tail": COBIEQC_JOB_STORE.logs_tail(job_id),
    }


@app.get("/api/tools/cobieqc/jobs/{job_id}/result")
def cobieqc_job_result(job_id: str):
    if not COBIEQC_JOB_STORE.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    job = COBIEQC_JOB_STORE.read_job(job_id)
    if job.get("status") not in {STATUS_DONE, STATUS_ERROR}:
        raise HTTPException(status_code=409, detail="Job not finished")

    job_dir = COBIEQC_JOB_STORE.get_job_dir(job_id)
    output_path = job_dir / (job.get("output_filename") or "report.html")
    preview_html = ""
    if output_path.exists() and output_path.stat().st_size > 0:
        preview_html = output_path.read_text(encoding="utf-8", errors="replace")[:200000]

    logs = COBIEQC_JOB_STORE.logs_tail(job_id, max_chars=24000)
    return {
        "ok": job.get("status") == STATUS_DONE,
        "output_filename": output_path.name,
        "preview_html": preview_html,
        "stdout_tail": _tail_text(logs),
        "stderr_tail": _tail_text(logs),
        "message": job.get("message"),
    }


@app.get("/api/tools/cobieqc/jobs/{job_id}/download")
def cobieqc_job_download(job_id: str):
    if not COBIEQC_JOB_STORE.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    job = COBIEQC_JOB_STORE.read_job(job_id)
    output_path = COBIEQC_JOB_STORE.get_job_dir(job_id) / (job.get("output_filename") or "report.html")
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(
        path=str(output_path),
        media_type="text/html",
        filename=output_path.name,
    )


@app.post("/api/session/{session_id}/step2ifc/auto")
def run_step2ifc_auto(
    session_id: str,
    payload: Dict[str, Any] = Body(...),
    background_tasks: BackgroundTasks = None,
):
    root = SESSION_STORE.ensure(session_id)
    if not STEP2IFC_AVAILABLE:
        raise HTTPException(status_code=503, detail=STEP2IFC_IMPORT_ERROR or "step2ifc dependencies unavailable")
    input_name = payload.get("input_file")
    if not input_name:
        raise HTTPException(status_code=400, detail="input_file is required")
    input_path = Path(root) / sanitize_filename(input_name)
    if not input_path.exists():
        raise HTTPException(status_code=404, detail="Input STEP file not found")

    output_name = payload.get("output_name") or f"{input_path.stem}.ifc"
    output_name = sanitize_filename(output_name)
    if not output_name.lower().endswith(".ifc"):
        output_name = f"{output_name}.ifc"
    output_path = Path(root) / output_name

    mapping_name = payload.get("mapping_file")
    mapping_path = None
    if mapping_name:
        safe_mapping = sanitize_filename(mapping_name)
        mapping_path = Path(root) / safe_mapping
        if not mapping_path.exists():
            raise HTTPException(status_code=404, detail="Mapping file not found")

    job_id = uuid.uuid4().hex
    STEP2IFC_JOBS[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "done": False,
        "error": False,
        "outputs": [],
        "mapping_path": str(mapping_path) if mapping_path else None,
    }
    APP_LOGGER.info("STEP2IFC job queued", extra={"job_id": job_id, "input": input_path.name, "output": output_name})

    if background_tasks is None:
        background_tasks = BackgroundTasks()
    background_tasks.add_task(run_step2ifc_auto_job, job_id, session_id, input_path, output_path)
    return {"job_id": job_id, "status_url": f"/api/session/{session_id}/step2ifc/auto/{job_id}"}


@app.get("/api/session/{session_id}/step2ifc/auto/{job_id}")
def get_step2ifc_status(session_id: str, job_id: str):
    SESSION_STORE.ensure(session_id)
    job = STEP2IFC_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/api/session/{session_id}/upload")
async def upload_files(session_id: str, files: List[UploadFile] = File(...)):
    root = SESSION_STORE.ensure(session_id)
    saved = []
    for f in files:
        safe = sanitize_filename(os.path.basename(f.filename))
        dest = os.path.join(root, safe)
        with open(dest, "wb") as dst:
            content = await f.read()
            dst.write(content)
        saved.append({"name": safe, "size": os.path.getsize(dest)})
    return {"files": saved}


@app.post("/api/ifc-qa/extract")
async def ifc_qa_extract(
    files: List[UploadFile] = File(...),
    qa_rules_csv: UploadFile = File(None),
    qa_property_requirements_csv: UploadFile = File(None),
    qa_unacceptable_values_csv: UploadFile = File(None),
    regex_patterns_csv: UploadFile = File(None),
    exclude_filter_csv: UploadFile = File(None),
    pset_template_csv: UploadFile = File(None),
    session_id: Optional[str] = Form(None),
    background_tasks: BackgroundTasks = None,
):
    if not files:
        raise HTTPException(status_code=400, detail="At least one IFC file is required")
    job_id = uuid.uuid4().hex
    IFC_QA_JOBS[job_id] = {
        "jobId": job_id,
        "status": "queued",
        "percent": 0,
        "currentFile": "",
        "currentStep": "Queued",
        "logs": [],
        "result_path": None,
        "summary": None,
    }

    upload_dir = Path(tempfile.mkdtemp(prefix="ifc_qa_uploads_"))
    ifc_paths: List[Path] = []
    for upload in files:
        safe = sanitize_filename(upload.filename)
        if not safe.lower().endswith(".ifc"):
            continue
        dest = upload_dir / safe
        with open(dest, "wb") as handle:
            handle.write(await upload.read())
        ifc_paths.append(dest)
    if not ifc_paths:
        raise HTTPException(status_code=400, detail="No IFC files uploaded")

    override_paths: Dict[str, Optional[Path]] = {
        "qa_rules": None,
        "qa_property_requirements": None,
        "qa_unacceptable_values": None,
        "regex_patterns": None,
        "exclude_filter": None,
        "pset_template": None,
    }

    async def save_override(upload: Optional[UploadFile], key: str) -> None:
        if not upload:
            return
        safe = sanitize_filename(upload.filename)
        dest = upload_dir / safe
        with open(dest, "wb") as handle:
            handle.write(await upload.read())
        override_paths[key] = dest

    await save_override(qa_rules_csv, "qa_rules")
    await save_override(qa_property_requirements_csv, "qa_property_requirements")
    await save_override(qa_unacceptable_values_csv, "qa_unacceptable_values")
    await save_override(regex_patterns_csv, "regex_patterns")
    await save_override(exclude_filter_csv, "exclude_filter")
    await save_override(pset_template_csv, "pset_template")

    if background_tasks is None:
        background_tasks = BackgroundTasks()
    background_tasks.add_task(run_ifc_qa_job, job_id, ifc_paths, override_paths, session_id)
    return {"jobId": job_id}


@app.get("/api/ifc-qa/progress/{job_id}")
def ifc_qa_progress(job_id: str):
    job = IFC_QA_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "status": job.get("status"),
        "percent": job.get("percent"),
        "currentFile": job.get("currentFile"),
        "currentStep": job.get("currentStep"),
        "logs": job.get("logs"),
    }


@app.get("/api/ifc-qa/result/{job_id}")
def ifc_qa_result(job_id: str):
    job = IFC_QA_JOBS.get(job_id)
    if not job or not job.get("result_path"):
        raise HTTPException(status_code=404, detail="Result not ready")
    path = Path(job["result_path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="Result file not found")
    return FileResponse(path, filename=path.name)


@app.get("/api/ifc-qa/summary/{job_id}")
def ifc_qa_summary(job_id: str):
    job = IFC_QA_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    summary = job.get("summary")
    if summary is None:
        raise HTTPException(status_code=404, detail="Summary not ready")
    return summary


@app.get("/api/ifc-qa/config/{session_id}")
def ifc_qa_config_status(session_id: str):
    override_dir = _qa_override_dir(session_id)
    overrides = sorted(p.name for p in override_dir.glob("*.csv"))
    return {
        "session_id": session_id,
        "overrides": overrides,
        "defaults": {k: str(v) for k, v in _qa_default_paths().items()},
    }


@app.get("/api/ifc-qa/config/{session_id}/download")
def ifc_qa_config_download(session_id: str):
    override_dir = _qa_override_dir(session_id)
    config_zip = Path(SESSION_STORE.ensure(session_id)) / "ifc_qa_configs.zip"
    defaults = _qa_default_paths()
    with zipfile.ZipFile(config_zip, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for key, path in defaults.items():
            override = override_dir / f"{key}.csv"
            chosen = override if override.exists() else path
            if chosen.exists():
                zipf.write(chosen, f"{key}.csv")
    return FileResponse(config_zip, filename="ifc_qa_configs.zip")


@app.get("/api/ifc-qa/config/{session_id}/regex")
def ifc_qa_config_regex(session_id: str):
    regex_path = _qa_config_path(session_id, "regex_patterns")
    patterns = []
    if regex_path.exists():
        with open(regex_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                patterns.append(
                    {
                        "key": row.get("key", ""),
                        "pattern": row.get("pattern", ""),
                        "enabled": (row.get("enabled") or "true").strip().lower(),
                    }
                )
    return {"patterns": patterns}


@app.post("/api/ifc-qa/config/{session_id}/upload")
async def ifc_qa_config_upload(session_id: str, config_zip: UploadFile = File(...)):
    override_dir = _qa_override_dir(session_id)
    zip_path = override_dir / sanitize_filename(config_zip.filename)
    with open(zip_path, "wb") as handle:
        handle.write(await config_zip.read())
    with zipfile.ZipFile(zip_path, "r") as zipf:
        for name in zipf.namelist():
            base = Path(name).name
            key = base.replace(".csv", "")
            if key not in _qa_default_paths():
                continue
            target = override_dir / f"{key}.csv"
            with zipf.open(name) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
    zip_path.unlink(missing_ok=True)
    return {"status": "ok"}


@app.post("/api/ifc-qa/config/{session_id}/reset")
def ifc_qa_config_reset(session_id: str):
    override_dir = _qa_override_dir(session_id)
    for entry in override_dir.glob("*.csv"):
        entry.unlink(missing_ok=True)
    return {"status": "ok"}


@app.post("/api/session/{session_id}/data-extractor/start")
def start_data_extractor(session_id: str, payload: Dict[str, Any] = Body(...), background_tasks: BackgroundTasks = None):
    root = Path(SESSION_STORE.ensure(session_id))
    ifc_files = payload.get("ifc_files") or []
    tables = payload.get("tables") or []
    if not ifc_files:
        raise HTTPException(status_code=400, detail="IFC files are required")
    if not tables:
        raise HTTPException(status_code=400, detail="Select at least one table")
    exclude_filter_name = payload.get("exclude_filter")
    pset_template_name = payload.get("pset_template")
    pset_template_default = payload.get("pset_template_default") or "GPA_Pset_Template.csv"
    regex_overrides = payload.get("regex_overrides") or {}

    defaults = load_default_config()
    defaults.update({k: v for k, v in regex_overrides.items() if v is not None})

    exclude_path = None
    if exclude_filter_name:
        safe = sanitize_filename(exclude_filter_name)
        exclude_candidate = root / safe
        if exclude_candidate.exists():
            exclude_path = str(exclude_candidate)

    pset_path = None
    if pset_template_name:
        safe = sanitize_filename(pset_template_name)
        pset_candidate = root / safe
        if pset_candidate.exists():
            pset_path = str(pset_candidate)
    if not pset_path and pset_template_default:
        default_candidate = RESOURCE_DIR / sanitize_filename(pset_template_default)
        if default_candidate.exists():
            pset_path = str(default_candidate)

    job_id = uuid.uuid4().hex
    DATA_EXTRACT_JOBS[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "done": False,
        "error": False,
        "outputs": [],
        "logs": [],
        "preview": None,
    }
    if background_tasks is None:
        background_tasks = BackgroundTasks()
    background_tasks.add_task(
        run_data_extractor_job,
        job_id,
        session_id,
        ifc_files,
        exclude_path,
        pset_path,
        tables,
        defaults,
    )
    return {"job_id": job_id, "status_url": f"/api/session/{session_id}/data-extractor/{job_id}"}


@app.get("/api/session/{session_id}/data-extractor/{job_id}")
def get_data_extractor_status(session_id: str, job_id: str):
    SESSION_STORE.ensure(session_id)
    job = DATA_EXTRACT_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/session/{session_id}/download")
def download_file(session_id: str, name: str):
    root = SESSION_STORE.ensure(session_id)
    safe = sanitize_filename(os.path.basename(name))
    path = os.path.join(root, safe)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=safe)


@app.get("/api/checks/definitions")
def api_check_definitions():
    defs = load_definitions()
    return {"definitions": [serialize_definition(d) for d in defs], "sections": CHECK_CACHE.get("summary", {})}


@app.get("/api/checks/mappings")
def api_check_mappings():
    return {
        "mapping_path": str(MAPPINGS_PATH),
        "expression_path": str(EXPRESSIONS_PATH),
        "mapping": load_mapping_config(),
        "expressions": load_expression_config(),
    }


@app.post("/api/checks/mappings")
def api_save_mapping(payload: Dict[str, Any] = Body(...)):
    check_id = payload.get("check_id")
    mapping = payload.get("mapping")
    if not check_id or not mapping:
        raise HTTPException(status_code=400, detail="check_id and mapping are required")
    data = save_mapping_for_check(check_id, mapping)
    load_definitions()
    return {"status": "ok", "mapping": data}


@app.post("/api/checks/expressions")
def api_save_expression(payload: Dict[str, Any] = Body(...)):
    check_id = payload.get("check_id")
    expression = payload.get("expression", "")
    if not check_id:
        raise HTTPException(status_code=400, detail="check_id is required")
    data = save_expression_for_check(check_id, expression)
    load_definitions()
    return {"status": "ok", "expressions": data}


@app.post("/api/session/{session_id}/checks/data")
def api_checks_data(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    ifc_file = payload.get("ifc_file")
    if not ifc_file:
        raise HTTPException(status_code=400, detail="ifc_file is required")
    path = os.path.join(root, sanitize_filename(ifc_file))
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    model = ifcopenshell.open(path)
    section = payload.get("section", "Spaces")
    riba = payload.get("riba_stage")
    ent_filter = payload.get("entity_filter")
    ent_filters = payload.get("entity_filters") or None
    table = build_table_data(model, section, riba, ent_filter, ent_filters)
    table["change_log"] = read_change_log(session_id)
    return table


@app.post("/api/session/{session_id}/checks/apply")
def api_checks_apply(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    ifc_file = payload.get("ifc_file")
    edits = payload.get("edits", [])
    if not ifc_file:
        raise HTTPException(status_code=400, detail="ifc_file is required")
    path = os.path.join(root, sanitize_filename(ifc_file))
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    out_name, audits = apply_edits(session_id, path, edits)
    return {
        "ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"},
        "audit": audits,
    }


@app.get("/api/session/{session_id}/checks/log")
def api_checks_log(session_id: str):
    SESSION_STORE.ensure(session_id)
    return {"entries": read_change_log(session_id)}


@app.post("/api/session/{session_id}/clean")
def run_cleaner(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    files = payload.get("files", [])
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    prefix = payload.get("prefix", "InfoDrainage")
    case_insensitive = bool(payload.get("case_insensitive", True))
    delete_psets_with_prefix = bool(payload.get("delete_psets_with_prefix", True))
    delete_properties_in_other_psets = bool(payload.get("delete_properties_in_other_psets", True))
    drop_empty_psets = bool(payload.get("drop_empty_psets", True))
    also_remove_loose_props = bool(payload.get("also_remove_loose_props", True))

    reports = []
    outputs = []
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    for fname in files:
        in_path = os.path.join(root, sanitize_filename(fname))
        if not os.path.isfile(in_path):
            raise HTTPException(status_code=404, detail=f"File not found: {fname}")
        base, ext = os.path.splitext(os.path.basename(in_path))
        out_name = f"{base}_cleaned_{ts}{ext or '.ifc'}"
        out_path = os.path.join(root, out_name)
        report = clean_ifc_file(
            in_path=in_path,
            out_path=out_path,
            prefix=prefix,
            case_insensitive=case_insensitive,
            delete_psets_with_prefix=delete_psets_with_prefix,
            delete_properties_in_other_psets=delete_properties_in_other_psets,
            drop_empty_psets=drop_empty_psets,
            also_remove_loose_props=also_remove_loose_props,
        )
        reports.append(report)
        outputs.append({"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"})
    return {"reports": reports, "outputs": outputs}


@app.post("/api/session/{session_id}/excel/extract")
def excel_extract(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    source = payload.get("ifc_file")
    if not source:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(source))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_extracted.xlsx"
    out_path = os.path.join(root, out_name)
    extract_to_excel(in_path, out_path)
    return {"excel": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/excel/update")
def excel_apply(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    ifc_name = payload.get("ifc_file")
    excel_name = payload.get("excel_file")
    if not ifc_name or not excel_name:
        raise HTTPException(status_code=400, detail="IFC and Excel files are required")
    in_path = os.path.join(root, sanitize_filename(ifc_name))
    xls_path = os.path.join(root, sanitize_filename(excel_name))
    if not os.path.isfile(in_path) or not os.path.isfile(xls_path):
        raise HTTPException(status_code=404, detail="Input file(s) not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_updated.ifc"
    out_path = os.path.join(root, out_name)
    update_ifc_from_excel(in_path, xls_path, out_path, update_mode=payload.get("update_mode", "update"), add_new=payload.get("add_new", "no"))
    return {"ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/storeys/parse")
def parse_storeys(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    return parse_ifc_storeys(in_path)


@app.post("/api/session/{session_id}/storeys/apply")
def apply_storeys(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    storey_id = payload.get("storey_id")
    if storey_id is None:
        raise HTTPException(status_code=400, detail="storey_id is required")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_gsb_adjusted.ifc"
    out_path = os.path.join(root, out_name)
    try:
        summary, path = apply_storey_changes(
            ifc_path=in_path,
            storey_id=storey_id,
            units_code=payload.get("units", "m"),
            gross_val=payload.get("gross"),
            net_val=payload.get("net"),
            mom_txt=payload.get("mom"),
            mirror=bool(payload.get("mirror", False)),
            target_z=payload.get("target_z"),
            countershift_geometry=bool(payload.get("countershift_geometry", True)),
            use_crs_mode=bool(payload.get("use_crs_mode", True)),
            update_all_mcs=bool(payload.get("update_all_mcs", True)),
            show_diag=bool(payload.get("show_diag", True)),
            crs_set_storey_elev=bool(payload.get("crs_set_storey_elev", True)),
            output_path=out_path,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "summary": summary,
        "ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"},
    }


@app.post("/api/session/{session_id}/proxy")
def proxy_mapper(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base, ext = os.path.splitext(os.path.basename(in_path))
    out_name = f"{base}_typed{ext or '.ifc'}"
    out_path = os.path.join(root, out_name)
    _, summary = rewrite_proxy_types(in_path, out_path)
    return {
        "summary": summary,
        "ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"},
    }


@app.post("/api/session/{session_id}/proxy/predefined/classes")
def proxy_predefined_classes(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    classes = list_instance_classes(in_path)
    return {"classes": classes}


@app.post("/api/session/{session_id}/proxy/predefined/scan")
def proxy_predefined_scan(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    class_filter = payload.get("classes") or []
    stats, rows = scan_predefined_types(in_path, class_filter=class_filter)
    return {"stats": stats, "rows": rows}


@app.post("/api/session/{session_id}/proxy/predefined/apply")
def proxy_predefined_apply(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    rows = payload.get("rows", [])
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    out_path, json_path, csv_path = apply_predefined_type_changes(in_path, rows)
    return {
        "ifc": {"name": os.path.basename(out_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(out_path)}"},
        "log_json": {"name": os.path.basename(json_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(json_path)}"},
        "log_csv": {"name": os.path.basename(csv_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(csv_path)}"},
    }


@app.post("/api/session/{session_id}/presentation-layer/scan")
def presentation_layer_scan(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    allowed_text = payload.get("allowed_text") or ""
    allowed_set = set(DEFAULT_ALLOWED_LAYERS)
    allowed_set.update(parse_allowed_layers(allowed_text))
    explicit_map = payload.get("explicit_map") or {}
    options = payload.get("options") or {}
    stats, rows = scan_layers(in_path, allowed_set, explicit_map, options)
    samples = sorted(list(allowed_set))[:5]
    return {"stats": stats, "rows": rows, "allowed_count": len(allowed_set), "allowed_samples": samples}


@app.post("/api/session/{session_id}/presentation-layer/apply")
def presentation_layer_apply(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    rows = payload.get("rows", [])
    options = payload.get("options") or {}
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    out_path, json_path, csv_path = apply_layer_changes(in_path, rows, options)
    return {
        "ifc": {"name": os.path.basename(out_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(out_path)}"},
        "log_json": {"name": os.path.basename(json_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(json_path)}"},
        "log_csv": {"name": os.path.basename(csv_path), "url": f"/api/session/{session_id}/download?name={os.path.basename(csv_path)}"},
    }


@app.post("/api/session/{session_id}/levels/list")
def api_levels_list(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    if not src:
        raise HTTPException(status_code=400, detail="No IFC file provided")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    return list_levels(in_path)


@app.post("/api/session/{session_id}/levels/update")
def api_levels_update(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    storey_id = payload.get("storey_id")
    if not src or storey_id is None:
        raise HTTPException(status_code=400, detail="ifc_file and storey_id are required")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_levels.ifc"
    out_path = os.path.join(root, out_name)
    try:
        update_level(in_path, int(storey_id), payload, out_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/levels/delete")
def api_levels_delete(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    storey_id = payload.get("storey_id")
    target_id = payload.get("target_storey_id")
    object_ids = payload.get("object_ids")
    if not src or storey_id is None or target_id is None:
        raise HTTPException(status_code=400, detail="ifc_file, storey_id, and target_storey_id are required")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_levels.ifc"
    out_path = os.path.join(root, out_name)
    try:
        delete_level(in_path, int(storey_id), int(target_id), object_ids, out_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/levels/add")
def api_levels_add(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    name = payload.get("name")
    if not src or not name:
        raise HTTPException(status_code=400, detail="ifc_file and name are required")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_levels.ifc"
    out_path = os.path.join(root, out_name)
    try:
        add_level(
            ifc_path=in_path,
            name=name,
            description=payload.get("description"),
            elevation=payload.get("elevation"),
            comp_height=payload.get("comp_height"),
            object_ids=payload.get("object_ids"),
            output_path=out_path,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/levels/reassign")
def api_levels_reassign(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    source_id = payload.get("source_storey_id")
    target_id = payload.get("target_storey_id")
    object_ids = payload.get("object_ids")
    if not src or source_id is None or target_id is None:
        raise HTTPException(status_code=400, detail="ifc_file, source_storey_id, and target_storey_id are required")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_name = f"{base}_levels.ifc"
    out_path = os.path.join(root, out_name)
    try:
        reassign_objects(in_path, int(source_id), int(target_id), object_ids, out_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ifc": {"name": out_name, "url": f"/api/session/{session_id}/download?name={out_name}"}}


@app.post("/api/session/{session_id}/levels/batch")
def api_levels_batch(session_id: str, payload: Dict[str, Any] = Body(...)):
    root = SESSION_STORE.ensure(session_id)
    src = payload.get("ifc_file")
    actions = payload.get("actions") or []
    if not src or not actions:
        raise HTTPException(status_code=400, detail="ifc_file and actions are required")
    in_path = os.path.join(root, sanitize_filename(src))
    if not os.path.isfile(in_path):
        raise HTTPException(status_code=404, detail="IFC file not found")
    base = os.path.splitext(os.path.basename(in_path))[0]
    work_path = os.path.join(root, f"{base}_levels_work.ifc")
    final_name = f"{base}_levels_batch.ifc"
    final_path = os.path.join(root, final_name)
    try:
        apply_level_actions(in_path, actions, work_path)
        shutil.copyfile(work_path, final_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ifc": {"name": final_name, "url": f"/api/session/{session_id}/download?name={final_name}"}, "actions": actions}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=7860)
