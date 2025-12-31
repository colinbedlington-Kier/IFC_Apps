import os
import re
import shutil
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from ifcopenshell.guid import new as new_guid
from pydantic import BaseModel, Field


# -----------------------------
# Session management
# -----------------------------
SESSION_ROOT = Path("sessions")
SESSION_ROOT.mkdir(exist_ok=True)
SESSION_TTL_SECONDS = 60 * 60  # 1 hour
SESSION_LOCK = threading.Lock()


@dataclass
class SessionFile:
    file_id: str
    name: str
    path: Path
    size: int
    created_at: float
    kind: Optional[str] = None


@dataclass
class SessionData:
    session_id: str
    root: Path
    files: Dict[str, SessionFile] = field(default_factory=dict)
    created_at: float = field(default_factory=lambda: time.time())
    updated_at: float = field(default_factory=lambda: time.time())
    closed: bool = False

    def add_file(self, path: Path, display_name: Optional[str] = None, kind: Optional[str] = None) -> SessionFile:
        display = display_name or path.name
        file_id = uuid4().hex
        size = path.stat().st_size if path.exists() else 0
        record = SessionFile(
            file_id=file_id,
            name=display,
            path=path,
            size=size,
            created_at=time.time(),
            kind=kind,
        )
        self.files[file_id] = record
        self.updated_at = time.time()
        return record


SESSIONS: Dict[str, SessionData] = {}


def sanitize_filename(base: str) -> str:
    cleaned = base.replace("\\", "_").replace("/", "_")
    for c in '<>:"|?*':
        cleaned = cleaned.replace(c, "_")
    return cleaned


def create_session() -> SessionData:
    session_id = uuid4().hex
    root = SESSION_ROOT / session_id
    root.mkdir(parents=True, exist_ok=True)
    session = SessionData(session_id=session_id, root=root)
    SESSIONS[session_id] = session
    return session


def get_session(session_id: Optional[str]) -> SessionData:
    with SESSION_LOCK:
        if session_id and session_id in SESSIONS:
            session = SESSIONS[session_id]
            if session.closed:
                del SESSIONS[session_id]
                return create_session()
            session.updated_at = time.time()
            return session
        return create_session()


def cleanup_session(session_id: str):
    session = SESSIONS.pop(session_id, None)
    if not session:
        return
    session.closed = True
    try:
        shutil.rmtree(session.root, ignore_errors=True)
    except Exception:
        pass


def cleanup_expired_sessions():
    now = time.time()
    expired = []
    with SESSION_LOCK:
        for sid, session in list(SESSIONS.items()):
            if session.closed:
                expired.append(sid)
                continue
            if now - session.updated_at > SESSION_TTL_SECONDS:
                expired.append(sid)
        for sid in expired:
            cleanup_session(sid)


# -----------------------------
# FastAPI app setup
# -----------------------------
app = FastAPI(title="IFC Toolkit Hub")
app.mount("/static", StaticFiles(directory="static", html=True), name="static")


@app.middleware("http")
async def session_middleware(request: Request, call_next):
    cleanup_expired_sessions()
    incoming = request.cookies.get("session_id")
    session = get_session(incoming)
    request.state.session = session
    response = await call_next(request)
    response.set_cookie(
        "session_id",
        session.session_id,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = Path("static/index.html")
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>IFC Toolkit Hub</h1>")


@app.get("/api/session")
async def session_info(request: Request):
    session: SessionData = request.state.session
    return {
        "session_id": session.session_id,
        "files": [serialize_file(f) for f in session.files.values()],
        "expires_in_seconds": SESSION_TTL_SECONDS,
    }


@app.post("/api/session/close")
async def close_session(request: Request):
    session: SessionData = request.state.session
    cleanup_session(session.session_id)
    return {"status": "closed"}


@app.post("/api/upload")
async def upload_files(request: Request, files: List[UploadFile] = File(...)):
    session: SessionData = request.state.session
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    saved = []
    for file in files:
        safe_name = sanitize_filename(file.filename or "upload.ifc")
        dest = session.root / safe_name
        with open(dest, "wb") as out:
            content = await file.read()
            out.write(content)
        record = session.add_file(dest)
        saved.append(serialize_file(record))
    return {"files": saved}


@app.get("/api/files")
async def list_files(request: Request):
    session: SessionData = request.state.session
    return {"files": [serialize_file(f) for f in session.files.values()]}


@app.get("/api/files/{file_id}")
async def download_file(file_id: str, request: Request):
    session: SessionData = request.state.session
    file = session.files.get(file_id)
    if not file or not file.path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=file.path, filename=file.name)


# -----------------------------
# Shared IFC helpers
# -----------------------------

def serialize_file(record: SessionFile) -> Dict[str, object]:
    return {
        "id": record.file_id,
        "name": record.name,
        "size": record.size,
        "created_at": record.created_at,
        "kind": record.kind,
    }


def locate_files(session: SessionData, ids: List[str]) -> List[SessionFile]:
    missing = [fid for fid in ids if fid not in session.files]
    if missing:
        raise HTTPException(status_code=404, detail=f"Missing files: {', '.join(missing)}")
    return [session.files[fid] for fid in ids]


# -----------------------------
# Tool 1: InfoDrainage property cleaner
# -----------------------------

def clean_ifc_file(
    in_path: str,
    out_path: str,
    prefix: str = "InfoDrainage",
    case_insensitive: bool = True,
    delete_psets_with_prefix: bool = True,
    delete_properties_in_other_psets: bool = True,
    drop_empty_psets: bool = True,
    also_remove_loose_props: bool = True,
) -> Dict[str, object]:
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


class CleanRequest(BaseModel):
    file_ids: List[str]
    prefix: str = "InfoDrainage"
    case_insensitive: bool = True
    delete_psets_with_prefix: bool = True
    delete_properties_in_other_psets: bool = True
    drop_empty_psets: bool = True
    also_remove_loose_props: bool = True


@app.post("/api/clean")
async def clean_ifc(request: Request, payload: CleanRequest):
    session: SessionData = request.state.session
    files = locate_files(session, payload.file_ids)
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    reports = []
    outputs = []
    for src in files:
        base, _ = os.path.splitext(src.name)
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_name = f"{base}_cleaned_{ts}.ifc"
        out_path = session.root / out_name
        report = clean_ifc_file(
            in_path=str(src.path),
            out_path=str(out_path),
            prefix=payload.prefix.strip() or "InfoDrainage",
            case_insensitive=payload.case_insensitive,
            delete_psets_with_prefix=payload.delete_psets_with_prefix,
            delete_properties_in_other_psets=payload.delete_properties_in_other_psets,
            drop_empty_psets=payload.drop_empty_psets,
            also_remove_loose_props=payload.also_remove_loose_props,
        )
        reports.append(report)
        record = session.add_file(out_path, display_name=out_name, kind="ifc")
        outputs.append(serialize_file(record))
    return {"reports": reports, "outputs": outputs}


# -----------------------------
# Tool 2: Excel extractor/updater
# -----------------------------
COBIE_MAPPING = {
    "COBie_Specification": {
        "scope": "T",
        "props": [
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
        ],
    },
    "COBie_Component": {
        "scope": "I",
        "props": [
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
        ],
    },
    "COBie_Asset": {"scope": "T", "props": [("AssetType", "Text")]},
    "COBie_Warranty": {
        "scope": "T",
        "props": [
            ("WarrantyDurationParts", "Real"),
            ("WarrantyGuarantorLabor", "Text"),
            ("WarrantyDurationLabor", "Real"),
            ("WarrantyDurationDescription", "Text"),
            ("WarrantyDurationUnit", "Text"),
            ("WarrantyGuarantorParts", "Text"),
        ],
    },
    "Pset_ManufacturerOccurence": {"scope": "I", "props": [("SerialNumber", "Text"), ("BarCode", "Text")]},
    "COBie_ServiceLife": {"scope": "T", "props": [("ServiceLifeDuration", "Real"), ("DurationUnit", "Text")]},
    "COBie_EconomicalImpactValues": {"scope": "T", "props": [("ReplacementCost", "Real")]},
    "COBie_Type": {
        "scope": "T",
        "props": [
            ("COBie", "Boolean"),
            ("CreatedBy", "Text"),
            ("CreatedOn", "Text"),
            ("Name", "Text"),
            ("Description", "Text"),
            ("Category", "Text"),
            ("Area", "Area"),
            ("Length", "Length"),
        ],
    },
    "COBie_System": {"scope": "I", "props": [("Name", "Text"), ("Description", "Text"), ("Category", "Text")]},
    "Classification_General": {
        "scope": "T",
        "props": [
            ("Classification.Uniclass.Pr.Number", "Text"),
            ("Classification.Uniclass.Pr.Description", "Text"),
            ("Classification.Uniclass.Ss.Number", "Text"),
            ("Classification.Uniclass.Ss.Description", "Text"),
            ("Classification.NRM1.Number", "Text"),
            ("Classification.NRM1.Description", "Text"),
        ],
    },
    "Pset_ManufacturerTypeInformation": {"scope": "T", "props": [("Manufacturer", "Text"), ("ModelNumber", "Text"), ("ModelReference", "Text")]},
    "PPset_DoorCommon": {"scope": "T", "props": [("FireRating", "Text")]},
    "Pset_BuildingCommon": {"scope": "T", "props": [("NumberOfStoreys", "Text")]},
    "COBie_Space": {"scope": "T", "props": [("RoomTag", "Text")]},
    "COBie_BuildingCommon_UK": {"scope": "T", "props": [("UPRN", "Text")]},
    "Additional_Pset_BuildingCommon": {"scope": "T", "props": [("BlockConstructionType", "Text"), ("MaximumBlockHeight", "Text")]},
    "Additional_Pset_SystemCommon": {"scope": "T", "props": [("SystemCategory", "Text"), ("SystemDescription", "Text"), ("SystemName", "Text")]},
}


RE_SPLIT_LIST = re.compile(r"[;,|\n]+|\s{2,}")


def clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
    return v


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


def extract_to_excel(ifc_file: str, output_path: str) -> str:
    ifc = ifcopenshell.open(ifc_file)

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
            elem_desc,
        ])
    elements_df = pd.DataFrame(
        element_data,
        columns=["GlobalId", "Class", "OccurrenceName", "OccurrenceType", "TypeName", "TypeDescription"],
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
            "IFCElementType.Name": type_name,
        }

        for pset, pname in all_pairs:
            key = f"{pset}.{pname}"
            row[key] = get_pset_value(elem, pset, pname)

        cobie_rows.append(row)

    cobie_df = pd.DataFrame(cobie_rows, columns=cobie_cols)

    def extract_uniclass(elem, target_name, is_ifc2x3):
        reference = ""
        name = ""
        for rel in elem.HasAssociations or []:
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


def ensure_aggregates(parent, child, ifc):
    rel = None
    for r in parent.IsDecomposedBy or []:
        if r.is_a("IfcRelAggregates"):
            rel = r
            break
    if rel is None:
        ifc.create_entity(
            "IfcRelAggregates",
            GlobalId=ifcopenshell.guid.new(),
            RelatingObject=parent,
            RelatedObjects=[child],
        )
    else:
        if child not in rel.RelatedObjects:
            rel.RelatedObjects = list(rel.RelatedObjects) + [child]


def update_ifc_from_excel(ifc_file: str, excel_file: str, output_path: str, update_mode: str = "update", add_new: str = "no") -> str:
    ifc = ifcopenshell.open(ifc_file)
    xls = pd.ExcelFile(excel_file)
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
                site = ifc.create_entity("IfcSite", GlobalId=ifcopenshell.guid.new(), Name=name or "Site")
            if site is not None:
                if name is not None:
                    site.Name = name
                if desc is not None:
                    site.Description = desc
                ensure_aggregates(project, site, ifc)
                if building is not None:
                    ensure_aggregates(site, building, ifc)
        elif dt == "Building":
            if building is None and add_new == "yes":
                building = ifc.create_entity(
                    "IfcBuilding",
                    GlobalId=ifcopenshell.guid.new(),
                    Name=clean_value(row.get("Name")) or "Building",
                )
            if building is not None:
                if pd.notna(row.get("Name")):
                    building.Name = clean_value(row["Name"]) or building.Name
                if pd.notna(row.get("Description")):
                    building.Description = clean_value(row["Description"]) or building.Description
                if site is not None:
                    ensure_aggregates(site, building, ifc)
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
                        GlobalId=ifcopenshell.guid.new(),
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
            for rel in elem.HasAssociations or []:
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
                    GlobalId=ifcopenshell.guid.new(),
                    RelatedObjects=[elem],
                    RelatingClassification=cref,
                )

    set_uniclass(uniclass_pr_df, "Uniclass Pr Products")
    set_uniclass(uniclass_ss_df, "Uniclass Ss Systems")

    ifc.write(output_path)
    return output_path


class ExtractRequest(BaseModel):
    file_id: str


class UpdateRequest(BaseModel):
    ifc_file_id: str
    excel_file_id: str
    update_mode: str = Field(default="update")
    add_new: str = Field(default="no")


@app.post("/api/extract")
async def extract_excel(request: Request, payload: ExtractRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.file_id])[0]
    base, _ = os.path.splitext(src.name)
    out_path = session.root / f"{base}_extracted.xlsx"
    extract_to_excel(str(src.path), str(out_path))
    record = session.add_file(out_path, display_name=out_path.name, kind="excel")
    return {"file": serialize_file(record)}


@app.post("/api/update")
async def update_from_excel(request: Request, payload: UpdateRequest):
    session: SessionData = request.state.session
    ifc_file = locate_files(session, [payload.ifc_file_id])[0]
    excel_file = locate_files(session, [payload.excel_file_id])[0]
    base, _ = os.path.splitext(ifc_file.name)
    out_path = session.root / f"{base}_updated.ifc"
    update_ifc_from_excel(str(ifc_file.path), str(excel_file.path), str(out_path), payload.update_mode, payload.add_new)
    record = session.add_file(out_path, display_name=out_path.name, kind="ifc")
    return {"file": serialize_file(record)}


# -----------------------------
# Tool 3: Global Z & BaseQuantities
# -----------------------------


def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sanitize_label(base: str) -> str:
    return sanitize_filename(base)


_SI_PREFIX_TO_M = {None: 1.0, "MILLI": 1e-3, "CENTI": 1e-2, "DECI": 1e-1, "KILO": 1e3}


def model_length_unit_in_m(model) -> float:
    try:
        projs = model.by_type("IfcProject")
        if not projs:
            return 1.0
        ua = getattr(projs[0], "UnitsInContext", None)
        if not ua:
            return 1.0
        for u in ua.Units or []:
            if u.is_a("IfcSIUnit") and getattr(u, "UnitType", None) == "LENGTHUNIT":
                if getattr(u, "Name", None) == "METRE":
                    pref = getattr(u, "Prefix", None)
                    return _SI_PREFIX_TO_M.get(pref, 1.0)
            if u.is_a("IfcConversionBasedUnit") and getattr(u, "UnitType", None) == "LENGTHUNIT":
                mu = getattr(u, "ConversionFactor", None)
                try:
                    v = getattr(mu, "ValueComponent", None)
                    if v is not None:
                        return float(v)
                except Exception:
                    pass
        return 1.0
    except Exception:
        return 1.0


def to_model_units_length(value, input_unit_code, model) -> float:
    if value in (None, ""):
        return None
    val_m = float(value) if input_unit_code == "m" else float(value) / 1000.0
    mu = model_length_unit_in_m(model)
    if mu <= 0:
        mu = 1.0
    return val_m / mu


def ui_to_meters(value, units_code) -> float:
    if value in (None, ""):
        return None
    return float(value) if units_code == "m" else float(value) / 1000.0


def meters_to_model_units(val_m, model) -> float:
    mu = model_length_unit_in_m(model)
    if mu <= 0:
        mu = 1.0
    return float(val_m) / mu


def get_first_owner_history(model):
    oh = model.by_type("IfcOwnerHistory")
    return oh[0] if oh else None


def find_storeys(model):
    rows = []
    for s in model.by_type("IfcBuildingStorey"):
        name = getattr(s, "Name", "") or ""
        elev = getattr(s, "Elevation", None)
        label = f"#{s.id()} • {name or '(no name)'} • Elev={elev if isinstance(elev,(int,float)) else '—'}"
        rows.append((s.id(), label, s, elev))
    rows.sort(key=lambda r: (r[3] if isinstance(r[3], (int, float)) else float("inf"), r[1]))
    return rows


def get_existing_elq(model, storey):
    for rel in model.by_type("IfcRelDefinesByProperties"):
        try:
            if storey in (rel.RelatedObjects or ()):
                elq = rel.RelatingPropertyDefinition
                if elq and elq.is_a("IfcElementQuantity") and elq.Name == "BaseQuantities":
                    return elq, rel
        except Exception:
            pass
    return None, None


def find_qtylength(elq, name):
    if not elq:
        return None
    for q in elq.Quantities or []:
        if q.is_a("IfcQuantityLength") and q.Name == name:
            return q
    return None


def ensure_qtylength(model, elq, name, value_model_units, description=None):
    q = find_qtylength(elq, name)
    if q:
        q.LengthValue = float(value_model_units)
        if description is not None:
            q.Description = description
        return q
    q = model.create_entity(
        "IfcQuantityLength",
        Name=name,
        Description=description if description else None,
        Unit=None,
        LengthValue=float(value_model_units),
    )
    elq.Quantities = tuple((elq.Quantities or ())) + (q,)
    return q


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
    elq, rel = get_existing_elq(model, storey)

    if elq is None:
        if model.schema.lower().startswith("ifc2x"):
            elq = model.create_entity(
                "IfcElementQuantity",
                GlobalId=new_guid(),
                OwnerHistory=owner_history,
                Name="BaseQuantities",
                Quantities=(),
            )
            rel = model.create_entity(
                "IfcRelDefinesByProperties",
                GlobalId=new_guid(),
                OwnerHistory=owner_history,
                RelatedObjects=(storey,),
                RelatingPropertyDefinition=elq,
            )
        else:
            elq = model.create_entity(
                "IfcElementQuantity",
                GlobalId=new_guid(),
                Name="BaseQuantities",
                Quantities=(),
            )
            rel = model.create_entity(
                "IfcRelDefinesByProperties",
                GlobalId=new_guid(),
                RelatedObjects=(storey,),
                RelatingPropertyDefinition=elq,
            )

    if gross_val_ui is not None:
        gross_model = to_model_units_length(gross_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "GrossHeight", gross_model)
    if net_val_ui is not None:
        net_model = to_model_units_length(net_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "NetHeight", net_model)

    if mirror_to_qto:
        qto_elq = None
        for rel2 in model.by_type("IfcRelDefinesByProperties"):
            try:
                if storey in (rel2.RelatedObjects or (())):
                    elq2 = rel2.RelatingPropertyDefinition
                    if elq2 and elq2.is_a("IfcElementQuantity") and elq2.Name == "Qto_BuildingStoreyBaseQuantities":
                        qto_elq = elq2
                        break
            except Exception:
                pass
        if qto_elq is None:
            owner_history = get_first_owner_history(model)
            if model.schema.lower().startswith("ifc2x"):
                qto_elq = model.create_entity(
                    "IfcElementQuantity",
                    GlobalId=new_guid(),
                    OwnerHistory=owner_history,
                    Name="Qto_BuildingStoreyBaseQuantities",
                    Quantities=(),
                )
                model.create_entity(
                    "IfcRelDefinesByProperties",
                    GlobalId=new_guid(),
                    OwnerHistory=owner_history,
                    RelatedObjects=(storey,),
                    RelatingPropertyDefinition=qto_elq,
                )
            else:
                qto_elq = model.create_entity(
                    "IfcElementQuantity",
                    GlobalId=new_guid(),
                    Name="Qto_BuildingStoreyBaseQuantities",
                    Quantities=(),
                )
                model.create_entity(
                    "IfcRelDefinesByProperties",
                    GlobalId=new_guid(),
                    RelatedObjects=(storey,),
                    RelatingPropertyDefinition=qto_elq,
                )
        if gross_val_ui is not None:
            ensure_qtylength(model, qto_elq, "GrossHeight", to_model_units_length(gross_val_ui, input_unit_code, model))
        if net_val_ui is not None:
            ensure_qtylength(model, qto_elq, "NetHeight", to_model_units_length(net_val_ui, input_unit_code, model))

    return elq, rel


def ascend_to_root_local_placement(lp):
    cur = lp
    guard = 0
    while cur and getattr(cur, "PlacementRelTo", None) is not None and guard < 200:
        cur = cur.PlacementRelTo
        guard += 1
    return cur


def get_location_cartesian_point(lp):
    if not lp:
        return None
    rel = getattr(lp, "RelativePlacement", None)
    if not rel or not rel.is_a("IfcAxis2Placement3D"):
        return None
    loc = getattr(rel, "Location", None)
    if loc and loc.is_a("IfcCartesianPoint"):
        return loc
    return None


def get_all_map_conversions(model):
    if model.schema == "IFC2X3":
        return []

    seen, out = set(), []
    for mc in model.by_type("IfcMapConversion") or []:
        try:
            if mc and mc.id() not in seen:
                out.append(mc)
                seen.add(mc.id())
        except Exception:
            pass
    for ctx in model.by_type("IfcGeometricRepresentationContext") or []:
        try:
            ops = getattr(ctx, "HasCoordinateOperation", None)
            if not ops:
                continue
            iterable = ops if isinstance(ops, (list, tuple)) else [ops]
            for op in iterable:
                if op and op.is_a("IfcMapConversion") and op.id() not in seen:
                    out.append(op)
                    seen.add(op.id())
        except Exception:
            pass
    return out


def countershift_product_local_points(model, delta_model):
    c = 0
    for prod in model.by_type("IfcProduct"):
        if prod.is_a("IfcProject") or prod.is_a("IfcSite") or prod.is_a("IfcBuilding") or prod.is_a("IfcBuildingStorey") or prod.is_a("IfcSpace"):
            continue

        lp = getattr(prod, "ObjectPlacement", None)
        if not lp:
            continue
        rel = getattr(lp, "RelativePlacement", None)
        if not (rel and rel.is_a("IfcAxis2Placement3D")):
            continue
        loc = getattr(rel, "Location", None)
        if not (loc and loc.is_a("IfcCartesianPoint")):
            continue

        coords = list(loc.Coordinates)
        if len(coords) >= 3 and coords[2] is not None:
            try:
                new_z = float(coords[2]) - float(delta_model)
                new_pt = model.create_entity(
                    "IfcCartesianPoint",
                    Coordinates=(
                        float(coords[0]) if coords[0] is not None else 0.0,
                        float(coords[1]) if coords[1] is not None else 0.0,
                        new_z,
                    ),
                )
                rel.Location = new_pt
                c += 1
            except Exception:
                pass
    return c


def describe_storeys(ifc_path: str) -> List[Dict[str, object]]:
    model = ifcopenshell.open(ifc_path)
    storeys = find_storeys(model)
    return [
        {
            "id": sid,
            "label": lbl,
            "elevation": elev,
            "name": getattr(storey, "Name", ""),
            "guid": getattr(storey, "GlobalId", ""),
        }
        for sid, lbl, storey, elev in storeys
    ]


def apply_global_adjustment(
    ifc_path: str,
    storey_id: int,
    units_code: str,
    gross_val,
    net_val,
    mom_txt: Optional[str],
    mirror: bool,
    target_z,
    countershift_geometry: bool,
    use_crs_mode: bool,
    update_all_mcs: bool,
    show_diag: bool,
    crs_set_storey_elev: bool,
    output_path: Path,
) -> Tuple[str, Path]:
    model = ifcopenshell.open(ifc_path)
    storey = model.by_id(int(storey_id)) if storey_id else None
    if not storey:
        raise HTTPException(status_code=400, detail="Storey not found")

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
    diag_lines = []

    mc_list = []
    if use_crs_mode and target_z not in (None, ""):
        all_mcs = get_all_map_conversions(model)
        if all_mcs:
            mc_list = all_mcs if update_all_mcs else [all_mcs[0]]

    if mc_list:
        new_m = ui_to_meters(target_z, units_code)
        old_m_first = float(getattr(mc_list[0], "OrthogonalHeight", 0.0) or 0.0)
        delta_m = new_m - old_m_first
        for idx, mc in enumerate(mc_list):
            old_m = float(getattr(mc, "OrthogonalHeight", 0.0) or 0.0)
            mc.OrthogonalHeight = float(new_m)
            if show_diag:
                diag_lines.append(f"CRS[{idx}] {old_m} m → {new_m} m (Δ={new_m-old_m} m)")
        if crs_set_storey_elev:
            target_mu = to_model_units_length(target_z, units_code, model)
            old_storey_elev = float(getattr(storey, "Elevation", 0.0) or 0.0)
            delta_model = meters_to_model_units(delta_m, model)
            storey.Elevation = float(target_mu)
            if show_diag:
                diag_lines.append(
                    f"Storey.Elevation (CRS mode ABS) {old_storey_elev} mu → {storey.Elevation} mu (target_mu={target_mu} mu, Δ={delta_model} mu)"
                )
        else:
            delta_model = meters_to_model_units(delta_m, model)
        used_path = "crs-mapconversion(all)" if (update_all_mcs and len(mc_list) > 1) else "crs-mapconversion"
    else:
        if target_z not in (None, ""):
            root_lp = ascend_to_root_local_placement(storey.ObjectPlacement)
            root_pt = get_location_cartesian_point(root_lp)
            if root_pt is None:
                raise HTTPException(status_code=400, detail="Could not find root CartesianPoint for storey placement")
            coords = list(root_pt.Coordinates)
            if len(coords) < 3:
                raise HTTPException(status_code=400, detail="Root CartesianPoint has no Z coordinate")
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
        site = (model.by_type('IfcSite') or [None])[0]
        site_ref = float(getattr(site, 'RefElevation', 0.0) or 0.0) if site else 0.0
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
    parts.append(f"Output: {output_path.name}")
    return "\n".join([p for p in parts if p]), output_path


class StoreyRequest(BaseModel):
    file_id: str


class GlobalAdjustRequest(BaseModel):
    ifc_file_id: str
    storey_id: int
    units_code: str = "m"
    gross: Optional[float] = None
    net: Optional[float] = None
    mom: Optional[str] = None
    mirror: bool = False
    target_z: Optional[float] = None
    countershift: bool = True
    crs_mode: bool = True
    update_all_mcs: bool = True
    show_diag: bool = True
    crs_set_storey_elev: bool = True


@app.post("/api/storeys")
async def list_storeys(request: Request, payload: StoreyRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.file_id])[0]
    return {"storeys": describe_storeys(str(src.path))}


@app.post("/api/global-z")
async def apply_global(request: Request, payload: GlobalAdjustRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.ifc_file_id])[0]
    base, _ = os.path.splitext(src.name)
    out_path = session.root / f"{base}_gsb_adjusted.ifc"
    summary, new_path = apply_global_adjustment(
        ifc_path=str(src.path),
        storey_id=payload.storey_id,
        units_code=payload.units_code,
        gross_val=payload.gross,
        net_val=payload.net,
        mom_txt=payload.mom,
        mirror=payload.mirror,
        target_z=payload.target_z,
        countershift_geometry=payload.countershift,
        use_crs_mode=payload.crs_mode,
        update_all_mcs=payload.update_all_mcs,
        show_diag=payload.show_diag,
        crs_set_storey_elev=payload.crs_set_storey_elev,
        output_path=out_path,
    )
    record = session.add_file(new_path, display_name=new_path.name, kind="ifc")
    return {"file": serialize_file(record), "summary": summary}


# -----------------------------
# Tool 4: Proxy to type mapper
# -----------------------------
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

TYPE_LIBRARY = {
    "waste terminal": {
        "type_entity": "IFCWASTETERMINALTYPE",
        "enum_set": "IfcWasteTerminalTypeEnum",
        "occ_entity": "IFCFLOWTERMINAL",
    },
    "distribution chamber element": {
        "type_entity": "IFCDISTRIBUTIONCHAMBERELEMENTTYPE",
        "enum_set": "IfcDistributionChamberElementTypeEnum",
        "occ_entity": "IFCDISTRIBUTIONCHAMBERELEMENT",
    },
    "pipe segment": {
        "type_entity": "IFCPIPESEGMENTTYPE",
        "enum_set": "IfcPipeSegmentTypeEnum",
        "occ_entity": "IFCFLOWSEGMENT",
    },
    "pipe": {
        "type_entity": "IFCPIPESEGMENTTYPE",
        "enum_set": "IfcPipeSegmentTypeEnum",
        "occ_entity": "IFCFLOWSEGMENT",
    },
    "tank": {
        "type_entity": "IFCTANKTYPE",
        "enum_set": "IfcTankTypeEnum",
        "occ_entity": "IFCFLOWSTORAGEDEVICE",
    },
}

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


def parse_type_tokens(type_name: str):
    parts = type_name.split("_")
    class_token = parts[0].strip().lower() if parts else ""
    predef_raw = parts[1].strip() if len(parts) > 1 else ""
    return class_token, predef_raw


def enum_from_token(raw: str, enum_set: str, enumlib: dict) -> str:
    if not raw:
        return "USERDEFINED"
    candidate = raw.replace(" ", "").upper()
    values = enumlib.get(enum_set, set())
    return candidate if candidate in values else "USERDEFINED"


def process_ifc_proxy(input_path: str, output_path: str) -> Tuple[str, str]:
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    enumlib = {}
    try:
        model = ifcopenshell.open(input_path)
        enumlib = build_enum_library(model)
    except Exception:
        enumlib = FALLBACK_ENUM_LIBRARY.copy()

    if not enumlib:
        enumlib = FALLBACK_ENUM_LIBRARY.copy()

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

            class_token, predef_raw = parse_type_tokens(type_name)
            class_norm = class_token.lower()

            lib_entry = None
            if class_norm in TYPE_LIBRARY:
                lib_entry = TYPE_LIBRARY[class_norm]
            elif "pipe" in type_name.lower():
                lib_entry = TYPE_LIBRARY["pipe"]

            if not lib_entry:
                stats["left_as_proxy_type"] += 1
                updated_lines.append(line)
                continue

            target_type = lib_entry["type_entity"]
            enum_set = lib_entry["enum_set"]

            forced = FORCED_PREDEFINED.get(target_type.lower())
            enum_val = forced if forced else enum_from_token(predef_raw, enum_set, enumlib)

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner}," f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["proxy_types_converted"] += 1

            typeid_to_occ_entity[type_id] = lib_entry["occ_entity"]
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

            class_token, predef_raw = parse_type_tokens(type_name)
            class_norm = class_token.lower()

            lib_entry = None
            if class_norm in TYPE_LIBRARY:
                lib_entry = TYPE_LIBRARY[class_norm]
            elif "pipe" in type_name.lower():
                lib_entry = TYPE_LIBRARY["pipe"]

            if not lib_entry:
                stats["left_as_building_type"] += 1
                updated_lines.append(line)
                continue

            target_type = lib_entry["type_entity"]
            enum_set = lib_entry["enum_set"]

            forced = FORCED_PREDEFINED.get(target_type.lower())
            enum_val = forced if forced else enum_from_token(predef_raw, enum_set, enumlib)

            new_line = f"{ws}{type_id}={target_type}('{guid}',{owner}," f"'{type_name}',{mid},.{enum_val}.);"
            updated_lines.append(new_line)
            stats["building_types_converted"] += 1

            typeid_to_occ_entity[type_id] = lib_entry["occ_entity"]
            continue

        updated_lines.append(line)

    occid_to_entity = {}

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

    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(final_lines)

    summary = (
        f"Input file:  {os.path.basename(input_path)}\n"
        f"Output file: {os.path.basename(output_path)}\n\n"
        f"Proxy types (IFCBUILDINGELEMENTPROXYTYPE) found: {stats['proxy_types_total']}\n"
        f"  → converted to specific IFC types: {stats['proxy_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTPROXYTYPE: {stats['left_as_proxy_type']}\n\n"
        f"Building types (IFCBUILDINGELEMENTTYPE) found: {stats['building_types_total']}\n"
        f"  → converted to specific IFC types: {stats['building_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTTYPE: {stats['left_as_building_type']}\n\n"
        f"Occurrences converted from IfcBuildingElementProxy to typed entities: {stats['occurrences_converted']}\n\n"
        "Occurrences are retyped only when an IfcRelDefinesByType exists and the referenced type could be mapped. "
        "Mapping is IFC2x3-compliant: waste terminals → IfcFlowTerminal/IfcWasteTerminalType, pipe segments → "
        "IfcFlowSegment/IfcPipeSegmentType, tanks → IfcFlowStorageDevice/IfcTankType, distribution chambers → "
        "IfcDistributionChamberElement/IfcDistributionChamberElementType.\n"
    )

    return output_path, summary


class ProxyMapRequest(BaseModel):
    file_id: str


@app.post("/api/proxy-map")
async def proxy_map(request: Request, payload: ProxyMapRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.file_id])[0]
    base, ext = os.path.splitext(src.name)
    out_path = session.root / f"{base}_typed{ext}"
    output_path, summary = process_ifc_proxy(str(src.path), str(out_path))
    record = session.add_file(Path(output_path), display_name=Path(output_path).name, kind="ifc")
    return {"file": serialize_file(record), "summary": summary}


# -----------------------------
# Tool 5: Level manager (list/add/move/delete)
# -----------------------------


def list_storeys_with_counts(ifc_path: str) -> List[Dict[str, object]]:
    model = ifcopenshell.open(ifc_path)
    storey_lookup = {s.id(): s for s in model.by_type("IfcBuildingStorey")}
    counts = {sid: 0 for sid in storey_lookup}
    for rel in model.by_type("IfcRelContainedInSpatialStructure"):
        try:
            sid = getattr(rel.RelatingStructure, "id", lambda: None)()
            if sid in counts:
                counts[sid] += len(rel.RelatedElements or [])
        except Exception:
            continue

    rows = []
    for s in storey_lookup.values():
        elev = getattr(s, "Elevation", None)
        rows.append(
            {
                "id": s.id(),
                "guid": getattr(s, "GlobalId", ""),
                "name": getattr(s, "Name", ""),
                "elevation": elev,
                "element_count": counts.get(s.id(), 0),
                "label": f"#{s.id()} • {getattr(s, 'Name', '') or '(no name)'} • Elev={elev if isinstance(elev,(int,float)) else '—'}",
            }
        )
    rows.sort(key=lambda r: (r["elevation"] if isinstance(r["elevation"], (int, float)) else float("inf"), r["name"]))
    return rows


def select_container(model):
    building = (model.by_type("IfcBuilding") or [None])[0]
    if building:
        return building
    site = (model.by_type("IfcSite") or [None])[0]
    if site:
        return site
    return (model.by_type("IfcProject") or [None])[0]


def create_storey_with_placement(model, name: str, elevation_ui: Optional[float], units_code: str) -> object:
    container = select_container(model)
    elevation_mu = to_model_units_length(elevation_ui, units_code, model) if elevation_ui not in (None, "") else None
    base_placement = getattr(container, "ObjectPlacement", None) if container else None

    loc = model.create_entity(
        "IfcCartesianPoint",
        Coordinates=(
            0.0,
            0.0,
            float(elevation_mu) if elevation_mu is not None else 0.0,
        ),
    )
    axis = model.create_entity("IfcAxis2Placement3D", Location=loc)
    lp_kwargs = {"RelativePlacement": axis}
    if base_placement is not None:
        lp_kwargs["PlacementRelTo"] = base_placement
    local_placement = model.create_entity("IfcLocalPlacement", **lp_kwargs)

    storey = model.create_entity(
        "IfcBuildingStorey",
        GlobalId=new_guid(),
        Name=name,
        Elevation=float(elevation_mu) if elevation_mu is not None else None,
        ObjectPlacement=local_placement,
    )
    if container is not None:
        ensure_aggregates(container, storey, model)
    return storey


def reassign_containment(model, source_storey, target_storey) -> int:
    moved = 0
    for rel in model.by_type("IfcRelContainedInSpatialStructure"):
        try:
            if rel.RelatingStructure == source_storey:
                rel.RelatingStructure = target_storey
                moved += len(rel.RelatedElements or [])
        except Exception:
            continue
    return moved


def delete_storey_and_reassign(model, storey, target_storey) -> None:
    reassign_containment(model, storey, target_storey)

    # Remove from aggregates
    for rel in list(model.by_type("IfcRelAggregates") or []):
        try:
            if storey in (rel.RelatedObjects or []):
                new_related = [ro for ro in rel.RelatedObjects if ro != storey]
                if new_related:
                    rel.RelatedObjects = new_related
                else:
                    model.remove(rel)
        except Exception:
            continue

    try:
        model.remove(storey)
    except Exception:
        pass


class LevelListRequest(BaseModel):
    file_id: str


class LevelAddRequest(BaseModel):
    ifc_file_id: str
    name: str
    elevation: Optional[float] = None
    units_code: str = "m"


class LevelMoveRequest(BaseModel):
    ifc_file_id: str
    source_storey_id: int
    target_storey_id: int


class LevelDeleteRequest(BaseModel):
    ifc_file_id: str
    delete_storey_id: int
    target_storey_id: int


@app.post("/api/levels/list")
async def list_levels(request: Request, payload: LevelListRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.file_id])[0]
    return {"levels": list_storeys_with_counts(str(src.path))}


@app.post("/api/levels/add")
async def add_level(request: Request, payload: LevelAddRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.ifc_file_id])[0]
    model = ifcopenshell.open(str(src.path))
    storey = create_storey_with_placement(model, payload.name, payload.elevation, payload.units_code)
    base, _ = os.path.splitext(src.name)
    out_path = session.root / f"{base}_with_level.ifc"
    model.write(out_path)
    record = session.add_file(out_path, display_name=out_path.name, kind="ifc")
    return {
        "file": serialize_file(record),
        "created_level": {"id": storey.id(), "guid": getattr(storey, 'GlobalId', ''), "name": getattr(storey, 'Name', ''), "elevation": getattr(storey, 'Elevation', None)},
        "levels": list_storeys_with_counts(str(out_path)),
    }


@app.post("/api/levels/move")
async def move_level_elements(request: Request, payload: LevelMoveRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.ifc_file_id])[0]
    model = ifcopenshell.open(str(src.path))
    source = model.by_id(payload.source_storey_id)
    target = model.by_id(payload.target_storey_id)
    if not source or not target:
        raise HTTPException(status_code=404, detail="Source or target storey not found")
    moved = reassign_containment(model, source, target)
    base, _ = os.path.splitext(src.name)
    out_path = session.root / f"{base}_moved.ifc"
    model.write(out_path)
    record = session.add_file(out_path, display_name=out_path.name, kind="ifc")
    return {"file": serialize_file(record), "moved_count": moved, "levels": list_storeys_with_counts(str(out_path))}


@app.post("/api/levels/delete")
async def delete_level(request: Request, payload: LevelDeleteRequest):
    session: SessionData = request.state.session
    src = locate_files(session, [payload.ifc_file_id])[0]
    model = ifcopenshell.open(str(src.path))
    delete_storey = model.by_id(payload.delete_storey_id)
    target_storey = model.by_id(payload.target_storey_id)
    if not delete_storey or not target_storey:
        raise HTTPException(status_code=404, detail="Storey not found")
    delete_storey_and_reassign(model, delete_storey, target_storey)
    base, _ = os.path.splitext(src.name)
    out_path = session.root / f"{base}_level_deleted.ifc"
    model.write(out_path)
    record = session.add_file(out_path, display_name=out_path.name, kind="ifc")
    return {"file": serialize_file(record), "levels": list_storeys_with_counts(str(out_path))}


# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=7860, reload=False)
