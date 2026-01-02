import datetime
import os
import re
import shutil
import tempfile
import traceback
import uuid
from typing import Any, Dict, List, Optional, Tuple

import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element
import pandas as pd
from fastapi import Body, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ifcopenshell.guid import new as new_guid


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
                    ensure_aggregates(site, building, ifc)
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
    return objs


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
        for rel in obj.ContainedInStructure or []:
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


def list_levels(ifc_path: str) -> Dict[str, Any]:
    model = ifcopenshell.open(ifc_path)
    result = []
    for st in model.by_type("IfcBuildingStorey"):
        objs = list_storey_objects(st)
        result.append(
            {
                "id": st.id(),
                "name": getattr(st, "Name", ""),
                "description": getattr(st, "Description", ""),
                "elevation": storey_elevation(st),
                "comp_height": storey_comp_height(st),
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
    if "elevation" in payload and payload.get("elevation") not in (None, ""):
        storey.Elevation = float(payload.get("elevation"))
    if "comp_height" in payload and hasattr(storey, "ElevationOfRefHeight"):
        comp = payload.get("comp_height")
        if comp not in (None, ""):
            storey.ElevationOfRefHeight = float(comp)
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
            for rel in obj.ContainedInStructure or []:
                if rel.is_a("IfcRelContainedInSpatialStructure"):
                    origin_storey = rel.RelatingStructure
                    break
            delta = 0.0
            if origin_storey:
                delta = storey_elevation(origin_storey) - storey_elevation(storey)
            adjust_local_placement_z(getattr(obj, "ObjectPlacement", None), delta)
            # remove from old relations
            for rel in obj.ContainedInStructure or []:
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


def rewrite_proxy_types(in_path: str, out_path: str) -> Tuple[str, str]:
    with open(in_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    enumlib = {}
    model = None
    try:
        model = ifcopenshell.open(in_path)
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
            if forced:
                enum_val = forced
            else:
                enum_val = enum_from_token(predef_raw, enum_set, enumlib)

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
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
            if forced:
                enum_val = forced
            else:
                enum_val = enum_from_token(predef_raw, enum_set, enumlib)

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["building_types_converted"] += 1

            typeid_to_occ_entity[type_id] = lib_entry["occ_entity"]
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
# FastAPI app + routes
# ----------------------------------------------------------------------------

app = FastAPI(title="IFC Toolkit Hub")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def startup_cleanup():
    SESSION_STORE.cleanup_stale()


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


@app.get("/storeys", response_class=HTMLResponse)
def storeys_page(request: Request):
    return templates.TemplateResponse("storeys.html", {"request": request, "active": "storeys"})


@app.get("/proxy", response_class=HTMLResponse)
def proxy_page(request: Request):
    return templates.TemplateResponse("proxy.html", {"request": request, "active": "proxy"})


@app.get("/files", response_class=HTMLResponse)
def files_page(request: Request):
    return templates.TemplateResponse("files.html", {"request": request, "active": "files"})


@app.get("/levels", response_class=HTMLResponse)
def levels_page(request: Request):
    return templates.TemplateResponse("levels.html", {"request": request, "active": "levels"})


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


@app.get("/api/session/{session_id}/download")
def download_file(session_id: str, name: str):
    root = SESSION_STORE.ensure(session_id)
    safe = sanitize_filename(os.path.basename(name))
    path = os.path.join(root, safe)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=safe)


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=7860)
