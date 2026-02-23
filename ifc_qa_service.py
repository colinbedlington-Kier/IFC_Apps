import csv
import datetime as dt
import io
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ifcopenshell
import ifcopenshell.util.element
import ifcopenshell.util.placement

BASE_OUTPUT = [
    "IFC Models",
    "IFC Classification",
    "IFC Object Type",
    "IFC Project",
    "IFC Properties",
    "IFC Pset Template",
    "IFC Spatial Structure",
    "IFC System",
]

HEADERS = {
    "model": ["Source_Path", "Source_File", "File_Codes", "Model_Schema_Status", "Date_Checked"],
    "classification": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Type", "Classification_Occurrence_Type", "Classification_Type", "Classification_Value"],
    "object": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Description", "IFC_ObjectType", "IFC_Type", "IFC_Type_Name", "IFC_Predefined_Type", "IFC_Layer", "IFC_Tag", "IFC_Name_Syntax_Check", "IFC_Name_Short_Code", "IFC_Type_Syntax_Check", "IFC_Name_Type_Code", "IFC_Layer_Syntax_Check", "IFC_Name_Duplicate", "IFC_LongName", "IFC_Type_Line_Ref", "IFC_Type_Description", "IFC_TypeId", "Coordinates_xyz"],
    "project": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Description", "IFC_LongName", "Child_Entities"],
    "properties": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Description", "IFC_Tag", "Property_Occurence_Type", "Property_Set", "Property_Name", "Property_Value"],
    "pset_template": ["IFC_Line_Ref_Template", "Source_File_Template", "IFC_Name_Template", "IFC_GlobalId_Template", "IFC_Entity_Occurrence_Type", "IFC_Tag_Template", "Property_Set_Template", "Property_Name_Template", "Pset_Validation"],
    "spatial": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Description", "IFC_Tag", "Spatial_Structure", "Building_Storey", "ContainedIn_Tag", "Spatial_Structure_Entity", "Assigns_To_Group"],
    "system": ["IFC_Line_Ref", "Source_File", "IFC_Name", "IFC_GlobalId", "IFC_Entity", "IFC_Description", "IFC_Tag", "Property_Set", "Property_Name", "Property_Value", "IFC_SystemName_Syntax_Check", "IFC_SystemName_Code"],
}

class IfcQaRegistry:
    def __init__(self, ttl_hours: int = 24):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.ttl_seconds = ttl_hours * 3600

    def create(self) -> str:
        with self.lock:
            job_id = uuid.uuid4().hex
            self.jobs[job_id] = {"status": "queued", "percent": 0, "logs": [], "files": [], "created": time.time(), "currentStep": "Queued"}
            return job_id

    def update(self, job_id: str, **kwargs: Any) -> None:
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].update(kwargs)

    def append_log(self, job_id: str, message: str) -> None:
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            logs = job.setdefault("logs", [])
            logs.append(message)
            if len(logs) > 400:
                del logs[:-400]

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        self.cleanup()
        with self.lock:
            return self.jobs.get(job_id)

    def cleanup(self) -> None:
        now = time.time()
        with self.lock:
            stale = [j for j, data in self.jobs.items() if now - data.get("created", now) > self.ttl_seconds]
            for job_id in stale:
                result = self.jobs[job_id].get("result_path")
                workdir = self.jobs[job_id].get("workdir")
                if result and os.path.exists(result):
                    try: os.remove(result)
                    except OSError: pass
                if workdir and os.path.exists(workdir):
                    shutil.rmtree(workdir, ignore_errors=True)
                self.jobs.pop(job_id, None)

REGISTRY = IfcQaRegistry()


def _str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return json.dumps(list(v))
    return str(v)


def _line_ref(entity: Any) -> str:
    return f"#{entity.id()}=" if entity is not None else ""


def _step_line_map(ifc_path: Path) -> Dict[int, str]:
    data: Dict[int, str] = {}
    with open(ifc_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line.startswith("#") or "=" not in line:
                continue
            try:
                left = line.split("=", 1)[0]
                idx = int(left[1:])
                data[idx] = line
            except Exception:
                continue
    return data


def _object_layer_name(obj: Any) -> str:
    try:
        layers = ifcopenshell.util.element.get_layers(obj) or []
    except Exception:
        layers = []
    for lyr in layers:
        name = getattr(lyr, "Name", "") or ""
        if name:
            return name
    return ""


def _coords(obj: Any) -> str:
    placement = getattr(obj, "ObjectPlacement", None)
    if not placement:
        return ""
    try:
        m = ifcopenshell.util.placement.get_local_placement(placement)
        return f"[{float(m[0][3]):.2f}, {float(m[1][3]):.2f}, {float(m[2][3]):.2f}]"
    except Exception:
        return ""


def _write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([_str(x) for x in r])


def _get_targets(model: ifcopenshell.file) -> List[Any]:
    out: List[Any] = []
    for p in model.by_type("IfcProduct"):
        out.append(p)
    for t in ["IfcSpace", "IfcBuildingStorey"]:
        for p in model.by_type(t):
            if p not in out:
                out.append(p)
    return out


def _type_for_obj(obj: Any) -> Any:
    try:
        return ifcopenshell.util.element.get_type(obj)
    except Exception:
        return None


def _extract_file(job_id: str, ifc_path: Path, out_root: Path, config: Dict[str, Any], include: Dict[str, bool], model_rows: List[List[Any]]) -> None:
    source = ifc_path.name
    model = ifcopenshell.open(str(ifc_path))
    line_map = _step_line_map(ifc_path)
    codes = source.split("-")
    schema = getattr(model, "schema", "")
    model_rows.append([f"session://{job_id}/{source}", source, str(codes), schema, dt.datetime.utcnow().isoformat() + "Z"])

    targets = _get_targets(model)
    types = [_type_for_obj(o) for o in targets]
    tnames = [getattr(t, "Name", "") for t in types if t is not None and getattr(t, "Name", "")]
    dupes = {n for n in tnames if tnames.count(n) > 1}

    short_codes = set(config.get("short_codes", []))
    layers = set(config.get("layers", []))
    entity_types = set(config.get("entity_types", []))
    uniclass = set(config.get("uniclass_system_category", []))
    pset_template = config.get("pset_template", {})

    if include.get("project"):
        rows = []
        for ifc_t in ["IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey"]:
            for e in model.by_type(ifc_t):
                children = []
                for rel in getattr(e, "IsDecomposedBy", []) or []:
                    for child in getattr(rel, "RelatedObjects", []) or []:
                        children.append(line_map.get(child.id(), _line_ref(child)))
                rows.append([_line_ref(e), source, getattr(e, "Name", "") or "", getattr(e, "GlobalId", "") or "", e.is_a(), getattr(e, "Description", "") or "", getattr(e, "LongName", "") or "", "; ".join(children)])
        _write_csv(out_root / "IFC Project" / f"IFC PROJECT - {source}.csv", HEADERS["project"], rows)

    prop_rows_all = []
    if include.get("object"):
        rows = []
        for o in targets:
            t = _type_for_obj(o)
            tname = getattr(t, "Name", "") if t else ""
            predefined = ""
            if hasattr(o, "PredefinedType") and getattr(o, "PredefinedType", None):
                predefined = str(o.PredefinedType)
            elif t is not None and hasattr(t, "PredefinedType") and getattr(t, "PredefinedType", None):
                predefined = str(t.PredefinedType)
            name = getattr(o, "Name", "") or ""
            type_tokens = tname.split("_") if tname else []
            type_ok = bool(tname)
            if len(type_tokens) >= 3:
                if len(codes) > 1:
                    type_ok = type_ok and type_tokens[0] == codes[1]
                if len(codes) > 5:
                    type_ok = type_ok and type_tokens[1] == codes[5]
                type_ok = type_ok and type_tokens[2] in entity_types
            else:
                type_ok = False
            stem = ""
            if len(type_tokens) >= 3:
                stem = "_".join(type_tokens[2:])
                while stem and stem[-1].isdigit():
                    stem = stem[:-1]
            short = ""
            if "-" in name:
                toks = name.split("-")
                if len(toks) > 1:
                    short = toks[1] + "-"
            name_ok = bool(name)
            if len(codes) > 1:
                name_ok = name_ok and name.startswith(f"{codes[1]}-")
            if short:
                name_ok = name_ok and short.rstrip("-") in short_codes
            layer = _object_layer_name(o)
            rows.append([_line_ref(o), source, name, getattr(o, "GlobalId", "") or "", o.is_a(), getattr(o, "Description", "") or "", getattr(o, "ObjectType", "") or "", t.is_a() if t else "", tname, predefined, layer, getattr(o, "Tag", "") or "", str(bool(name_ok)), short, str(bool(type_ok)), stem, str(layer in layers), str(tname in dupes if tname else False), getattr(o, "LongName", "") or "", line_map.get(t.id(), _line_ref(t)) if t else "", getattr(t, "Description", "") if t else "", getattr(t, "GlobalId", "") if t else "", _coords(o)])
        _write_csv(out_root / "IFC Object Type" / f"IFC OBJECT TYPE - {source}.csv", HEADERS["object"], rows)

    if include.get("classification"):
        rows = []
        for rel in model.by_type("IfcRelAssociatesClassification"):
            c = getattr(rel, "RelatingClassification", None)
            if not c:
                continue
            ctype = getattr(getattr(c, "ReferencedSource", None), "Name", "") or getattr(c, "Name", "") or ""
            ident = getattr(c, "Identification", "") or getattr(c, "ItemReference", "") or ""
            cname = getattr(c, "Name", "") or ""
            cvalue = f"{ident}: {cname}" if ident and cname else (cname or ident or _str(c))
            for o in getattr(rel, "RelatedObjects", []) or []:
                t = _type_for_obj(o)
                rows.append([_line_ref(o), source, getattr(o, "Name", "") or "", getattr(o, "GlobalId", "") or "", o.is_a(), t.is_a() if t else "", "Type" if o.is_a("IfcTypeObject") else "Occurrence", ctype, cvalue])
        _write_csv(out_root / "IFC Classification" / f"IFC CLASSIFICATION - {source}.csv", HEADERS["classification"], rows)

    if include.get("properties") or include.get("system") or include.get("pset_template"):
        prop_rows, prop_index = [], set()
        for o in targets:
            for rel in getattr(o, "IsDefinedBy", []) or []:
                pset = getattr(rel, "RelatingPropertyDefinition", None)
                if not pset or not pset.is_a("IfcPropertySet"):
                    continue
                for p in getattr(pset, "HasProperties", []) or []:
                    if not p.is_a("IfcPropertySingleValue"):
                        continue
                    val = getattr(getattr(p, "NominalValue", None), "wrappedValue", getattr(p, "NominalValue", ""))
                    row = [_line_ref(o), source, getattr(o, "Name", "") or "", getattr(o, "GlobalId", "") or "", o.is_a(), getattr(o, "Description", "") or "", getattr(o, "Tag", "") or "", "Occurrence", getattr(pset, "Name", "") or "", getattr(p, "Name", "") or "", _str(val)]
                    prop_rows.append(row)
                    prop_rows_all.append(row)
                    prop_index.add((o.id(), getattr(pset, "Name", "") or "", getattr(p, "Name", "") or ""))
            t = _type_for_obj(o)
            for pset in getattr(t, "HasPropertySets", []) or [] if t else []:
                if not pset.is_a("IfcPropertySet"):
                    continue
                for p in getattr(pset, "HasProperties", []) or []:
                    if not p.is_a("IfcPropertySingleValue"):
                        continue
                    val = getattr(getattr(p, "NominalValue", None), "wrappedValue", getattr(p, "NominalValue", ""))
                    row = [_line_ref(o), source, getattr(o, "Name", "") or "", getattr(o, "GlobalId", "") or "", o.is_a(), getattr(o, "Description", "") or "", getattr(o, "Tag", "") or "", "Type", getattr(pset, "Name", "") or "", getattr(p, "Name", "") or "", _str(val)]
                    prop_rows.append(row)
                    prop_rows_all.append(row)
                    prop_index.add((o.id(), getattr(pset, "Name", "") or "", getattr(p, "Name", "")))
        if include.get("properties"):
            _write_csv(out_root / "IFC Properties" / f"IFC PROPERTIES - {source}.csv", HEADERS["properties"], prop_rows)

        if include.get("system"):
            srows = []
            for r in prop_rows:
                if r[8] != "COBie_System" or r[9] not in {"SystemCategory", "SystemName", "SystemDescription"}:
                    continue
                check, code = "", ""
                if r[9] == "SystemCategory":
                    check = str(r[10] in uniclass)
                    lead = (r[10] or "").split(":", 1)[0]
                    parts = lead.split("_")
                    code = "_".join(parts[:4]) if len(parts) >= 4 else lead
                elif r[9] == "SystemName":
                    parts = (r[10] or "").split("_")
                    check = str(len(parts) >= 4)
                    code = "_".join(parts[:4]) if len(parts) >= 4 else ""
                srows.append(r[:8] + [r[8], r[9], r[10], check, code])
            _write_csv(out_root / "IFC System" / f"IFC SYSTEM - {source}.csv", HEADERS["system"], srows)

        if include.get("pset_template"):
            rows = []
            for o in targets:
                t = _type_for_obj(o)
                combo = f"{o.is_a()}-{t.is_a() if t else ''}".rstrip("-")
                reqs = pset_template.get(combo, [])
                for req in reqs:
                    pset_name, prop_name = req.get("Property_Set_Template", ""), req.get("Property_Name_Template", "")
                    ok = (o.id(), pset_name, prop_name) in prop_index
                    rows.append([_line_ref(o), source, getattr(o, "Name", "") or "", getattr(o, "GlobalId", "") or "", combo, getattr(o, "Tag", "") or "", pset_name, prop_name, "Defined" if ok else "Not Defined"])
            _write_csv(out_root / "IFC Pset Template" / f"IFC PSET TEMPLATE - {source}.csv", HEADERS["pset_template"], rows)

    if include.get("spatial"):
        rows = []
        for o in targets:
            container = ifcopenshell.util.element.get_container(o)
            storey_name = ""
            c_name = getattr(container, "Name", "") if container else ""
            c_tag = getattr(container, "Tag", "") if container else ""
            c_ent = container.is_a() if container else ""
            cur = container
            while cur:
                if cur.is_a("IfcBuildingStorey"):
                    storey_name = getattr(cur, "Name", "") or ""
                    break
                cur = ifcopenshell.util.element.get_container(cur)
            group_name = ""
            for rel in getattr(o, "HasAssignments", []) or []:
                grp = getattr(rel, "RelatingGroup", None)
                if grp:
                    group_name = getattr(grp, "Name", "") or ""
                    break
            rows.append([_line_ref(o), source, getattr(o, "Name", "") or "", getattr(o, "GlobalId", "") or "", o.is_a(), getattr(o, "Description", "") or "", getattr(o, "Tag", "") or "", c_name or "", storey_name, c_tag or "", c_ent or "", group_name])
        _write_csv(out_root / "IFC Spatial Structure" / f"IFC SPATIAL - {source}.csv", HEADERS["spatial"], rows)


def run_job(job_id: str, file_records: List[Tuple[str, str]], options: Dict[str, Any], config: Dict[str, Any]) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="ifc_qa_v2_"))
    out_root = workdir / "IFC Output"
    for d in BASE_OUTPUT:
        (out_root / d).mkdir(parents=True, exist_ok=True)
    REGISTRY.update(job_id, status="running", currentStep="Starting", workdir=str(workdir), files=[{"name":n,"percent":0} for n,_ in file_records])
    include = options.get("selected_sheets", {
        "model": True, "project": True, "object": True, "properties": True, "classification": True, "spatial": True, "system": True, "pset_template": True
    })
    model_rows: List[List[Any]] = []
    for i, (name, path) in enumerate(file_records, start=1):
        REGISTRY.append_log(job_id, f"Processing {name}")
        REGISTRY.update(job_id, currentFile=name, currentStep="Extracting", percent=int((i-1)/len(file_records)*90))
        _extract_file(job_id, Path(path), out_root, config, include, model_rows)
        job = REGISTRY.get(job_id) or {}
        files = job.get("files", [])
        for f in files:
            if f.get("name") == name:
                f["percent"] = 100
        REGISTRY.update(job_id, files=files)
    _write_csv(out_root / "IFC Models" / "IFC MODEL TABLE.csv", HEADERS["model"], model_rows)

    zip_path = workdir / "IFC Output.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in out_root.rglob("*.csv"):
            zf.write(p, p.relative_to(workdir).as_posix())
    REGISTRY.update(job_id, status="complete", percent=100, currentStep="Complete", result_path=str(zip_path))


def start_job(file_records: List[Tuple[str, str]], options: Dict[str, Any], config: Dict[str, Any]) -> str:
    job_id = REGISTRY.create()
    th = threading.Thread(target=run_job, args=(job_id, file_records, options, config), daemon=True)
    th.start()
    return job_id


def default_config_from_dir(reference_dir: Path) -> Dict[str, Any]:
    cfg = {}
    for name in ["default_config", "pset_template", "uniclass_system_category", "short_codes", "layers", "entity_types", "property_exclusions"]:
        p = reference_dir / f"{name}.json"
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                cfg[name] = json.load(f)
        else:
            cfg[name] = [] if name != "pset_template" else {}
    cfg["pset_template"] = cfg.get("pset_template", {})
    return cfg
