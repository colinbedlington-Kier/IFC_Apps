import csv
import datetime as dt
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import ifcopenshell
import ifcopenshell.util.element
import ifcopenshell.util.placement
from backend.ifc_qa.config_loader import build_config_indexes, load_default_config

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

STAGE_WEIGHTS = {
    "open": 2.0,
    "definesByType": 8.0,
    "definesByProps": 30.0,
    "containment": 10.0,
    "aggregates": 5.0,
    "classification": 5.0,
    "predefined": 5.0,
    "layers": 10.0,
    "coords": 10.0,
    "write": 15.0,
}


@dataclass
class BasicRow:
    line_ref: str
    name: str
    gid: str
    entity: str
    desc: str
    tag: str
    longname: str
    objecttype: str


@dataclass
class TypeRow:
    line_ref: str
    name: str
    entity: str
    gid: str
    desc: str
    predefined: str


@dataclass
class ObjectFact:
    obj_id: int
    line_ref: str
    name: str
    gid: str
    entity: str
    desc: str
    objecttype: str
    tag: str
    longname: str
    type_id: Optional[int]
    type_entity: str
    type_name: str
    type_desc: str
    type_gid: str
    type_line_ref: str
    predefined: str
    layer: str
    coords: str
    containing_structure: str
    containing_structure_tag: str
    containing_structure_entity: str
    storey: str
    group: str


@dataclass
class FileContext:
    f: ifcopenshell.file
    source_file: str
    ifc_path: Path
    schema: str
    include: Dict[str, bool]
    objects: List[Any] = field(default_factory=list)
    obj_basic: Dict[int, BasicRow] = field(default_factory=dict)
    obj_type: Dict[int, Optional[int]] = field(default_factory=dict)
    type_basic: Dict[int, TypeRow] = field(default_factory=dict)
    predefined: Dict[int, str] = field(default_factory=dict)
    layers: Dict[int, str] = field(default_factory=dict)
    coords: Dict[int, str] = field(default_factory=dict)
    psets_occ: Dict[int, Dict[Tuple[str, str], str]] = field(default_factory=dict)
    psets_type: Dict[int, Dict[Tuple[str, str], str]] = field(default_factory=dict)
    contained_in: Dict[int, Optional[int]] = field(default_factory=dict)
    assigns_group: Dict[int, str] = field(default_factory=dict)
    aggregates_children: Dict[int, List[int]] = field(default_factory=dict)
    classifications: List[List[str]] = field(default_factory=list)
    line_map: Dict[int, str] = field(default_factory=dict)


class IfcQaRegistry:
    def __init__(self, ttl_hours: int = 24):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.ttl_seconds = ttl_hours * 3600

    def create(self, **metadata: Any) -> str:
        with self.lock:
            job_id = uuid.uuid4().hex
            self.jobs[job_id] = {
                "status": "queued",
                "percent": 0,
                "overall_percent": 0,
                "logs": [],
                "files": [],
                "created": time.time(),
                "currentStep": "Queued",
                "per_file_stage": {},
                "per_file_percent": {},
                "processed_counts": {},
                "session_id": metadata.get("session_id"),
                "manifest_summary": {},
            }
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

    def patch_file_progress(self, job_id: str, file_name: str, percent: float, stage: str, counts: Dict[str, int]) -> None:
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            per_file = dict(job.get("per_file_percent", {}))
            per_file[file_name] = round(max(0.0, min(100.0, percent)), 2)
            stages = dict(job.get("per_file_stage", {}))
            stages[file_name] = stage
            processed = dict(job.get("processed_counts", {}))
            processed[file_name] = counts
            files = list(job.get("files", []))
            for f in files:
                if f.get("name") == file_name:
                    f["percent"] = per_file[file_name]
                    f["stage"] = stage
                    f["counts"] = counts
            overall = 0.0
            if files:
                overall = sum(float(f.get("percent", 0.0)) for f in files) / len(files)
            job.update(
                {
                    "per_file_percent": per_file,
                    "per_file_stage": stages,
                    "processed_counts": processed,
                    "files": files,
                    "overall_percent": round(overall, 2),
                    "percent": int(round(overall)),
                    "currentFile": file_name,
                    "currentStep": stage,
                }
            )

    def patch_file_state(self, job_id: str, source_file: str, **updates: Any) -> None:
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            files = list(job.get("files", []))
            changed = False
            for item in files:
                if item.get("source_file") == source_file or item.get("name") == source_file:
                    item.update(updates)
                    item["name"] = source_file
                    item["source_file"] = source_file
                    changed = True
                    break
            if changed:
                totals = [float(f.get("overall_percent", f.get("percent", 0.0)) or 0.0) for f in files]
                overall = (sum(totals) / len(totals)) if totals else 0.0
                job.update(
                    {
                        "files": files,
                        "overall_percent": round(overall, 2),
                        "percent": int(round(overall)),
                        "currentFile": source_file,
                        "currentStep": updates.get("stage", job.get("currentStep", "")),
                    }
                )

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
                    try:
                        os.remove(result)
                    except OSError:
                        pass
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
    with open(ifc_path, "rb") as f:
        for raw in f:
            if not raw or raw[0] != 35 or b"=" not in raw:  # '#'
                continue
            try:
                eq = raw.find(b"=")
                idx = int(raw[1:eq])
                data[idx] = raw.decode("utf-8", errors="ignore").strip()
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

    rep = getattr(obj, "Representation", None)
    reps = getattr(rep, "Representations", []) if rep else []
    for shape in reps or []:
        for item in getattr(shape, "Items", []) or []:
            for la in getattr(item, "LayerAssignment", []) or []:
                name = getattr(la, "Name", "") or ""
                if name:
                    return name
            for la in getattr(item, "LayerAssignments", []) or []:
                name = getattr(la, "Name", "") or ""
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


def _iter_property_values(pset: Any) -> Iterable[Tuple[str, str]]:
    for p in getattr(pset, "HasProperties", []) or []:
        if not p.is_a("IfcPropertySingleValue"):
            continue
        nominal = getattr(p, "NominalValue", None)
        val = getattr(nominal, "wrappedValue", nominal)
        yield (getattr(p, "Name", "") or "", _str(val))


def _get_targets(model: ifcopenshell.file) -> List[Any]:
    out: List[Any] = []
    seen: Set[int] = set()
    for ent in model.by_type("IfcProduct"):
        eid = ent.id()
        if eid not in seen:
            seen.add(eid)
            out.append(ent)
    for t in ["IfcSpace", "IfcBuildingStorey"]:
        for ent in model.by_type(t):
            eid = ent.id()
            if eid not in seen:
                seen.add(eid)
                out.append(ent)
    return out


def _required_line_map(include: Dict[str, bool]) -> bool:
    return bool(include.get("project") or include.get("object"))


def _progress_for_stage(done: float, total: float) -> float:
    if total <= 0:
        return 1.0
    return max(0.0, min(1.0, done / total))


def _update_stage_progress(
    job_id: str,
    source_file: str,
    stage: str,
    processed: int,
    total: int,
    stage_done: Dict[str, float],
    counts: Dict[str, int],
    last_emit: Dict[str, float],
) -> None:
    stage_done[stage] = _progress_for_stage(processed, total)
    weighted = 0.0
    for k, w in STAGE_WEIGHTS.items():
        weighted += w * stage_done.get(k, 0.0)
    now = time.time()
    should_emit = processed == total or now - last_emit.get("t", 0.0) >= 2.0 or (total > 0 and (processed % max(1, total // 100) == 0))
    if should_emit:
        REGISTRY.patch_file_progress(job_id, source_file, weighted, stage, dict(counts))
        last_emit["t"] = now


def _collect_context(job_id: str, ifc_path: Path, source_file: str, include: Dict[str, bool]) -> FileContext:
    started = time.perf_counter()
    model = ifcopenshell.open(str(ifc_path))
    ctx = FileContext(f=model, source_file=source_file, ifc_path=ifc_path, schema=getattr(model, "schema", ""), include=include)

    counts: Dict[str, int] = {"objects": 0}
    stage_done: Dict[str, float] = {}
    last_emit = {"t": 0.0}
    _update_stage_progress(job_id, source_file, "open", 1, 1, stage_done, counts, last_emit)

    ctx.objects = _get_targets(model)
    object_ids = {o.id() for o in ctx.objects}
    for o in ctx.objects:
        oid = o.id()
        ctx.obj_basic[oid] = BasicRow(
            line_ref=_line_ref(o),
            name=getattr(o, "Name", "") or "",
            gid=getattr(o, "GlobalId", "") or "",
            entity=o.is_a(),
            desc=getattr(o, "Description", "") or "",
            tag=getattr(o, "Tag", "") or "",
            longname=getattr(o, "LongName", "") or "",
            objecttype=getattr(o, "ObjectType", "") or "",
        )
        ctx.psets_occ[oid] = {}
        ctx.contained_in[oid] = None

    stage_t = time.perf_counter()
    rel_type = ctx.f.by_type("IfcRelDefinesByType")
    counts["rel_defines_by_type_total"] = len(rel_type)
    for i, rel in enumerate(rel_type, start=1):
        type_obj = getattr(rel, "RelatingType", None)
        tid = type_obj.id() if type_obj else None
        if tid and tid not in ctx.type_basic:
            ctx.type_basic[tid] = TypeRow(
                line_ref=_line_ref(type_obj),
                name=getattr(type_obj, "Name", "") or "",
                entity=type_obj.is_a(),
                gid=getattr(type_obj, "GlobalId", "") or "",
                desc=getattr(type_obj, "Description", "") or "",
                predefined=_str(getattr(type_obj, "PredefinedType", "") or ""),
            )
            ctx.psets_type.setdefault(tid, {})
            for pset in getattr(type_obj, "HasPropertySets", []) or []:
                if not pset or not pset.is_a("IfcPropertySet"):
                    continue
                pset_name = getattr(pset, "Name", "") or ""
                if not pset_name:
                    continue
                for prop_name, value in _iter_property_values(pset):
                    ctx.psets_type[tid][(pset_name, prop_name)] = value
        for obj in getattr(rel, "RelatedObjects", []) or []:
            oid = obj.id()
            if oid in object_ids:
                ctx.obj_type[oid] = tid
        counts["rel_defines_by_type_processed"] = i
        _update_stage_progress(job_id, source_file, "definesByType", i, len(rel_type), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: definesByType {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    rel_props = ctx.f.by_type("IfcRelDefinesByProperties")
    counts["rel_defines_by_props_total"] = len(rel_props)
    for i, rel in enumerate(rel_props, start=1):
        pdef = getattr(rel, "RelatingPropertyDefinition", None)
        if not pdef or not pdef.is_a("IfcPropertySet"):
            counts["rel_defines_by_props_processed"] = i
            _update_stage_progress(job_id, source_file, "definesByProps", i, len(rel_props), stage_done, counts, last_emit)
            continue
        pset_name = getattr(pdef, "Name", "") or ""
        if not pset_name:
            counts["rel_defines_by_props_processed"] = i
            _update_stage_progress(job_id, source_file, "definesByProps", i, len(rel_props), stage_done, counts, last_emit)
            continue
        kvs = list(_iter_property_values(pdef))
        for obj in getattr(rel, "RelatedObjects", []) or []:
            oid = obj.id()
            if oid not in object_ids:
                continue
            bucket = ctx.psets_occ.setdefault(oid, {})
            for prop_name, value in kvs:
                bucket[(pset_name, prop_name)] = value
        counts["rel_defines_by_props_processed"] = i
        _update_stage_progress(job_id, source_file, "definesByProps", i, len(rel_props), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: definesByProps {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    rel_contain = ctx.f.by_type("IfcRelContainedInSpatialStructure")
    counts["rel_containment_total"] = len(rel_contain)
    for i, rel in enumerate(rel_contain, start=1):
        structure = getattr(rel, "RelatingStructure", None)
        sid = structure.id() if structure else None
        for obj in getattr(rel, "RelatedElements", []) or []:
            oid = obj.id()
            if oid in object_ids:
                ctx.contained_in[oid] = sid
        counts["rel_containment_processed"] = i
        _update_stage_progress(job_id, source_file, "containment", i, len(rel_contain), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: containment {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    rel_aggs = ctx.f.by_type("IfcRelAggregates")
    counts["rel_aggregates_total"] = len(rel_aggs)
    for i, rel in enumerate(rel_aggs, start=1):
        parent = getattr(rel, "RelatingObject", None)
        if parent:
            pid = parent.id()
            ctx.aggregates_children.setdefault(pid, [])
            for child in getattr(rel, "RelatedObjects", []) or []:
                ctx.aggregates_children[pid].append(child.id())
        counts["rel_aggregates_processed"] = i
        _update_stage_progress(job_id, source_file, "aggregates", i, len(rel_aggs), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: aggregates {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    rel_class = ctx.f.by_type("IfcRelAssociatesClassification")
    counts["rel_classification_total"] = len(rel_class)
    for i, rel in enumerate(rel_class, start=1):
        c = getattr(rel, "RelatingClassification", None)
        if c:
            ctype = getattr(getattr(c, "ReferencedSource", None), "Name", "") or getattr(c, "Name", "") or ""
            ident = getattr(c, "Identification", "") or getattr(c, "ItemReference", "") or ""
            cname = getattr(c, "Name", "") or ""
            cvalue = f"{ident}: {cname}" if ident and cname else (cname or ident or _str(c))
            for o in getattr(rel, "RelatedObjects", []) or []:
                oid = o.id()
                if oid not in object_ids:
                    continue
                t = ctx.obj_type.get(oid)
                type_entity = ctx.type_basic.get(t).entity if t and t in ctx.type_basic else ""
                ctx.classifications.append([
                    _line_ref(o),
                    source_file,
                    getattr(o, "Name", "") or "",
                    getattr(o, "GlobalId", "") or "",
                    o.is_a(),
                    type_entity,
                    "Type" if o.is_a("IfcTypeObject") else "Occurrence",
                    ctype,
                    cvalue,
                ])
        counts["rel_classification_processed"] = i
        _update_stage_progress(job_id, source_file, "classification", i, len(rel_class), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: classification {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    for i, o in enumerate(ctx.objects, start=1):
        oid = o.id()
        type_id = ctx.obj_type.get(oid)
        predefined = _str(getattr(o, "PredefinedType", "") or "")
        if not predefined and type_id and type_id in ctx.type_basic:
            predefined = ctx.type_basic[type_id].predefined
        ctx.predefined[oid] = predefined

        grp = ""
        for rel in getattr(o, "HasAssignments", []) or []:
            g = getattr(rel, "RelatingGroup", None)
            if g:
                grp = getattr(g, "Name", "") or ""
                if grp:
                    break
        ctx.assigns_group[oid] = grp
        counts["objects"] = i
        _update_stage_progress(job_id, source_file, "predefined", i, len(ctx.objects), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: predefined {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    for i, o in enumerate(ctx.objects, start=1):
        ctx.layers[o.id()] = _object_layer_name(o)
        _update_stage_progress(job_id, source_file, "layers", i, len(ctx.objects), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: layers {time.perf_counter()-stage_t:.2f}s")

    stage_t = time.perf_counter()
    for i, o in enumerate(ctx.objects, start=1):
        ctx.coords[o.id()] = _coords(o)
        _update_stage_progress(job_id, source_file, "coords", i, len(ctx.objects), stage_done, counts, last_emit)
    REGISTRY.append_log(job_id, f"{source_file}: coords {time.perf_counter()-stage_t:.2f}s")

    if _required_line_map(include):
        ctx.line_map = _step_line_map(ifc_path)

    elapsed = time.perf_counter() - started
    REGISTRY.append_log(job_id, f"Indexed {source_file} in {elapsed:.2f}s (objects={len(ctx.objects)})")
    return ctx


def _storey_name_from_map(ctx: FileContext, sid: Optional[int]) -> str:
    cur = sid
    visited: Set[int] = set()
    while cur and cur not in visited:
        visited.add(cur)
        ent = ctx.f.by_id(cur)
        if not ent:
            return ""
        if ent.is_a("IfcBuildingStorey"):
            return getattr(ent, "Name", "") or ""
        parent = None
        for rel in getattr(ent, "Decomposes", []) or []:
            parent = getattr(rel, "RelatingObject", None)
            if parent:
                break
        cur = parent.id() if parent else None
    return ""


def _object_facts(ctx: FileContext) -> Iterable[ObjectFact]:
    for o in ctx.objects:
        oid = o.id()
        basic = ctx.obj_basic[oid]
        tid = ctx.obj_type.get(oid)
        trow = ctx.type_basic.get(tid) if tid else None
        sid = ctx.contained_in.get(oid)
        structure = ctx.f.by_id(sid) if sid else None
        yield ObjectFact(
            obj_id=oid,
            line_ref=basic.line_ref,
            name=basic.name,
            gid=basic.gid,
            entity=basic.entity,
            desc=basic.desc,
            objecttype=basic.objecttype,
            tag=basic.tag,
            longname=basic.longname,
            type_id=tid,
            type_entity=trow.entity if trow else "",
            type_name=trow.name if trow else "",
            type_desc=trow.desc if trow else "",
            type_gid=trow.gid if trow else "",
            type_line_ref=ctx.line_map.get(tid, trow.line_ref if trow else "") if tid else "",
            predefined=ctx.predefined.get(oid, ""),
            layer=ctx.layers.get(oid, ""),
            coords=ctx.coords.get(oid, ""),
            containing_structure=getattr(structure, "Name", "") or "",
            containing_structure_tag=getattr(structure, "Tag", "") or "",
            containing_structure_entity=structure.is_a() if structure else "",
            storey=_storey_name_from_map(ctx, sid),
            group=ctx.assigns_group.get(oid, ""),
        )


def _csv_writer(path: Path, header: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "w", newline="", encoding="utf-8")
    writer = csv.writer(handle)
    writer.writerow(header)
    return handle, writer


def _write_outputs(job_id: str, ctx: FileContext, out_root: Path, config: Dict[str, Any]) -> List[Any]:
    source = ctx.source_file
    codes = source.split("-")
    indexes = config.get("_indexes") if isinstance(config.get("_indexes"), dict) else build_config_indexes(config)
    short_code_set = indexes.get("short_code_set", set())
    short_codes_by_entity = indexes.get("short_codes_by_entity", {})
    layer_set = indexes.get("layer_set", set())
    entity_type_by_key = indexes.get("entity_type_by_key", {})
    entity_types_by_natural_language_entity = indexes.get("entity_types_by_natural_language_entity", {})
    system_category_value_set = indexes.get("system_category_value_set", set())
    system_category_by_number = indexes.get("system_category_by_number", {})
    pset_template = indexes.get("pset_template_map", {})

    counts = {"write_objects_total": len(ctx.objects), "write_objects_processed": 0}
    stage_done = {k: 1.0 for k in STAGE_WEIGHTS if k != "write"}
    last_emit = {"t": 0.0}

    facts = list(_object_facts(ctx))
    type_names = [f.type_name for f in facts if f.type_name]
    dupes = {n for n in type_names if type_names.count(n) > 1}

    if ctx.include.get("project"):
        h, w = _csv_writer(out_root / "IFC Project" / f"IFC PROJECT - {source}.csv", HEADERS["project"])
        try:
            for ifc_t in ["IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey"]:
                for e in ctx.f.by_type(ifc_t):
                    children = [ctx.line_map.get(cid, f"#{cid}=") for cid in ctx.aggregates_children.get(e.id(), [])]
                    w.writerow([
                        _line_ref(e),
                        source,
                        getattr(e, "Name", "") or "",
                        getattr(e, "GlobalId", "") or "",
                        e.is_a(),
                        getattr(e, "Description", "") or "",
                        getattr(e, "LongName", "") or "",
                        "; ".join(children),
                    ])
        finally:
            h.close()

    if ctx.include.get("object"):
        h, w = _csv_writer(out_root / "IFC Object Type" / f"IFC OBJECT TYPE - {source}.csv", HEADERS["object"])
        try:
            for i, fact in enumerate(facts, start=1):
                type_tokens: List[str] = fact.type_name.split("_") if fact.type_name else []
                type_key = "-".join([fact.entity or "", fact.type_entity or "", fact.predefined or ""])
                type_ok = bool(type_key and type_key in entity_type_by_key)

                if not type_ok and fact.type_name:
                    if len(type_tokens) >= 3:
                        natural_entity = type_tokens[2]
                        type_ok = natural_entity in entity_types_by_natural_language_entity

                stem = ""
                if len(type_tokens) >= 3:
                    stem = "_".join(type_tokens[2:])
                    while stem and stem[-1].isdigit():
                        stem = stem[:-1]

                short = ""
                if "-" in fact.name:
                    toks = fact.name.split("-")
                    if len(toks) > 1:
                        short = toks[1] + "-"

                name_ok = bool(fact.name)
                if len(codes) > 1:
                    name_ok = name_ok and fact.name.startswith(f"{codes[1]}-")
                if short:
                    short_value = short.rstrip("-")
                    name_ok = name_ok and short_value in short_code_set
                    natural_entity_rows = short_codes_by_entity.get(fact.entity, [])
                    if natural_entity_rows:
                        allowed = {
                            r.get("Nomenclature_Short_Code")
                            for r in natural_entity_rows
                            if r.get("Nomenclature_Short_Code")
                        }
                        name_ok = name_ok and short_value in allowed

                w.writerow([
                    fact.line_ref,
                    source,
                    fact.name,
                    fact.gid,
                    fact.entity,
                    fact.desc,
                    fact.objecttype,
                    fact.type_entity,
                    fact.type_name,
                    fact.predefined,
                    fact.layer,
                    fact.tag,
                    str(bool(name_ok)),
                    short,
                    str(bool(type_ok)),
                    stem,
                    str(fact.layer in layer_set),
                    str(fact.type_name in dupes if fact.type_name else False),
                    fact.longname,
                    fact.type_line_ref,
                    fact.type_desc,
                    fact.type_gid,
                    fact.coords,
                ])
                counts["write_objects_processed"] = i
                _update_stage_progress(job_id, source, "write", i, len(facts), stage_done, counts, last_emit)
        finally:
            h.close()

    prop_index: Dict[int, Set[Tuple[str, str]]] = {}
    if ctx.include.get("properties") or ctx.include.get("system") or ctx.include.get("pset_template"):
        props_handle, props_writer = (None, None)
        if ctx.include.get("properties"):
            props_handle, props_writer = _csv_writer(out_root / "IFC Properties" / f"IFC PROPERTIES - {source}.csv", HEADERS["properties"])

        system_handle, system_writer = (None, None)
        if ctx.include.get("system"):
            system_handle, system_writer = _csv_writer(out_root / "IFC System" / f"IFC SYSTEM - {source}.csv", HEADERS["system"])

        try:
            for fact in facts:
                oid = fact.obj_id
                prop_index.setdefault(oid, set())
                for (pset_name, prop_name), value in ctx.psets_occ.get(oid, {}).items():
                    row = [fact.line_ref, source, fact.name, fact.gid, fact.entity, fact.desc, fact.tag, "Occurrence", pset_name, prop_name, value]
                    if props_writer:
                        props_writer.writerow(row)
                    prop_index[oid].add((pset_name, prop_name))
                    if system_writer and pset_name == "COBie_System" and prop_name in {"SystemCategory", "SystemName", "SystemDescription"}:
                        check, code = "", ""
                        if prop_name == "SystemCategory":
                            check = str(value in system_category_value_set)
                            lead = (value or "").split(":", 1)[0]
                            if lead in system_category_by_number:
                                code = lead
                            else:
                                parts = lead.split("_")
                                code = "_".join(parts[:4]) if len(parts) >= 4 else lead
                        elif prop_name == "SystemName":
                            parts = (value or "").split("_")
                            check = str(len(parts) >= 4)
                            code = "_".join(parts[:4]) if len(parts) >= 4 else ""
                        system_writer.writerow(row[:8] + [row[8], row[9], row[10], check, code])

                type_props = ctx.psets_type.get(fact.type_id, {}) if fact.type_id else {}
                for (pset_name, prop_name), value in type_props.items():
                    row = [fact.line_ref, source, fact.name, fact.gid, fact.entity, fact.desc, fact.tag, "Type", pset_name, prop_name, value]
                    if props_writer:
                        props_writer.writerow(row)
                    prop_index[oid].add((pset_name, prop_name))
                    if system_writer and pset_name == "COBie_System" and prop_name in {"SystemCategory", "SystemName", "SystemDescription"}:
                        check, code = "", ""
                        if prop_name == "SystemCategory":
                            check = str(value in system_category_value_set)
                            lead = (value or "").split(":", 1)[0]
                            if lead in system_category_by_number:
                                code = lead
                            else:
                                parts = lead.split("_")
                                code = "_".join(parts[:4]) if len(parts) >= 4 else lead
                        elif prop_name == "SystemName":
                            parts = (value or "").split("_")
                            check = str(len(parts) >= 4)
                            code = "_".join(parts[:4]) if len(parts) >= 4 else ""
                        system_writer.writerow(row[:8] + [row[8], row[9], row[10], check, code])
        finally:
            if props_handle:
                props_handle.close()
            if system_handle:
                system_handle.close()

        if ctx.include.get("pset_template"):
            h, w = _csv_writer(out_root / "IFC Pset Template" / f"IFC PSET TEMPLATE - {source}.csv", HEADERS["pset_template"])
            try:
                for fact in facts:
                    combo = f"{fact.entity}-{fact.type_entity}".rstrip("-")
                    reqs = pset_template.get(combo, [])
                    seen = prop_index.get(fact.obj_id, set())
                    for req in reqs:
                        pset_name = req.get("Property_Set_Template", "")
                        prop_name = req.get("Property_Name_Template", "")
                        ok = (pset_name, prop_name) in seen
                        w.writerow([
                            fact.line_ref,
                            source,
                            fact.name,
                            fact.gid,
                            combo,
                            fact.tag,
                            pset_name,
                            prop_name,
                            "Defined" if ok else "Not Defined",
                        ])
            finally:
                h.close()

    if ctx.include.get("classification"):
        h, w = _csv_writer(out_root / "IFC Classification" / f"IFC CLASSIFICATION - {source}.csv", HEADERS["classification"])
        try:
            for row in ctx.classifications:
                w.writerow(row)
        finally:
            h.close()

    if ctx.include.get("spatial"):
        h, w = _csv_writer(out_root / "IFC Spatial Structure" / f"IFC SPATIAL - {source}.csv", HEADERS["spatial"])
        try:
            for fact in facts:
                w.writerow([
                    fact.line_ref,
                    source,
                    fact.name,
                    fact.gid,
                    fact.entity,
                    fact.desc,
                    fact.tag,
                    fact.containing_structure,
                    fact.storey,
                    fact.containing_structure_tag,
                    fact.containing_structure_entity,
                    fact.group,
                ])
        finally:
            h.close()

    _update_stage_progress(job_id, source, "write", len(facts), max(1, len(facts)), stage_done, counts, last_emit)
    return [f"session://{job_id}/{source}", source, str(codes), ctx.schema, dt.datetime.utcnow().isoformat() + "Z"]


def _process_file(job_id: str, ifc_path: Path, out_root: Path, config: Dict[str, Any], include: Dict[str, bool]) -> List[Any]:
    source = ifc_path.name
    t0 = time.perf_counter()
    REGISTRY.append_log(job_id, f"Processing {source}")
    REGISTRY.patch_file_progress(job_id, source, 0.0, "open", {})
    ctx = _collect_context(job_id, ifc_path, source, include)
    model_row = _write_outputs(job_id, ctx, out_root, config)
    elapsed = time.perf_counter() - t0
    REGISTRY.append_log(job_id, f"Completed {source} in {elapsed:.2f}s")
    REGISTRY.patch_file_progress(job_id, source, 100.0, "complete", {"objects": len(ctx.objects)})
    return model_row


def _write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([_str(x) for x in r])


def _session_manifest_path(session_root: Path) -> Path:
    return session_root / "manifest.json"


def _load_manifest(session_root: Path, session_id: str) -> Dict[str, Any]:
    path = _session_manifest_path(session_root)
    if path.exists():
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    now = dt.datetime.utcnow().isoformat() + "Z"
    return {"session_id": session_id, "created_at": now, "updated_at": now, "processed_files": []}


def _save_manifest(session_root: Path, manifest: Dict[str, Any]) -> None:
    manifest["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
    path = _session_manifest_path(session_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)


def _remove_existing_outputs(out_root: Path, source_file: str) -> None:
    patterns = [
        f"IFC PROJECT - {source_file}.csv",
        f"IFC OBJECT TYPE - {source_file}.csv",
        f"IFC PROPERTIES - {source_file}.csv",
        f"IFC PSET TEMPLATE - {source_file}.csv",
        f"IFC CLASSIFICATION - {source_file}.csv",
        f"IFC SPATIAL - {source_file}.csv",
        f"IFC SYSTEM - {source_file}.csv",
    ]
    for pattern in patterns:
        for path in out_root.rglob(pattern):
            path.unlink(missing_ok=True)


def _collect_output_paths(out_root: Path, source_file: str) -> Dict[str, str]:
    return {
        "classification": (out_root / "IFC Classification" / f"IFC CLASSIFICATION - {source_file}.csv").as_posix(),
        "object_type": (out_root / "IFC Object Type" / f"IFC OBJECT TYPE - {source_file}.csv").as_posix(),
        "project": (out_root / "IFC Project" / f"IFC PROJECT - {source_file}.csv").as_posix(),
        "properties": (out_root / "IFC Properties" / f"IFC PROPERTIES - {source_file}.csv").as_posix(),
        "pset_template": (out_root / "IFC Pset Template" / f"IFC PSET TEMPLATE - {source_file}.csv").as_posix(),
        "spatial": (out_root / "IFC Spatial Structure" / f"IFC SPATIAL - {source_file}.csv").as_posix(),
        "system": (out_root / "IFC System" / f"IFC SYSTEM - {source_file}.csv").as_posix(),
    }


def _rebuild_session_zip(session_root: Path, out_root: Path) -> Path:
    zip_path = session_root / "IFC Output.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in out_root.rglob("*.csv"):
            zf.write(p, p.relative_to(session_root).as_posix())
    return zip_path


def run_session_job(
    job_id: str,
    session_root: Path,
    session_id: str,
    file_records: List[Tuple[str, str]],
    options: Dict[str, Any],
    config: Dict[str, Any],
    mode: str = "append",
) -> None:
    out_root = session_root / "IFC Output"
    for d in BASE_OUTPUT:
        (out_root / d).mkdir(parents=True, exist_ok=True)
    include = options.get("selected_sheets", {
        "model": True,
        "project": True,
        "object": True,
        "properties": True,
        "classification": True,
        "spatial": True,
        "system": True,
        "pset_template": True,
    })
    manifest = _load_manifest(session_root, session_id)
    if mode == "replace":
        manifest["processed_files"] = []
        shutil.rmtree(out_root, ignore_errors=True)
        for d in BASE_OUTPUT:
            (out_root / d).mkdir(parents=True, exist_ok=True)
    existing_names = {entry.get("source_file", "") for entry in manifest.get("processed_files", [])}
    now = dt.datetime.utcnow().isoformat() + "Z"
    files_state = []
    for original_name, _ in file_records:
        source_file = Path(original_name).name
        duplicate = source_file in existing_names
        files_state.append(
            {
                "name": source_file,
                "source_file": source_file,
                "status": "uploaded",
                "upload_percent": 100,
                "process_percent": 0,
                "overall_percent": 50,
                "stage": "uploaded",
                "message": "Uploaded, waiting to process…" if not duplicate else "Duplicate filename detected, replacing existing outputs.",
                "added_at": now,
                "duplicate": duplicate,
            }
        )
    REGISTRY.update(job_id, status="running", currentStep="uploaded", files=files_state, session_id=session_id)
    had_failures = False
    try:
        for _, path in file_records:
            source = Path(path).name
            try:
                REGISTRY.patch_file_state(job_id, source, status="processing", stage="processing", message="Extracting properties…", process_percent=5, overall_percent=53)
                if source in existing_names:
                    _remove_existing_outputs(out_root, source)
                    manifest["processed_files"] = [entry for entry in manifest.get("processed_files", []) if entry.get("source_file") != source]
                model_row = _process_file(job_id, Path(path), out_root, config, include)
                entry = {
                    "source_file": source,
                    "source_path": str(path),
                    "status": "complete",
                    "added_at": dt.datetime.utcnow().isoformat() + "Z",
                    "outputs": _collect_output_paths(out_root, source),
                    "model_table_row": {
                        "Source_Path": model_row[0],
                        "Source_File": model_row[1],
                        "File_Codes": model_row[2],
                        "Model_Schema_Status": model_row[3],
                        "Date_Checked": model_row[4],
                    },
                }
                manifest.setdefault("processed_files", []).append(entry)
                existing_names.add(source)
                REGISTRY.patch_file_state(job_id, source, status="complete", stage="complete", message="Complete", process_percent=100, overall_percent=100, upload_percent=100, success=True, outputs=entry["outputs"], error="")
            except Exception as exc:
                had_failures = True
                REGISTRY.append_log(job_id, f"Failed {source}: {exc}")
                REGISTRY.patch_file_state(job_id, source, status="failed", stage="failed", message=f"Failed: {exc}", process_percent=100, overall_percent=100, success=False, outputs={}, error=str(exc))

        model_rows = [
            [
                item.get("model_table_row", {}).get("Source_Path", ""),
                item.get("model_table_row", {}).get("Source_File", ""),
                item.get("model_table_row", {}).get("File_Codes", ""),
                item.get("model_table_row", {}).get("Model_Schema_Status", ""),
                item.get("model_table_row", {}).get("Date_Checked", ""),
            ]
            for item in sorted(manifest.get("processed_files", []), key=lambda row: row.get("source_file", ""))
        ]
        _write_csv(out_root / "IFC Models" / "IFC MODEL TABLE.csv", HEADERS["model"], model_rows)
        zip_path = _rebuild_session_zip(session_root, out_root)
        parent_session_root = session_root.parent
        if parent_session_root.exists():
            shutil.copy2(zip_path, parent_session_root / "IFC QA Output.zip")
        _save_manifest(session_root, manifest)
        REGISTRY.update(
            job_id,
            status="complete_with_errors" if had_failures else "complete",
            percent=100,
            overall_percent=100,
            currentStep="Complete with errors" if had_failures else "Complete",
            result_path=str(zip_path),
            manifest_summary={
                "session_id": session_id,
                "model_count": len(manifest.get("processed_files", [])),
                "updated_at": manifest.get("updated_at"),
                "source_files": [x.get("source_file", "") for x in manifest.get("processed_files", [])],
                "has_zip": zip_path.exists(),
                "failed_count": sum(1 for item in REGISTRY.get(job_id).get("files", []) if item.get("status") == "failed") if REGISTRY.get(job_id) else 0,
            },
        )
    except Exception as exc:
        REGISTRY.append_log(job_id, f"Failed: {exc}")
        REGISTRY.update(job_id, status="failed", currentStep="Failed", percent=100)


def run_job(job_id: str, file_records: List[Tuple[str, str]], options: Dict[str, Any], config: Dict[str, Any]) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="ifc_qa_v2_"))
    out_root = workdir / "IFC Output"
    for d in BASE_OUTPUT:
        (out_root / d).mkdir(parents=True, exist_ok=True)

    include = options.get(
        "selected_sheets",
        {
            "model": True,
            "project": True,
            "object": True,
            "properties": True,
            "classification": True,
            "spatial": True,
            "system": True,
            "pset_template": True,
        },
    )

    max_workers = int(options.get("max_workers") or min(3, max(1, (os.cpu_count() or 2) // 2)))
    max_workers = max(1, min(3, max_workers))

    REGISTRY.update(
        job_id,
        status="running",
        currentStep="Starting",
        workdir=str(workdir),
        files=[{"name": n, "percent": 0, "stage": "queued", "counts": {}} for n, _ in file_records],
    )

    model_rows: List[List[Any]] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(_process_file, job_id, Path(path), out_root, config, include)
                for _, path in file_records
            ]
            for fut in as_completed(futures):
                model_rows.append(fut.result())

        _write_csv(out_root / "IFC Models" / "IFC MODEL TABLE.csv", HEADERS["model"], model_rows)

        zip_path = workdir / "IFC Output.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in out_root.rglob("*.csv"):
                zf.write(p, p.relative_to(workdir).as_posix())

        REGISTRY.update(job_id, status="complete", percent=100, overall_percent=100, currentStep="Complete", result_path=str(zip_path))
    except Exception as exc:
        REGISTRY.append_log(job_id, f"Failed: {exc}")
        REGISTRY.update(job_id, status="failed", currentStep="Failed", percent=100)


def start_job(file_records: List[Tuple[str, str]], options: Dict[str, Any], config: Dict[str, Any]) -> str:
    job_id = REGISTRY.create()
    th = threading.Thread(target=run_job, args=(job_id, file_records, options, config), daemon=True)
    th.start()
    return job_id


def start_session_job(
    session_root: Path,
    session_id: str,
    file_records: List[Tuple[str, str]],
    options: Dict[str, Any],
    config: Dict[str, Any],
    mode: str,
) -> str:
    job_id = REGISTRY.create(session_id=session_id)
    th = threading.Thread(
        target=run_session_job,
        args=(job_id, session_root, session_id, file_records, options, config, mode),
        daemon=True,
    )
    th.start()
    return job_id


def read_session_summary(session_root: Path, session_id: str) -> Dict[str, Any]:
    manifest = _load_manifest(session_root, session_id)
    zip_path = session_root / "IFC Output.zip"
    processed = manifest.get("processed_files", [])
    return {
        "session_id": session_id,
        "model_count": len(processed),
        "source_files": [entry.get("source_file", "") for entry in processed],
        "updated_at": manifest.get("updated_at"),
        "has_zip": zip_path.exists(),
    }


def default_config_from_dir(reference_dir: Path) -> Dict[str, Any]:
    del reference_dir
    return load_default_config()
