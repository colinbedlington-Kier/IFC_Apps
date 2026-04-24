from __future__ import annotations

import csv
import json
import os
import time
import traceback
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import ifcopenshell
import logging

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None  # type: ignore


class AreaSpaceError(Exception):
    pass


_KEYWORDS = ("information cad layer", "cad layer", "layer", "presentation layer")
LOGGER = logging.getLogger("ifc_app.area_spaces")
LARGE_IFC_WARNING_MB = float(os.getenv("AREA_SPACES_LARGE_IFC_WARNING_MB", "200"))
MEMORY_ABORT_THRESHOLD = float(os.getenv("AREA_SPACES_MEMORY_ABORT_THRESHOLD", "0.80"))


@dataclass
class LayerSignal:
    source: str
    name: str
    value: str
    reason: str


@dataclass
class Candidate:
    step_id: int
    global_id: str
    name: str
    long_name: str
    object_type: str
    matched_source: str
    matched_name: str
    matched_value: str
    reason: str
    has_representation: bool
    spatial_parent: str


@dataclass
class ScanResult:
    source_file: str
    total_spaces: int
    candidates: List[Candidate]


@dataclass
class PurgeResult:
    source_file: str
    total_spaces: int
    candidates_found: int
    selected_for_purge: int
    purged_count: int
    output_ifc: str
    report_csv: str


def _str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(_str(item) for item in value)
    return str(value)


def _normalize_lower(value: Any) -> str:
    return _str(value).strip().lower()


def _contains_area(text: Any) -> bool:
    return "area" in _normalize_lower(text)


def _iter_layer_assignments(item: Any) -> Iterable[Any]:
    for attr in ("LayerAssignments", "LayerAssignment"):
        value = getattr(item, attr, None)
        if value:
            if isinstance(value, (list, tuple)):
                yield from value
            else:
                yield value


def _collect_property_signals(space: Any) -> List[LayerSignal]:
    signals: List[LayerSignal] = []
    for rel in getattr(space, "IsDefinedBy", []) or []:
        pdef = getattr(rel, "RelatingPropertyDefinition", None)
        if pdef is None:
            continue
        props = list(getattr(pdef, "HasProperties", []) or [])
        for prop in props:
            prop_name = _str(getattr(prop, "Name", ""))
            if not any(keyword in _normalize_lower(prop_name) for keyword in _KEYWORDS):
                continue
            prop_value = ""
            nominal = getattr(prop, "NominalValue", None)
            if nominal is not None:
                prop_value = _str(getattr(nominal, "wrappedValue", nominal))
            if not prop_value and hasattr(prop, "ListValues"):
                prop_value = _str(getattr(prop, "ListValues", []))
            signals.append(
                LayerSignal(
                    source="property_set",
                    name=prop_name,
                    value=prop_value,
                    reason="property-layer-signal",
                )
            )
    return signals


def get_ifcspace_layer_signals(space: Any) -> List[LayerSignal]:
    signals: List[LayerSignal] = []

    for assignment in _iter_layer_assignments(space):
        signals.append(
            LayerSignal(
                source="space.layer_assignment",
                name=_str(getattr(assignment, "Name", "")),
                value=_str(getattr(assignment, "Description", "")),
                reason="direct-space-layer-assignment",
            )
        )

    representation = getattr(space, "Representation", None)
    reps = list(getattr(representation, "Representations", []) or []) if representation else []
    for rep in reps:
        for assignment in _iter_layer_assignments(rep):
            signals.append(
                LayerSignal(
                    source="representation.layer_assignment",
                    name=_str(getattr(assignment, "Name", "")),
                    value=_str(getattr(assignment, "Description", "")),
                    reason="representation-layer-assignment",
                )
            )
        for item in list(getattr(rep, "Items", []) or []):
            for assignment in _iter_layer_assignments(item):
                signals.append(
                    LayerSignal(
                        source="representation.item.layer_assignment",
                        name=_str(getattr(assignment, "Name", "")),
                        value=_str(getattr(assignment, "Description", "")),
                        reason="representation-item-layer-assignment",
                    )
                )

    signals.extend(_collect_property_signals(space))

    return signals


def _spatial_parent_label(space: Any) -> str:
    for rel in getattr(space, "Decomposes", []) or []:
        parent = getattr(rel, "RelatingObject", None)
        if parent is not None:
            return f"{parent.is_a()}:{_str(getattr(parent, 'Name', ''))}"
    for rel in getattr(space, "ContainedInStructure", []) or []:
        parent = getattr(rel, "RelatingStructure", None)
        if parent is not None:
            return f"{parent.is_a()}:{_str(getattr(parent, 'Name', ''))}"
    return ""


def is_area_space_candidate(space: Any) -> Optional[Candidate]:
    if not hasattr(space, "is_a") or not space.is_a("IfcSpace"):
        return None

    signals = get_ifcspace_layer_signals(space)
    for signal in signals:
        if _contains_area(signal.name) or _contains_area(signal.value):
            return Candidate(
                step_id=int(space.id()),
                global_id=_str(getattr(space, "GlobalId", "")),
                name=_str(getattr(space, "Name", "")),
                long_name=_str(getattr(space, "LongName", "")),
                object_type=_str(getattr(space, "ObjectType", "")),
                matched_source=signal.source,
                matched_name=signal.name,
                matched_value=signal.value,
                reason=signal.reason,
                has_representation=getattr(space, "Representation", None) is not None,
                spatial_parent=_spatial_parent_label(space),
            )
    return None


def scan_ifc_for_area_spaces(path: Path) -> ScanResult:
    if not path.exists():
        raise AreaSpaceError(f"IFC file not found: {path.name}")
    file_size_mb = round(path.stat().st_size / (1024 * 1024), 2)
    LOGGER.info("area_spaces_scan_start filename=%s size_mb=%s", path.name, file_size_mb)
    if file_size_mb >= LARGE_IFC_WARNING_MB:
        LOGGER.warning("area_spaces_large_ifc_warning filename=%s size_mb=%s threshold_mb=%s", path.name, file_size_mb, LARGE_IFC_WARNING_MB)
    open_started = time.perf_counter()
    try:
        LOGGER.info("ifc_open_start filename=%s", path.name)
        model = ifcopenshell.open(str(path))
        LOGGER.info("ifc_open_complete filename=%s duration_ms=%d", path.name, int((time.perf_counter() - open_started) * 1000))
    except Exception as exc:
        raise AreaSpaceError(f"Unable to open IFC file {path.name}: {exc}") from exc

    scan_started = time.perf_counter()
    spaces = model.by_type("IfcSpace")
    LOGGER.info("ifcspace_count filename=%s count=%s", path.name, len(spaces))
    candidates: List[Candidate] = []
    for space in spaces:
        candidate = is_area_space_candidate(space)
        if candidate is not None:
            candidates.append(candidate)
    LOGGER.info("candidates_count filename=%s count=%s", path.name, len(candidates))
    rss_mb = None
    if psutil is not None:
        try:
            rss_mb = round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 2)
        except Exception:
            rss_mb = None
    LOGGER.info("scan_complete filename=%s duration_ms=%d memory_mb=%s", path.name, int((time.perf_counter() - scan_started) * 1000), rss_mb)

    return ScanResult(source_file=path.name, total_spaces=len(spaces), candidates=candidates)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _update_related_collection(rel: Any, attr: str, remove_ids: set[int]) -> bool:
    current = _as_list(getattr(rel, attr, None))
    if not current:
        return False
    kept = [item for item in current if int(item.id()) not in remove_ids]
    if len(kept) == len(current):
        return False
    setattr(rel, attr, tuple(kept))
    return True


def _cleanup_relationships(model: ifcopenshell.file, remove_ids: set[int]) -> None:
    relation_specs: Sequence[Tuple[str, str]] = [
        ("IfcRelContainedInSpatialStructure", "RelatedElements"),
        ("IfcRelAggregates", "RelatedObjects"),
        ("IfcRelAssociates", "RelatedObjects"),
        ("IfcRelDefinesByProperties", "RelatedObjects"),
        ("IfcRelAssigns", "RelatedObjects"),
    ]

    for rel_type, attr in relation_specs:
        for rel in list(model.by_type(rel_type)):
            changed = _update_related_collection(rel, attr, remove_ids)
            if changed and not _as_list(getattr(rel, attr, None)):
                model.remove(rel)

    for rel in list(model.by_type("IfcRelSpaceBoundary")):
        relating_space = getattr(rel, "RelatingSpace", None)
        related_elem = getattr(rel, "RelatedBuildingElement", None)
        if (relating_space is not None and int(relating_space.id()) in remove_ids) or (
            related_elem is not None and int(related_elem.id()) in remove_ids
        ):
            model.remove(rel)


def _write_csv_report(path: Path, rows: List[Dict[str, Any]]) -> None:
    headers = [
        "source_file",
        "step_id",
        "global_id",
        "name",
        "long_name",
        "object_type",
        "matched_source",
        "matched_name",
        "matched_value",
        "reason",
        "has_representation",
        "spatial_parent",
        "status",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _memory_too_high() -> bool:
    if psutil is None:
        return False
    try:
        vm = psutil.virtual_memory()
        return float(vm.percent) / 100.0 >= MEMORY_ABORT_THRESHOLD
    except Exception:
        return False


def purge_area_spaces(source_path: Path, selected_global_ids_or_step_ids: Sequence[str], output_path: Path) -> PurgeResult:
    if not source_path.exists():
        raise AreaSpaceError(f"IFC file not found: {source_path.name}")
    selected_tokens = {str(token).strip() for token in selected_global_ids_or_step_ids if str(token).strip()}
    LOGGER.info("purge_start filename=%s selected_count=%s", source_path.name, len(selected_tokens))
    if not selected_tokens:
        raise AreaSpaceError("No selected candidates were provided for purge")
    open_started = time.perf_counter()
    try:
        LOGGER.info("ifc_open_start filename=%s", source_path.name)
        model = ifcopenshell.open(str(source_path))
        LOGGER.info("ifc_open_complete filename=%s duration_ms=%d", source_path.name, int((time.perf_counter() - open_started) * 1000))
    except Exception as exc:
        raise AreaSpaceError(f"Unable to open IFC file {source_path.name} for purge: {exc}") from exc

    spaces = model.by_type("IfcSpace")
    total_spaces = len(spaces)
    candidates_found = 0
    candidate_rows: List[Candidate] = []
    selected_ids: set[int] = set()
    for space in spaces:
        candidate = is_area_space_candidate(space)
        if candidate is not None:
            candidates_found += 1
            candidate_rows.append(candidate)
            if candidate.global_id in selected_tokens or str(candidate.step_id) in selected_tokens:
                selected_ids.add(candidate.step_id)

    to_remove = [space for space in spaces if int(space.id()) in selected_ids]
    remove_ids = {int(space.id()) for space in to_remove}

    _cleanup_relationships(model, remove_ids)
    LOGGER.info("purge_remove_relationships_complete filename=%s count=%s", source_path.name, len(remove_ids))

    for space in to_remove:
        model.remove(space)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if _memory_too_high():
        raise AreaSpaceError("Not enough memory to safely write cleaned IFC on this plan.")
    write_started = time.perf_counter()
    try:
        LOGGER.info("purge_write_start filename=%s output=%s", source_path.name, output_path.name)
        model.write(str(output_path))
        LOGGER.info("purge_write_complete filename=%s duration_ms=%d", source_path.name, int((time.perf_counter() - write_started) * 1000))
    except Exception as exc:
        LOGGER.error("purge_failed filename=%s exception=%s stack=%s", source_path.name, exc, traceback.format_exc())
        raise AreaSpaceError(f"Failed writing cleaned IFC {output_path.name}: {exc}") from exc

    report_path = output_path.with_name(output_path.stem.replace(".area-spaces-purged", "") + ".area-spaces-purge-report.csv")
    rows: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        payload = asdict(candidate)
        payload["source_file"] = source_path.name
        payload["global_id"] = payload.pop("global_id")
        payload["status"] = "purged" if candidate.step_id in remove_ids else "not_selected"
        rows.append(payload)
    _write_csv_report(report_path, rows)

    return PurgeResult(
        source_file=source_path.name,
        total_spaces=total_spaces,
        candidates_found=candidates_found,
        selected_for_purge=len(selected_tokens),
        purged_count=len(remove_ids),
        output_ifc=output_path.name,
        report_csv=report_path.name,
    )


def package_outputs(session_root: Path, artifacts: Sequence[str], zip_name: str) -> str:
    zip_path = session_root / zip_name
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for artifact in artifacts:
            file_path = session_root / os.path.basename(artifact)
            if file_path.exists() and file_path.is_file():
                zf.write(file_path, arcname=file_path.name)
    return zip_path.name


def result_to_log_payload(scan: ScanResult | PurgeResult) -> str:
    return json.dumps(asdict(scan), sort_keys=True)
