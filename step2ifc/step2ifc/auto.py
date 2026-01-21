from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import hashlib
import statistics

import yaml

from step2ifc.config import ConversionConfig, TypeMappingRule
from step2ifc.geometry import GeometryProcessor
from step2ifc.heuristics import geometry_archetype, infer_class, normalize_tokens, token_signature
from step2ifc.ifc_writer import IfcWriter
from step2ifc.io_step import StepReader
from step2ifc.logging import configure_logging, log_inference, log_event
from step2ifc.mapping import MappingEngine, PartContext
from step2ifc.qc import PartQcResult, QcReporter


@dataclass
class UnitInference:
    unit: str
    confidence: float
    rationale: List[str]


def infer_units(header_hint: Optional[str], diagonals: List[float]) -> UnitInference:
    rationale: List[str] = []
    if header_hint:
        rationale.append(f"STEP header unit hint: {header_hint}")
        if header_hint == "mm":
            return UnitInference(unit="mm", confidence=0.9, rationale=rationale)
        if header_hint == "m":
            return UnitInference(unit="m", confidence=0.9, rationale=rationale)
        if header_hint == "in":
            return UnitInference(unit="in", confidence=0.9, rationale=rationale)
    if not diagonals:
        return UnitInference(unit="mm", confidence=0.4, rationale=rationale + ["No geometry extents available; defaulted to mm."])
    median = statistics.median(diagonals)
    rationale.append(f"Median bbox diagonal: {median:.4f}")
    if 0.001 <= median <= 5:
        return UnitInference(unit="m", confidence=0.7, rationale=rationale + ["Diagonal suggests meter-scale parts."])
    if 0.1 <= median <= 200:
        return UnitInference(unit="in", confidence=0.6, rationale=rationale + ["Diagonal suggests inch-scale parts."])
    if 1 <= median <= 5000:
        return UnitInference(unit="mm", confidence=0.7, rationale=rationale + ["Diagonal suggests millimeter-scale parts."])
    return UnitInference(unit="mm", confidence=0.4, rationale=rationale + ["Ambiguous scale; defaulted to mm."])


def extract_unit_hint(step_path: Path) -> Optional[str]:
    header = step_path.read_text(encoding="utf-8", errors="ignore")[:50000].upper()
    if "INCH" in header:
        return "in"
    if "MILLI" in header:
        return "mm"
    if "METRE" in header or "METER" in header:
        return "m"
    return None


def write_autogen_mapping(
    path: Path,
    source_hash: str,
    unit_inference: UnitInference,
    rules: List[Dict[str, object]],
    schema: str,
) -> None:
    payload = {
        "generated_at": QcReporter().build_report(schema, unit_inference.unit, source_hash, []).created_at,
        "source_hash": source_hash,
        "schema": schema,
        "unit_inference": {
            "unit": unit_inference.unit,
            "confidence": unit_inference.confidence,
            "rationale": unit_inference.rationale,
        },
        "rules": rules,
        "default_rule": {"ifc_class": "IfcBuildingElementProxy"},
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def build_config_from_rules(unit: str, schema: str, rules: List[Dict[str, object]]) -> ConversionConfig:
    type_mappings: List[TypeMappingRule] = []
    for rule in rules:
        match = rule.get("match", {})
        assign = rule.get("assign", {})
        type_mappings.append(
            TypeMappingRule(
                match_name_regex=match.get("name_regex"),
                match_assembly_prefix=match.get("path_prefix"),
                match_path_regex=match.get("path_regex"),
                match_layer=match.get("layer"),
                match_color=match.get("color"),
                geometry_archetype=match.get("geometry_archetype"),
                ifc_class=assign.get("ifc_class", "IfcBuildingElementProxy"),
                object_type=assign.get("object_type"),
            )
        )
    return ConversionConfig(schema=schema, units=unit, type_mappings=type_mappings)


def auto_convert(input_step: Path, output_ifc: Path, schema: str = "IFC4") -> int:
    run_id = hashlib.md5(str(output_ifc).encode("utf-8")).hexdigest()
    log_path = output_ifc.with_suffix(".log.jsonl")
    logger = configure_logging(log_path)
    log_event(logger, "auto_conversion_start", {"run_id": run_id, "input": str(input_step)})

    step_reader = StepReader()
    geometry = GeometryProcessor()

    parts = sorted(step_reader.read(input_step), key=lambda part: part.label_path)

    diagonals: List[float] = []
    part_metrics: Dict[str, Tuple[Tuple[float, float, float, float, float, float], Optional[float]]] = {}
    for part in parts:
        metrics = geometry.validate_and_heal(part.shape)
        bbox = metrics.bbox
        diag = (bbox[3] - bbox[0]) ** 2 + (bbox[4] - bbox[1]) ** 2 + (bbox[5] - bbox[2]) ** 2
        diagonals.append(diag ** 0.5)
        part_metrics[part.label_path] = (bbox, metrics.volume)

    unit_inference = infer_units(extract_unit_hint(input_step), diagonals)
    log_inference(
        logger,
        "unit_inferred",
        unit_inference.confidence,
        {"unit": unit_inference.unit, "rationale": unit_inference.rationale},
    )

    source_hash = hashlib.sha256(input_step.read_bytes()).hexdigest()

    cluster_map: Dict[str, Dict[str, object]] = {}
    part_candidates: Dict[str, List[Dict[str, float]]] = {}

    for part in parts:
        bbox, _ = part_metrics[part.label_path]
        tokens = normalize_tokens(part.name, part.label_path)
        archetype = geometry_archetype(bbox)
        inference = infer_class(tokens, archetype)
        part_candidates[part.label_path] = inference.candidates
        signature = token_signature(tokens)
        cluster_key = f"{signature}|{archetype}|{part.layer or ''}|{part.color or ''}"
        cluster = cluster_map.setdefault(
            cluster_key,
            {
                "tokens": signature,
                "archetype": archetype,
                "layer": part.layer,
                "color": str(part.color) if part.color else None,
                "scores": {},
                "count": 0,
            },
        )
        scores = cluster["scores"]
        scores[inference.ifc_class] = scores.get(inference.ifc_class, 0.0) + inference.confidence
        cluster["count"] = cluster["count"] + 1

    rules: List[Dict[str, object]] = []
    for cluster_key in sorted(cluster_map.keys()):
        cluster = cluster_map[cluster_key]
        scores: Dict[str, float] = cluster["scores"]
        ifc_class = max(scores.items(), key=lambda item: item[1])[0]
        confidence = round(scores[ifc_class] / max(cluster["count"], 1), 3)
        name_regex = None
        if cluster["tokens"] != "UNSPECIFIED":
            name_regex = f".*({cluster['tokens'].replace('_', '|')}).*"
        rules.append(
            {
                "match": {
                    "name_regex": name_regex,
                    "layer": cluster["layer"],
                    "color": cluster["color"],
                    "geometry_archetype": cluster["archetype"],
                },
                "assign": {
                    "ifc_class": ifc_class,
                    "object_type": ifc_class.replace("Ifc", ""),
                },
                "confidence": confidence,
            }
        )
        log_inference(
            logger,
            "class_inferred",
            confidence,
            {"cluster": cluster_key, "ifc_class": ifc_class},
        )

    mapping_path = output_ifc.parent / "classmap.autogen.yaml"
    write_autogen_mapping(mapping_path, source_hash, unit_inference, rules, schema)

    config = build_config_from_rules(unit_inference.unit, schema, rules)
    config.project = "Project"
    config.site = "Site"
    config.building = "Building"
    config.storey = "Storey"

    writer = IfcWriter(schema=config.schema, units=config.units)
    writer.configure_hierarchy(config.project, config.site, config.building, config.storey)
    mapping = MappingEngine(config)
    qc_parts: List[PartQcResult] = []

    for part in parts:
        bbox, volume = part_metrics[part.label_path]
        archetype = geometry_archetype(bbox)
        try:
            if config.geom == "mesh":
                geometry.mesh(part.shape, config.mesh_deflection, config.mesh_angle)
            vertices, faces = geometry.triangulate(part.shape)
            representation = writer.add_brep_representation(part.shape)
            if representation is None:
                representation = writer.add_mesh_representation(vertices, faces)

            context = PartContext(
                part=part,
                source_hash=source_hash,
                assembly_path=part.label_path,
                project_key=config.project,
                metadata={
                    "Layer": part.layer or "",
                    "Color": str(part.color) if part.color else "",
                    "GeometryArchetype": archetype,
                },
            )
            mapping_result = mapping.map_part(context)
            guid = writer.new_guid(mapping.stable_guid_seed(context))
            element = writer.add_element(
                ifc_class=mapping_result.ifc_class,
                name=mapping_result.name,
                object_type=mapping_result.object_type,
                tag=mapping_result.tag,
                representation=representation,
            )
            element.GlobalId = guid
            writer.add_pset(
                element,
                "Pset_Source",
                {
                    "SourceFile": {"value": str(input_step), "data_type": "IfcLabel"},
                    "SourceFormat": {"value": "STEP", "data_type": "IfcLabel"},
                    "SourceHash": {"value": source_hash, "data_type": "IfcLabel"},
                    "AssemblyPath": {"value": part.label_path, "data_type": "IfcLabel"},
                    "OriginalName": {"value": part.name, "data_type": "IfcLabel"},
                    "ConversionTimestamp": {"value": writer.timestamp(), "data_type": "IfcDateTime"},
                },
            )
            qc_parts.append(
                PartQcResult(
                    name=part.name,
                    assembly_path=part.label_path,
                    converted=True,
                    reason=None,
                    bbox=list(bbox),
                    volume=volume,
                    repaired=False,
                    candidates=part_candidates.get(part.label_path),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            qc_parts.append(
                PartQcResult(
                    name=part.name,
                    assembly_path=part.label_path,
                    converted=False,
                    reason=str(exc),
                    candidates=part_candidates.get(part.label_path),
                )
            )
            log_event(logger, "part_failed", {"run_id": run_id, "name": part.name, "error": str(exc)})
            continue

    writer.write(output_ifc)

    qc_reporter = QcReporter()
    assumptions = [
        {
            "assumption": "Units inferred automatically",
            "confidence": unit_inference.confidence,
            "rationale": unit_inference.rationale,
        },
        {
            "assumption": "Right-handed coordinate system; no axis transforms applied",
            "confidence": 0.8,
        },
    ]
    report = qc_reporter.build_report(
        schema=config.schema,
        units=config.units,
        source_hash=source_hash,
        parts=qc_parts,
        assumptions=assumptions,
        mesh_settings={"deflection": config.mesh_deflection, "angle": config.mesh_angle},
    )
    report.validation = {"basic": qc_reporter.basic_ifc_checks(output_ifc)}
    qc_reporter.save(report, output_ifc)
    log_event(logger, "auto_conversion_complete", {"run_id": run_id, "output": str(output_ifc)})
    return 0
