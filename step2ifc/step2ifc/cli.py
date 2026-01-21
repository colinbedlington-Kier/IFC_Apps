from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
import uuid

from step2ifc.config import ConversionConfig
from step2ifc.auto import auto_convert
from step2ifc.geometry import GeometryProcessor
from step2ifc.ifc_writer import IfcWriter
from step2ifc.io_step import StepReader
from step2ifc.logging import configure_logging, log_event
from step2ifc.mapping import MappingEngine, PartContext
from step2ifc.qc import PartQcResult, QcReporter


def compute_source_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="step2ifc")
    subparsers = parser.add_subparsers(dest="command")

    convert = subparsers.add_parser("convert")
    convert.add_argument("--in", dest="input_path", required=True)
    convert.add_argument("--out", dest="output_path", required=True)
    convert.add_argument("--schema", default="IFC4")
    convert.add_argument("--units", default="mm")
    convert.add_argument("--project", default="Project")
    convert.add_argument("--site", default="Site")
    convert.add_argument("--building", default="Building")
    convert.add_argument("--storey", default="Storey")
    convert.add_argument("--geom", choices=["brep", "mesh"], default="brep")
    convert.add_argument("--mesh-deflection", type=float, default=0.5)
    convert.add_argument("--mesh-angle", type=float, default=0.5)
    convert.add_argument("--merge-by-name", action="store_true")
    convert.add_argument("--split-by-assembly", action="store_true")
    convert.add_argument("--default-type", default="IfcBuildingElementProxy")
    convert.add_argument("--class-map", dest="class_map")
    convert.add_argument("--log", dest="log_path")

    auto = subparsers.add_parser("auto")
    auto.add_argument("--in", dest="input_path", required=True)
    auto.add_argument("--out", dest="output_path", required=True)
    auto.add_argument("--schema", default="IFC4")
    return parser


def run_convert(args: argparse.Namespace) -> int:
    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    run_id = uuid.uuid4().hex

    log_path = Path(args.log_path) if args.log_path else output_path.with_suffix(".log.jsonl")
    logger = configure_logging(log_path)
    log_event(logger, "conversion_start", {"run_id": run_id, "input": str(input_path)})

    config = ConversionConfig()
    if args.class_map:
        config = ConversionConfig.load(Path(args.class_map))

    config.schema = args.schema
    config.units = args.units
    config.project = args.project
    config.site = args.site
    config.building = args.building
    config.storey = args.storey
    config.geom = args.geom
    config.mesh_deflection = args.mesh_deflection
    config.mesh_angle = args.mesh_angle
    config.merge_by_name = args.merge_by_name
    config.split_by_assembly = args.split_by_assembly
    config.default_type = args.default_type

    source_hash = compute_source_hash(input_path)

    step_reader = StepReader()
    geometry = GeometryProcessor()
    mapping = MappingEngine(config)
    writer = IfcWriter(schema=config.schema, units=config.units)
    writer.configure_hierarchy(config.project, config.site, config.building, config.storey)

    parts = step_reader.read(input_path)
    qc_parts: List[PartQcResult] = []
    assembly_objects: Dict[str, object] = {}
    seen_names: Dict[str, str] = {}

    for part in parts:
        assembly_path = part.label_path
        try:
            normalized_name = mapping.normalize_name(part.name)
            if config.merge_by_name and normalized_name in seen_names:
                qc_parts.append(
                    PartQcResult(
                        name=part.name,
                        assembly_path=assembly_path,
                        converted=False,
                        reason=f"Merged by name with {seen_names[normalized_name]}",
                    )
                )
                log_event(
                    logger,
                    "part_merged",
                    {"run_id": run_id, "name": part.name, "assembly": assembly_path},
                )
                continue
            seen_names[normalized_name] = assembly_path
            metrics = geometry.validate_and_heal(part.shape)
            if config.geom == "mesh":
                geometry.mesh(part.shape, config.mesh_deflection, config.mesh_angle)
            vertices, faces = geometry.triangulate(part.shape)
            if config.geom == "brep":
                representation = writer.add_brep_representation(part.shape)
                if representation is None:
                    representation = writer.add_mesh_representation(vertices, faces)
            else:
                representation = writer.add_mesh_representation(vertices, faces)

            context = PartContext(
                part=part,
                source_hash=source_hash,
                assembly_path=assembly_path,
                project_key=config.project,
                metadata={
                    **config.metadata_defaults,
                    "Layer": part.layer or "",
                    "Color": str(part.color) if part.color else "",
                    "GeometryArchetype": "unknown",
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
            base_pset = {
                "SourceFile": {"value": str(input_path), "data_type": "IfcLabel"},
                "SourceFormat": {"value": "STEP", "data_type": "IfcLabel"},
                "SourceHash": {"value": source_hash, "data_type": "IfcLabel"},
                "AssemblyPath": {"value": assembly_path, "data_type": "IfcLabel"},
                "OriginalName": {"value": part.name, "data_type": "IfcLabel"},
                "ConversionTimestamp": {"value": writer.timestamp(), "data_type": "IfcDateTime"},
            }
            writer.add_pset(element, "Pset_Source", base_pset)
            if mapping_result.properties:
                writer.add_pset(element, "Pset_Mapped", mapping_result.properties)
            if mapping_result.classification:
                writer.add_classification(
                    element,
                    mapping_result.classification["system"],
                    mapping_result.classification["code"],
                    mapping_result.classification["title"],
                )

            if config.split_by_assembly:
                assembly_key = "/".join(assembly_path.split("/")[:-1])
                if assembly_key:
                    assembly = assembly_objects.get(assembly_key)
                    if assembly is None:
                        assembly_tag = hashlib.md5(assembly_key.encode("utf-8")).hexdigest()[:12]
                        assembly = writer.add_assembly(assembly_key, assembly_tag)
                        assembly_objects[assembly_key] = assembly
                    writer.assign_aggregation(assembly, [element])

            qc_parts.append(
                PartQcResult(
                    name=part.name,
                    assembly_path=assembly_path,
                    converted=True,
                    reason=None,
                    bbox=list(metrics.bbox),
                    volume=metrics.volume,
                    repaired=metrics.repaired,
                )
            )
            log_event(
                logger,
                "part_converted",
                {"run_id": run_id, "name": part.name, "assembly": assembly_path, "guid": guid},
            )
        except Exception as exc:  # pragma: no cover - defensive
            qc_parts.append(
                PartQcResult(
                    name=part.name,
                    assembly_path=assembly_path,
                    converted=False,
                    reason=str(exc),
                )
            )
            log_event(
                logger,
                "part_failed",
                {"run_id": run_id, "name": part.name, "assembly": assembly_path, "error": str(exc)},
            )
            continue

    writer.write(output_path)

    qc_reporter = QcReporter()
    report = qc_reporter.build_report(
        schema=config.schema,
        units=config.units,
        source_hash=source_hash,
        parts=qc_parts,
        mesh_settings={"deflection": config.mesh_deflection, "angle": config.mesh_angle},
    )
    report.validation = {"basic": qc_reporter.basic_ifc_checks(output_path)}
    validation = qc_reporter.run_ifc_validation(output_path)
    if validation:
        report.validation["ifcopenshell"] = validation
    qc_reporter.save(report, output_path)

    log_event(logger, "conversion_complete", {"run_id": run_id, "output": str(output_path)})
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "convert":
        return run_convert(args)
    if args.command == "auto":
        return auto_convert(Path(args.input_path), Path(args.output_path), schema=args.schema)
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
