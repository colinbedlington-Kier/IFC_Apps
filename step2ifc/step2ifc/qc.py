from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from step2ifc.logging import get_logger

import importlib.util

if importlib.util.find_spec("ifcopenshell"):
    import ifcopenshell
    import ifcopenshell.validate
else:  # pragma: no cover - runtime dependency check
    ifcopenshell = None


@dataclass
class PartQcResult:
    name: str
    assembly_path: str
    converted: bool
    reason: Optional[str]
    bbox: Optional[List[float]] = None
    volume: Optional[float] = None
    repaired: bool = False


@dataclass
class QcReport:
    schema: str
    units: str
    source_hash: str
    total_parts: int
    converted_parts: int
    failures: int
    bounding_boxes: List[List[float]] = field(default_factory=list)
    volumes: List[float] = field(default_factory=list)
    invalid_solids: int = 0
    parts: List[PartQcResult] = field(default_factory=list)
    validation: Optional[Dict[str, Any]] = None
    mesh_settings: Optional[Dict[str, float]] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


class QcReporter:
    def __init__(self) -> None:
        self.logger = get_logger()

    def build_report(
        self,
        schema: str,
        units: str,
        source_hash: str,
        parts: List[PartQcResult],
        mesh_settings: Optional[Dict[str, float]] = None,
    ) -> QcReport:
        converted = sum(1 for part in parts if part.converted)
        failures = sum(1 for part in parts if not part.converted)
        bbox_list = [part.bbox for part in parts if part.bbox]
        volumes = [part.volume for part in parts if part.volume is not None]
        invalid_solids = sum(1 for part in parts if part.volume is None)
        return QcReport(
            schema=schema,
            units=units,
            source_hash=source_hash,
            total_parts=len(parts),
            converted_parts=converted,
            failures=failures,
            bounding_boxes=bbox_list,
            volumes=volumes,
            invalid_solids=invalid_solids,
            parts=parts,
            mesh_settings=mesh_settings,
        )

    def run_ifc_validation(self, ifc_path: Path) -> Optional[Dict[str, Any]]:
        if ifcopenshell is None or not hasattr(ifcopenshell, "validate"):
            self.logger.warning("ifcopenshell validation not available")
            return None
        try:
            report = ifcopenshell.validate.validate(ifc_path)
        except Exception as exc:  # pragma: no cover - external dependency
            self.logger.warning("ifcopenshell validation failed", extra={"error": str(exc)})
            return None
        return report

    def basic_ifc_checks(self, ifc_path: Path) -> Dict[str, Any]:
        checks = {"project": False, "site": False, "building": False, "storey": False, "has_geometry": False}
        if ifcopenshell is None:
            return checks
        model = ifcopenshell.open(str(ifc_path))
        checks["project"] = bool(model.by_type("IfcProject"))
        checks["site"] = bool(model.by_type("IfcSite"))
        checks["building"] = bool(model.by_type("IfcBuilding"))
        checks["storey"] = bool(model.by_type("IfcBuildingStorey"))
        checks["has_geometry"] = bool(model.by_type("IfcShapeRepresentation"))
        return checks

    def save(self, report: QcReport, base_path: Path) -> None:
        json_path = base_path.with_suffix(".qc.json")
        text_path = base_path.with_suffix(".qc.txt")
        json_path.write_text(json.dumps(report, default=lambda o: o.__dict__, indent=2), encoding="utf-8")

        lines = [
            f"QC Report - {report.created_at}",
            f"Schema: {report.schema}",
            f"Units: {report.units}",
            f"Source hash: {report.source_hash}",
            f"Total parts: {report.total_parts}",
            f"Converted: {report.converted_parts}",
            f"Failures: {report.failures}",
            f"Invalid solids: {report.invalid_solids}",
        ]
        if report.volumes:
            top_volumes = sorted(report.volumes, reverse=True)[:10]
            lines.append(f"Top volumes: {top_volumes}")
        text_path.write_text("\n".join(lines), encoding="utf-8")
        self.logger.info("QC report written", extra={"json": str(json_path), "text": str(text_path)})
