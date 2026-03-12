import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ifcopenshell
import ifcopenshell.util.placement
import numpy as np


@dataclass
class TransformRequest:
    current_xyz: Tuple[float, float, float]
    target_xyz: Tuple[float, float, float]
    rotation_deg: float
    tolerance: float = 0.001
    output_suffix: str = "_moved_rotated"
    rotate_about_global_z: bool = True
    preserve_metadata: bool = True


def _translation_matrix(x: float, y: float, z: float) -> np.ndarray:
    return np.array(
        [
            [1.0, 0.0, 0.0, x],
            [0.0, 1.0, 0.0, y],
            [0.0, 0.0, 1.0, z],
            [0.0, 0.0, 0.0, 1.0],
        ],
        dtype=float,
    )


def _rotation_z_matrix(rotation_deg: float) -> np.ndarray:
    radians = math.radians(rotation_deg)
    c = math.cos(radians)
    s = math.sin(radians)
    return np.array(
        [
            [c, -s, 0.0, 0.0],
            [s, c, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
        ],
        dtype=float,
    )


def build_transform_matrix(current_xyz: Tuple[float, float, float], target_xyz: Tuple[float, float, float], rotation_deg: float) -> np.ndarray:
    t1 = _translation_matrix(-current_xyz[0], -current_xyz[1], -current_xyz[2])
    rz = _rotation_z_matrix(rotation_deg)
    t2 = _translation_matrix(target_xyz[0], target_xyz[1], target_xyz[2])
    return t2 @ rz @ t1


def _transform_point(matrix: np.ndarray, point_xyz: Tuple[float, float, float]) -> Tuple[float, float, float]:
    vec = np.array([point_xyz[0], point_xyz[1], point_xyz[2], 1.0], dtype=float)
    out = matrix @ vec
    return float(out[0]), float(out[1]), float(out[2])


def _matrix_to_axis2placement3d(model: ifcopenshell.file, matrix: np.ndarray):
    origin = model.create_entity("IfcCartesianPoint", Coordinates=(float(matrix[0, 3]), float(matrix[1, 3]), float(matrix[2, 3])))
    z_axis = model.create_entity("IfcDirection", DirectionRatios=(float(matrix[0, 2]), float(matrix[1, 2]), float(matrix[2, 2])))
    x_axis = model.create_entity("IfcDirection", DirectionRatios=(float(matrix[0, 0]), float(matrix[1, 0]), float(matrix[2, 0])))
    return model.create_entity("IfcAxis2Placement3D", Location=origin, Axis=z_axis, RefDirection=x_axis)


def _top_level_local_placements(model: ifcopenshell.file) -> List[Any]:
    placements = []
    for lp in model.by_type("IfcLocalPlacement"):
        if getattr(lp, "PlacementRelTo", None) is None:
            placements.append(lp)
    return placements


def _placement_location(lp: Any) -> Optional[Tuple[float, float, float]]:
    rel = getattr(lp, "RelativePlacement", None)
    loc = getattr(rel, "Location", None) if rel else None
    if not loc:
        return None
    coords = list(getattr(loc, "Coordinates", ()) or ())
    coords += [0.0] * max(0, 3 - len(coords))
    return float(coords[0] or 0.0), float(coords[1] or 0.0), float(coords[2] or 0.0)


def _first_spatial_snapshot(model: ifcopenshell.file, cls: str) -> Optional[Dict[str, Any]]:
    items = model.by_type(cls)
    if not items:
        return None
    ent = items[0]
    lp = getattr(ent, "ObjectPlacement", None)
    return {
        "entity_id": int(ent.id()),
        "name": getattr(ent, "Name", None),
        "placement_xyz": _placement_location(lp),
    }


def _map_conversion_summary(model: ifcopenshell.file) -> Dict[str, Any]:
    mcs = model.by_type("IfcMapConversion")
    details: List[Dict[str, Any]] = []
    for mc in mcs:
        details.append(
            {
                "id": int(mc.id()),
                "eastings": getattr(mc, "Eastings", None),
                "northings": getattr(mc, "Northings", None),
                "orthogonal_height": getattr(mc, "OrthogonalHeight", None),
                "x_axis_abscissa": getattr(mc, "XAxisAbscissa", None),
                "x_axis_ordinate": getattr(mc, "XAxisOrdinate", None),
            }
        )
    return {
        "count": len(mcs),
        "metadata_updated": False,
        "note": "MapConversion / georeferencing metadata was inspected and left unchanged in v1.",
        "entries": details,
    }


def transform_ifc_file(input_path: str, output_path: str, req: TransformRequest, logger) -> Dict[str, Any]:
    if not req.rotate_about_global_z:
        raise ValueError("This v1 endpoint only supports rotation about global Z")

    model = ifcopenshell.open(input_path)
    transform_matrix = build_transform_matrix(req.current_xyz, req.target_xyz, req.rotation_deg)
    logger.info("IFC Move/Rotate: detected %s total IfcLocalPlacement entities", len(model.by_type("IfcLocalPlacement")))

    site_before = _first_spatial_snapshot(model, "IfcSite")
    building_before = _first_spatial_snapshot(model, "IfcBuilding")
    map_conversion = _map_conversion_summary(model)

    updated = 0
    skipped = 0
    top_level = _top_level_local_placements(model)
    logger.info("IFC Move/Rotate: top-level placements to update=%s", len(top_level))

    for lp in top_level:
        try:
            local_matrix = np.array(ifcopenshell.util.placement.get_local_placement(lp), dtype=float)
            new_world = transform_matrix @ local_matrix
            lp.RelativePlacement = _matrix_to_axis2placement3d(model, new_world)
            updated += 1
        except Exception as exc:
            skipped += 1
            logger.warning("IFC Move/Rotate: skipped placement id=%s (%s)", int(lp.id()), exc)

    site_after = _first_spatial_snapshot(model, "IfcSite")
    building_after = _first_spatial_snapshot(model, "IfcBuilding")

    transformed_ref = _transform_point(transform_matrix, req.current_xyz)
    residual_vec = (
        transformed_ref[0] - req.target_xyz[0],
        transformed_ref[1] - req.target_xyz[1],
        transformed_ref[2] - req.target_xyz[2],
    )
    residual_abs = math.sqrt(residual_vec[0] ** 2 + residual_vec[1] ** 2 + residual_vec[2] ** 2)

    model.write(output_path)

    return {
        "input_file": os.path.basename(input_path),
        "output_file": os.path.basename(output_path),
        "updated_top_level_placements": updated,
        "skipped_placements": skipped,
        "transform": {
            "order": "T = T2 * Rz * T1",
            "rotation_convention": "Positive angle is counter-clockwise when looking down +Z toward origin.",
            "current_xyz": req.current_xyz,
            "target_xyz": req.target_xyz,
            "rotation_deg": req.rotation_deg,
            "matrix": [[float(v) for v in row] for row in transform_matrix],
            "translation_component": [float(transform_matrix[0, 3]), float(transform_matrix[1, 3]), float(transform_matrix[2, 3])],
        },
        "validation": {
            "tolerance": req.tolerance,
            "transformed_reference_xyz": transformed_ref,
            "residual_delta_xyz": residual_vec,
            "residual_distance": residual_abs,
            "status": "PASS" if residual_abs <= req.tolerance else "FAIL",
        },
        "spatial_snapshots": {
            "site_before": site_before,
            "site_after": site_after,
            "building_before": building_before,
            "building_after": building_after,
        },
        "georeferencing": map_conversion,
        "notes": [
            "Placements updated at top-level IfcLocalPlacement roots to preserve relative child placement chains.",
            "IfcMapConversion and related georeferencing metadata were not modified.",
        ],
    }
