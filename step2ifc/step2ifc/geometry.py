from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from step2ifc.logging import get_logger

import importlib.util

if importlib.util.find_spec("OCC.Core.BRep"):
    from OCC.Core.BRep import BRep_Tool
    from OCC.Core.BRepBndLib import brepbndlib_Add
    from OCC.Core.BRepCheck import BRepCheck_Analyzer
    from OCC.Core.BRepGProp import brepgprop_VolumeProperties
    from OCC.Core.BRepMesh import BRepMesh_IncrementalMesh
    from OCC.Core.Bnd import Bnd_Box
    from OCC.Core.GProp import GProp_GProps
    from OCC.Core.ShapeFix import ShapeFix_Shape
    from OCC.Core.TopAbs import TopAbs_FACE
    from OCC.Core.TopExp import TopExp_Explorer
else:  # pragma: no cover - runtime dependency check
    BRepCheck_Analyzer = None


@dataclass
class ShapeMetrics:
    bbox: Tuple[float, float, float, float, float, float]
    volume: Optional[float]
    centroid: Optional[Tuple[float, float, float]]
    valid: bool
    repaired: bool
    repair_message: Optional[str]


class GeometryProcessor:
    def __init__(self) -> None:
        self.logger = get_logger()

    def validate_and_heal(self, shape: object) -> ShapeMetrics:
        if BRepCheck_Analyzer is None:
            raise RuntimeError("pythonocc-core is required for geometry processing")

        repaired = False
        repair_message = None

        analyzer = BRepCheck_Analyzer(shape)
        valid = analyzer.IsValid()
        if not valid:
            fixer = ShapeFix_Shape(shape)
            fixer.Perform()
            shape = fixer.Shape()
            repaired = True
            analyzer = BRepCheck_Analyzer(shape)
            valid = analyzer.IsValid()
            repair_message = "ShapeFix_Shape applied"
            self.logger.info("Shape repaired", extra={"message": repair_message})

        bbox = self._compute_bbox(shape)
        volume, centroid = self._compute_volume_and_centroid(shape)
        return ShapeMetrics(
            bbox=bbox,
            volume=volume,
            centroid=centroid,
            valid=valid,
            repaired=repaired,
            repair_message=repair_message,
        )

    def mesh(self, shape: object, deflection: float, angle: float) -> None:
        if BRepMesh_IncrementalMesh is None:
            raise RuntimeError("pythonocc-core is required for meshing")
        mesh = BRepMesh_IncrementalMesh(shape, deflection, False, angle, True)
        mesh.Perform()

    def triangulate(self, shape: object) -> Tuple[list, list]:
        if BRep_Tool is None:
            raise RuntimeError("pythonocc-core is required for triangulation")
        vertices = []
        faces = []
        explorer = TopExp_Explorer(shape, TopAbs_FACE)
        vertex_index = 0
        while explorer.More():
            face = explorer.Current()
            triangulation = BRep_Tool.Triangulation(face, None)
            if triangulation is None:
                explorer.Next()
                continue
            nodes = triangulation.Nodes()
            triangles = triangulation.Triangles()
            node_map = {}
            for idx in range(1, triangulation.NbNodes() + 1):
                point = nodes.Value(idx)
                node_map[idx] = vertex_index
                vertices.append([point.X(), point.Y(), point.Z()])
                vertex_index += 1
            for idx in range(1, triangulation.NbTriangles() + 1):
                tri = triangles.Value(idx)
                n1, n2, n3 = tri.Get()
                faces.append([node_map[n1], node_map[n2], node_map[n3]])
            explorer.Next()
        return vertices, faces

    def _compute_bbox(self, shape: object) -> Tuple[float, float, float, float, float, float]:
        box = Bnd_Box()
        brepbndlib_Add(shape, box)
        xmin, ymin, zmin, xmax, ymax, zmax = box.Get()
        return xmin, ymin, zmin, xmax, ymax, zmax

    def _compute_volume_and_centroid(self, shape: object) -> Tuple[Optional[float], Optional[Tuple[float, float, float]]]:
        props = GProp_GProps()
        try:
            brepgprop_VolumeProperties(shape, props)
        except Exception:
            return None, None
        volume = props.Mass()
        if volume == 0:
            return None, None
        centroid = props.CentreOfMass()
        return volume, (centroid.X(), centroid.Y(), centroid.Z())
