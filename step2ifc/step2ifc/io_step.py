from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from step2ifc.logging import get_logger

import importlib.util

if importlib.util.find_spec("OCC.Core.STEPControl"):
    from OCC.Core.STEPControl import STEPControl_Reader
    from OCC.Core.STEPCAFControl import STEPCAFControl_Reader
    from OCC.Core.TCollection import TCollection_ExtendedString
    from OCC.Core.TDF import TDF_LabelSequence
    from OCC.Core.TDocStd import TDocStd_Document
    from OCC.Core.XCAFDoc import (
        XCAFDoc_DocumentTool_ColorTool,
        XCAFDoc_DocumentTool_LayerTool,
        XCAFDoc_DocumentTool_ShapeTool,
    )
else:  # pragma: no cover - runtime dependency check
    STEPControl_Reader = None
    STEPCAFControl_Reader = None


@dataclass
class StepPart:
    name: str
    label_path: str
    shape: object
    color: Optional[tuple] = None
    layer: Optional[str] = None


class StepReader:
    def __init__(self) -> None:
        self.logger = get_logger()

    def read(self, path: Path) -> List[StepPart]:
        if STEPCAFControl_Reader is None:
            raise RuntimeError("pythonocc-core is required for STEP reading")

        self.logger.info("Reading STEP file", extra={"path": str(path)})
        doc = TDocStd_Document(TCollection_ExtendedString("step"))
        reader = STEPCAFControl_Reader()
        reader.SetColorMode(True)
        reader.SetLayerMode(True)
        reader.SetNameMode(True)
        status = reader.ReadFile(str(path))
        if status != 1:
            raise RuntimeError(f"STEP read failed with status {status}")

        transfer_ok = reader.Transfer(doc)
        if not transfer_ok:
            self.logger.warning("STEPCAF transfer failed; falling back to STEPControl")
            return self._fallback_stepcontrol(path)

        shape_tool = XCAFDoc_DocumentTool_ShapeTool(doc.Main())
        color_tool = XCAFDoc_DocumentTool_ColorTool(doc.Main())
        layer_tool = XCAFDoc_DocumentTool_LayerTool(doc.Main())

        labels = TDF_LabelSequence()
        shape_tool.GetFreeShapes(labels)
        parts: List[StepPart] = []
        for index in range(labels.Length()):
            label = labels.Value(index + 1)
            parts.extend(self._traverse_label(shape_tool, color_tool, layer_tool, label, parent_path=""))
        if not parts:
            self.logger.warning("No assembly structure found; fallback to STEPControl")
            return self._fallback_stepcontrol(path)
        return parts

    def _traverse_label(
        self,
        shape_tool,
        color_tool,
        layer_tool,
        label,
        parent_path: str,
    ) -> List[StepPart]:
        parts: List[StepPart] = []
        name = shape_tool.GetShapeLabelName(label)
        label_name = str(name) if name else "Unnamed"
        label_path = f"{parent_path}/{label_name}" if parent_path else label_name

        if shape_tool.IsAssembly(label):
            children = TDF_LabelSequence()
            shape_tool.GetComponents(label, children)
            for idx in range(children.Length()):
                child = children.Value(idx + 1)
                parts.extend(self._traverse_label(shape_tool, color_tool, layer_tool, child, label_path))
            return parts

        shape = shape_tool.GetShape(label)
        color = self._get_color(color_tool, label)
        layer = self._get_layer(layer_tool, label)
        parts.append(StepPart(name=label_name, label_path=label_path, shape=shape, color=color, layer=layer))
        return parts

    def _get_color(self, color_tool, label) -> Optional[tuple]:
        color = color_tool.GetColor(label)
        if color is None:
            return None
        return (color.Red(), color.Green(), color.Blue())

    def _get_layer(self, layer_tool, label) -> Optional[str]:
        layers = layer_tool.GetLayers(label)
        if layers is None or layers.IsEmpty():
            return None
        name = layer_tool.GetLayerName(layers.First())
        return str(name) if name else None

    def _fallback_stepcontrol(self, path: Path) -> List[StepPart]:
        if STEPControl_Reader is None:
            raise RuntimeError("pythonocc-core is required for STEP reading")
        reader = STEPControl_Reader()
        status = reader.ReadFile(str(path))
        if status != 1:
            raise RuntimeError(f"STEP read failed with status {status}")
        reader.TransferRoots()
        shape = reader.Shape(1)
        if shape.IsNull():
            raise RuntimeError("STEPControl reader returned null shape")
        return [StepPart(name=path.stem, label_path=path.stem, shape=shape)]


# Fallback option if pythonocc-core is unavailable:
# - FreeCAD can be used in headless mode if installed (e.g., `freecadcmd`).
#   This project does not ship a FreeCAD adapter, but the recommended flow is to
#   implement a reader that returns StepPart records with shape handles or
#   triangulated meshes compatible with the IFC writer.
