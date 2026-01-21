from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class NameNormalizationRule:
    pattern: str
    replacement: str


@dataclass
class PropertyMapping:
    name: str
    value: str
    data_type: str = "IfcLabel"


@dataclass
class ClassificationMapping:
    system: str
    code: str
    title: Optional[str] = None


@dataclass
class TypeMappingRule:
    match_name_regex: Optional[str] = None
    match_assembly_prefix: Optional[str] = None
    ifc_class: str = "IfcBuildingElementProxy"
    object_type: Optional[str] = None
    classification: Optional[ClassificationMapping] = None
    properties: List[PropertyMapping] = field(default_factory=list)


@dataclass
class ConversionConfig:
    name_normalization: List[NameNormalizationRule] = field(default_factory=list)
    type_mappings: List[TypeMappingRule] = field(default_factory=list)
    default_type: str = "IfcBuildingElementProxy"
    merge_by_name: bool = False
    split_by_assembly: bool = False
    schema: str = "IFC4"
    units: str = "mm"
    project: str = "Project"
    site: str = "Site"
    building: str = "Building"
    storey: str = "Storey"
    geom: str = "brep"
    mesh_deflection: float = 0.5
    mesh_angle: float = 0.5
    metadata_defaults: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def load(path: Path) -> "ConversionConfig":
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        name_norm = [NameNormalizationRule(**item) for item in data.get("name_normalization", [])]
        type_mappings: List[TypeMappingRule] = []
        for item in data.get("type_mappings", []):
            classification = None
            if item.get("classification"):
                classification = ClassificationMapping(**item["classification"])
            properties = [PropertyMapping(**prop) for prop in item.get("properties", [])]
            type_mappings.append(
                TypeMappingRule(
                    match_name_regex=item.get("match_name_regex"),
                    match_assembly_prefix=item.get("match_assembly_prefix"),
                    ifc_class=item.get("ifc_class", "IfcBuildingElementProxy"),
                    object_type=item.get("object_type"),
                    classification=classification,
                    properties=properties,
                )
            )
        return ConversionConfig(
            name_normalization=name_norm,
            type_mappings=type_mappings,
            default_type=data.get("default_type", "IfcBuildingElementProxy"),
            merge_by_name=bool(data.get("merge_by_name", False)),
            split_by_assembly=bool(data.get("split_by_assembly", False)),
            schema=data.get("schema", "IFC4"),
            units=data.get("units", "mm"),
            project=data.get("project", "Project"),
            site=data.get("site", "Site"),
            building=data.get("building", "Building"),
            storey=data.get("storey", "Storey"),
            geom=data.get("geom", "brep"),
            mesh_deflection=float(data.get("mesh_deflection", 0.5)),
            mesh_angle=float(data.get("mesh_angle", 0.5)),
            metadata_defaults=data.get("properties", {}).get("defaults", {}),
        )


def load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
