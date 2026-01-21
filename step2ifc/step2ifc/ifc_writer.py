from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import uuid

from step2ifc.logging import get_logger

import importlib.util

if importlib.util.find_spec("ifcopenshell"):
    import ifcopenshell
    import ifcopenshell.api
    import ifcopenshell.guid
else:  # pragma: no cover - runtime dependency check
    ifcopenshell = None


@dataclass
class IfcContext:
    file: "ifcopenshell.file"
    project: object
    site: object
    building: object
    storey: object
    context: object


class IfcWriter:
    def __init__(self, schema: str, units: str) -> None:
        if ifcopenshell is None:
            raise RuntimeError("ifcopenshell is required for IFC writing")
        self.logger = get_logger()
        self.schema = schema
        self.units = units
        self.file = ifcopenshell.file(schema=schema)
        self.context = self._setup_project()

    def _setup_project(self) -> IfcContext:
        project = ifcopenshell.api.run("root.create_entity", self.file, ifc_class="IfcProject", name="Project")
        site = ifcopenshell.api.run("root.create_entity", self.file, ifc_class="IfcSite", name="Site")
        building = ifcopenshell.api.run("root.create_entity", self.file, ifc_class="IfcBuilding", name="Building")
        storey = ifcopenshell.api.run("root.create_entity", self.file, ifc_class="IfcBuildingStorey", name="Storey")
        ifcopenshell.api.run(
            "aggregate.assign_object",
            self.file,
            relating_object=project,
            products=[site],
        )
        ifcopenshell.api.run("aggregate.assign_object", self.file, relating_object=site, products=[building])
        ifcopenshell.api.run(
            "aggregate.assign_object",
            self.file,
            relating_object=building,
            products=[storey],
        )

        context = ifcopenshell.api.run(
            "context.add_context",
            self.file,
            context_identifier="Body",
            context_type="Model",
        )
        ifcopenshell.api.run("unit.assign_unit", self.file, length_units=self.units)
        return IfcContext(file=self.file, project=project, site=site, building=building, storey=storey, context=context)

    def configure_hierarchy(self, project: str, site: str, building: str, storey: str) -> None:
        self.context.project.Name = project
        self.context.site.Name = site
        self.context.building.Name = building
        self.context.storey.Name = storey

    def add_element(
        self,
        ifc_class: str,
        name: str,
        object_type: Optional[str],
        tag: str,
        representation,
    ) -> object:
        element = ifcopenshell.api.run("root.create_entity", self.file, ifc_class=ifc_class, name=name)
        element.Tag = tag
        if object_type:
            element.ObjectType = object_type
        if representation:
            ifcopenshell.api.run(
                "geometry.assign_representation",
                self.file,
                product=element,
                representation=representation,
            )
        ifcopenshell.api.run(
            "spatial.assign_container",
            self.file,
            products=[element],
            relating_structure=self.context.storey,
        )
        return element

    def add_assembly(self, name: str, tag: str) -> object:
        assembly = ifcopenshell.api.run(
            "root.create_entity",
            self.file,
            ifc_class="IfcElementAssembly",
            name=name,
        )
        assembly.Tag = tag
        ifcopenshell.api.run(
            "spatial.assign_container",
            self.file,
            products=[assembly],
            relating_structure=self.context.storey,
        )
        return assembly

    def assign_aggregation(self, parent: object, children: list[object]) -> None:
        ifcopenshell.api.run(
            "aggregate.assign_object",
            self.file,
            relating_object=parent,
            products=children,
        )

    def add_brep_representation(self, shape: object) -> Optional[object]:
        """Try to add a BRep representation using ifcopenshell API.

        If the API is unavailable, return None and allow the caller to fallback to mesh.
        """
        try:
            return ifcopenshell.api.run(
                "geometry.add_brep_representation",
                self.file,
                context=self.context.context,
                shape=shape,
            )
        except Exception as exc:  # pragma: no cover - adapter fallback
            self.logger.warning("BRep representation failed; falling back to mesh", extra={"error": str(exc)})
            return None

    def add_mesh_representation(self, vertices, faces) -> object:
        return ifcopenshell.api.run(
            "geometry.add_mesh_representation",
            self.file,
            context=self.context.context,
            vertices=vertices,
            faces=faces,
        )

    def add_pset(self, element: object, name: str, properties: Dict[str, Dict[str, str]]) -> None:
        pset = ifcopenshell.api.run("pset.add_pset", self.file, product=element, name=name)
        for prop_name, payload in properties.items():
            ifcopenshell.api.run(
                "pset.edit_pset",
                self.file,
                pset=pset,
                properties={prop_name: payload["value"]},
            )

    def add_classification(self, element: object, system: str, code: str, title: str) -> None:
        classification = ifcopenshell.api.run(
            "classification.add_classification",
            self.file,
            name=system,
        )
        reference = ifcopenshell.api.run(
            "classification.add_reference",
            self.file,
            identification=code,
            name=title,
            classification=classification,
        )
        ifcopenshell.api.run(
            "classification.assign_classification",
            self.file,
            products=[element],
            classification=reference,
        )

    def new_guid(self, seed: str) -> str:
        guid = uuid.uuid5(uuid.NAMESPACE_URL, seed)
        return ifcopenshell.guid.compress(guid.hex)

    def write(self, path: Path) -> None:
        self.file.write(str(path))
        self.logger.info("IFC written", extra={"path": str(path), "schema": self.schema})

    @staticmethod
    def timestamp() -> str:
        return datetime.utcnow().isoformat() + "Z"
