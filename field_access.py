import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import ifcopenshell
from ifcopenshell.guid import new as new_guid

from classification_writer import (
    attach_classification,
    find_classification_value,
)


class FieldKind(str, Enum):
    ATTRIBUTE = "attribute"
    PROPERTY = "property"
    QUANTITY = "quantity"
    CLASSIFICATION = "classification"
    PREDEFINEDTYPE = "predefinedtype"

    @classmethod
    def from_str(cls, value: str) -> "FieldKind":
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.ATTRIBUTE


@dataclass
class FieldDescriptor:
    kind: FieldKind
    attribute_name: Optional[str] = None
    property_name: Optional[str] = None
    pset_name: Optional[str] = None
    quantity_name: Optional[str] = None
    qto_name: Optional[str] = None
    classification_system: Optional[str] = None
    expression: Optional[str] = None
    generated: bool = False

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Any]) -> "FieldDescriptor":
        return cls(
            kind=FieldKind.from_str(mapping.get("kind", "attribute")),
            attribute_name=mapping.get("attribute"),
            property_name=mapping.get("property"),
            pset_name=mapping.get("pset"),
            quantity_name=mapping.get("quantity"),
            qto_name=mapping.get("qto"),
            classification_system=mapping.get("classification_system") or mapping.get("system_name"),
            expression=mapping.get("expression"),
        )

    def path_label(self) -> str:
        if self.kind == FieldKind.ATTRIBUTE:
            return f"Attribute:{self.attribute_name}"
        if self.kind == FieldKind.PROPERTY:
            return f"Pset:{self.pset_name}/{self.property_name}"
        if self.kind == FieldKind.QUANTITY:
            return f"Quantity:{self.qto_name}/{self.quantity_name}"
        if self.kind == FieldKind.CLASSIFICATION:
            return f"Classification:{self.classification_system}"
        if self.kind == FieldKind.PREDEFINEDTYPE:
            return "PredefinedType"
        return "Field"


def _owner_history(model):
    for oh in model.by_type("IfcOwnerHistory"):
        return oh
    org = model.create_entity("IfcOrganization", Name="IFC Toolkit Hub")
    app = model.create_entity(
        "IfcApplication",
        ApplicationDeveloper=org,
        Version="1.0",
        ApplicationFullName="IFC Toolkit Hub",
        ApplicationIdentifier="IFCTH",
    )
    person = model.create_entity("IfcPerson", FamilyName="Toolkit")
    actor = model.create_entity("IfcPersonAndOrganization", ThePerson=person, TheOrganization=org)
    return model.create_entity(
        "IfcOwnerHistory",
        OwningUser=actor,
        OwningApplication=app,
        CreationDate=int(datetime.datetime.utcnow().timestamp()),
    )


def _ensure_pset(model, element, pset_name: str):
    for rel in _property_rels(element):
        if rel.is_a("IfcRelDefinesByProperties"):
            pset = rel.RelatingPropertyDefinition
            if pset and pset.is_a("IfcPropertySet") and getattr(pset, "Name", None) == pset_name:
                return pset
    owner_history = _owner_history(model)
    pset = model.create_entity(
        "IfcPropertySet",
        GlobalId=new_guid(),
        OwnerHistory=owner_history,
        Name=pset_name,
        HasProperties=[],
    )
    rel = model.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId=new_guid(),
        OwnerHistory=owner_history,
        RelatingPropertyDefinition=pset,
        RelatedObjects=[element],
    )
    return pset


def _ensure_quantity(model, element, qto_name: str):
    for rel in _property_rels(element):
        if rel.is_a("IfcRelDefinesByProperties"):
            qto = rel.RelatingPropertyDefinition
            if qto and qto.is_a("IfcElementQuantity") and getattr(qto, "Name", None) == qto_name:
                return qto
    owner_history = _owner_history(model)
    qto = model.create_entity(
        "IfcElementQuantity",
        GlobalId=new_guid(),
        OwnerHistory=owner_history,
        Name=qto_name,
        Quantities=[],
    )
    rel = model.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId=new_guid(),
        OwnerHistory=owner_history,
        RelatingPropertyDefinition=qto,
        RelatedObjects=[element],
    )
    return qto


def _property_rels(element):
    try:
        rels = list(getattr(element, "IsDefinedBy", []) or [])
        if rels:
            return rels
    except Exception:
        pass
    file = getattr(getattr(element, "wrapped_data", None), "file", None)
    if file:
        return [r for r in file.by_type("IfcRelDefinesByProperties") if element in (r.RelatedObjects or [])]
    return []


def _create_quantity(model, descriptor: FieldDescriptor, value: Any):
    name = descriptor.quantity_name or "Quantity"
    val = float(value or 0.0)
    lname = name.lower()
    if "area" in lname:
        return model.create_entity("IfcQuantityArea", Name=name, AreaValue=val)
    if "volume" in lname:
        return model.create_entity("IfcQuantityVolume", Name=name, VolumeValue=val)
    if "count" in lname:
        return model.create_entity("IfcQuantityCount", Name=name, CountValue=val)
    return model.create_entity("IfcQuantityLength", Name=name, LengthValue=val)


def _coerce_nominal_value(model, value: Any):
    if hasattr(value, "is_a"):
        return value
    if isinstance(value, str):
        return model.create_entity("IfcLabel", value)
    if isinstance(value, (int, float)):
        return model.create_entity("IfcReal", float(value))
    return model.create_entity("IfcLabel", str(value))


def get_value(element, descriptor: FieldDescriptor) -> Any:
    if descriptor.kind == FieldKind.ATTRIBUTE:
        return getattr(element, descriptor.attribute_name or "", None)
    if descriptor.kind == FieldKind.PROPERTY:
        pset_name = descriptor.pset_name or ""
        prop_name = descriptor.property_name or ""
        for rel in _property_rels(element):
            if rel.is_a("IfcRelDefinesByProperties"):
                pset = rel.RelatingPropertyDefinition
                if pset and pset.is_a("IfcPropertySet") and getattr(pset, "Name", None) == pset_name:
                    for prop in pset.HasProperties or []:
                        if getattr(prop, "Name", None) == prop_name:
                            nominal = getattr(prop, "NominalValue", None)
                            if nominal is not None and hasattr(nominal, "wrappedValue"):
                                return nominal.wrappedValue
                            if nominal is not None:
                                return nominal
                            if hasattr(prop, "EnumerationValues"):
                                vals = getattr(prop, "EnumerationValues") or []
                                if vals:
                                    val = vals[0]
                                    return val.wrappedValue if hasattr(val, "wrappedValue") else val
                            return None
    if descriptor.kind == FieldKind.QUANTITY:
        qto_name = descriptor.qto_name or "BaseQuantities"
        qty_name = descriptor.quantity_name or ""
        for rel in _property_rels(element):
            if rel.is_a("IfcRelDefinesByProperties"):
                qto = rel.RelatingPropertyDefinition
                if qto and qto.is_a("IfcElementQuantity") and getattr(qto, "Name", None) == qto_name:
                    for qty in qto.Quantities or []:
                        if getattr(qty, "Name", None) == qty_name:
                            return getattr(qty, "LengthValue", None) or getattr(qty, "AreaValue", None) or getattr(qty, "VolumeValue", None)
    if descriptor.kind == FieldKind.CLASSIFICATION:
        return find_classification_value(element, descriptor.classification_system or "")
    if descriptor.kind == FieldKind.PREDEFINEDTYPE:
        return getattr(element, "PredefinedType", None)
    return None


def set_value(model, element, descriptor: FieldDescriptor, value: Any) -> Tuple[Any, Any]:
    old_value = get_value(element, descriptor)
    if descriptor.kind == FieldKind.ATTRIBUTE:
        setattr(element, descriptor.attribute_name or "", value)
        return old_value, value
    if descriptor.kind == FieldKind.PROPERTY:
        pset = _ensure_pset(model, element, descriptor.pset_name or "Pset_Custom")
        prop = None
        for existing in pset.HasProperties or []:
            if getattr(existing, "Name", None) == descriptor.property_name:
                prop = existing
                break
        if not prop:
            prop = model.create_entity(
                "IfcPropertySingleValue",
                Name=descriptor.property_name or "Value",
                NominalValue=_coerce_nominal_value(model, value),
            )
            pset.HasProperties = list(pset.HasProperties or []) + [prop]
        else:
            prop.NominalValue = _coerce_nominal_value(model, value)
        return old_value, value
    if descriptor.kind == FieldKind.QUANTITY:
        qto = _ensure_quantity(model, element, descriptor.qto_name or "BaseQuantities")
        qty = None
        for existing in qto.Quantities or []:
            if getattr(existing, "Name", None) == descriptor.quantity_name:
                qty = existing
                break
        if not qty:
            qty = _create_quantity(model, descriptor, value)
            qto.Quantities = list(qto.Quantities or []) + [qty]
        else:
            if hasattr(qty, "LengthValue"):
                qty.LengthValue = float(value or 0.0)
            elif hasattr(qty, "AreaValue"):
                qty.AreaValue = float(value or 0.0)
            elif hasattr(qty, "VolumeValue"):
                qty.VolumeValue = float(value or 0.0)
        return old_value, value
    if descriptor.kind == FieldKind.CLASSIFICATION:
        attach_classification(model, element, descriptor.classification_system or "Classification", str(value) if value is not None else "")
        return old_value, value
    if descriptor.kind == FieldKind.PREDEFINEDTYPE:
        setattr(element, "PredefinedType", value)
        return old_value, value
    return old_value, value
