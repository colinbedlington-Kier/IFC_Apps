from typing import Any, Optional, Tuple

import ifcopenshell
from ifcopenshell.guid import new as new_guid


def _matches_system(cls_obj, system_name: str) -> bool:
    try:
        return (getattr(cls_obj, "Name", "") or "").strip().lower() == system_name.strip().lower()
    except Exception:
        return False


def ensure_classification(model, system_name: str):
    for cls in model.by_type("IfcClassification"):
        if _matches_system(cls, system_name):
            return cls
    return model.create_entity(
        "IfcClassification",
        Name=system_name,
        Source=None,
        Edition=None,
        EditionDate=None,
    )


def ensure_classification_reference(model, classification, identification: str, name: Optional[str] = None):
    for ref in model.by_type("IfcClassificationReference"):
        if getattr(ref, "ReferencedSource", None) == classification and getattr(ref, "Identification", None) == identification:
            return ref
    return model.create_entity(
        "IfcClassificationReference",
        Name=name or identification,
        Identification=identification,
        ReferencedSource=classification,
    )


def attach_classification(model, element, system_name: str, identification: str, name: Optional[str] = None):
    classification = ensure_classification(model, system_name)
    ref = ensure_classification_reference(model, classification, identification, name=name)

    def _existing_rels():
        rels = []
        try:
            rels = list(getattr(element, "HasAssociations", []) or [])
        except Exception:
            rels = []
        if not rels and hasattr(element, "wrapped_data") and getattr(element.wrapped_data, "file", None):
            file = element.wrapped_data.file
            rels = [r for r in file.by_type("IfcRelAssociatesClassification") if element in (r.RelatedObjects or [])]
        return rels

    for rel in _existing_rels():
        if rel.is_a("IfcRelAssociatesClassification") and rel.RelatingClassification == ref:
            if element not in rel.RelatedObjects:
                rel.RelatedObjects = list(rel.RelatedObjects) + [element]
            return rel
    rel = model.create_entity(
        "IfcRelAssociatesClassification",
        GlobalId=new_guid(),
        RelatedObjects=[element],
        RelatingClassification=ref,
    )
    return rel


def find_classification_value(element, system_name: str) -> Optional[str]:
    rels = []
    try:
        rels = list(getattr(element, "HasAssociations", []) or [])
    except Exception:
        rels = []
    if not rels and hasattr(element, "wrapped_data") and getattr(element.wrapped_data, "file", None):
        rels = [r for r in element.wrapped_data.file.by_type("IfcRelAssociatesClassification") if element in (r.RelatedObjects or [])]
    for rel in rels:
        if rel.is_a("IfcRelAssociatesClassification"):
            ref = getattr(rel, "RelatingClassification", None)
            if ref and _matches_system(getattr(ref, "ReferencedSource", None) or ref, system_name):
                return getattr(ref, "Identification", None) or getattr(ref, "ItemReference", None)
    return None


def count_classification_relationships(element) -> int:
    rels = []
    try:
        rels = list(getattr(element, "HasAssociations", []) or [])
    except Exception:
        rels = []
    if not rels and hasattr(element, "wrapped_data") and getattr(element.wrapped_data, "file", None):
        rels = [r for r in element.wrapped_data.file.by_type("IfcRelAssociatesClassification") if element in (r.RelatedObjects or [])]
    return sum(1 for rel in rels if rel.is_a("IfcRelAssociatesClassification"))
