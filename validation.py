from dataclasses import dataclass
from typing import List, Optional

import ifcopenshell

from field_access import FieldDescriptor, FieldKind


@dataclass
class ValidationIssue:
    check_id: str
    message: str
    severity: str = "warning"


def _predefined_enum_values(schema: str, entity_name: str) -> List[str]:
    try:
        schema_decl = ifcopenshell.ifcopenshell_wrapper.schema_by_name(schema)
        decl = schema_decl.declaration_by_name(entity_name)
        for attr in decl.attributes():
            if attr.name().lower() == "predefinedtype":
                declared = attr.type_of_attribute().declared_type()
                if declared and hasattr(declared, "enumeration_items"):
                    return list(declared.enumeration_items())
    except Exception:
        return []
    return []


def validate_value(model, element, descriptor: FieldDescriptor, value, check_id: Optional[str] = None) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    if value in (None, "", [], {}):
        issues.append(ValidationIssue(check_id=check_id or descriptor.path_label(), message="Required value is missing"))
        return issues

    if descriptor.kind == FieldKind.PREDEFINEDTYPE:
        schema_name = model.schema if isinstance(model.schema, str) else getattr(model.schema, "name", str(model.schema))
        allowed = _predefined_enum_values(schema_name, element.is_a())
        if allowed and str(value) not in allowed:
            issues.append(
                ValidationIssue(
                    check_id=check_id or descriptor.path_label(),
                    message=f"Value '{value}' not in enumeration ({', '.join(allowed)})",
                )
            )
    return issues
