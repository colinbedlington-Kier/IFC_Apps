import re
from typing import Any, Dict

from field_access import FieldDescriptor, FieldKind, get_value


TOKEN_RE = re.compile(r"\{([^{}]+)\}")


class ExpressionEngine:
    def __init__(self, model):
        self.model = model

    def evaluate(self, expression: str, element) -> str:
        if not expression:
            return ""

        def replace(match):
            token = match.group(1)
            return str(self._resolve_token(token, element) or "")

        return TOKEN_RE.sub(replace, expression)

    def _resolve_token(self, token: str, element) -> Any:
        if token.lower().startswith("pset_"):
            if "." in token:
                pset, prop = token.split(".", 1)
                fd = FieldDescriptor(kind=FieldKind.PROPERTY, pset_name=pset, property_name=prop)
                return get_value(element, fd)
        if token.lower().startswith("qto_"):
            if "." in token:
                qto, qty = token.split(".", 1)
                fd = FieldDescriptor(kind=FieldKind.QUANTITY, qto_name=qto, quantity_name=qty)
                return get_value(element, fd)
        if token.lower().startswith("class."):
            system = token.split(".", 1)[1]
            fd = FieldDescriptor(kind=FieldKind.CLASSIFICATION, classification_system=system)
            return get_value(element, fd)
        # Attribute fallback
        fd = FieldDescriptor(kind=FieldKind.ATTRIBUTE, attribute_name=token)
        return get_value(element, fd)
