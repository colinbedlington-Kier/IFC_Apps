from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import hashlib
import re

from step2ifc.config import ConversionConfig, PropertyMapping, TypeMappingRule
from step2ifc.io_step import StepPart
from step2ifc.logging import get_logger


@dataclass
class PartMappingResult:
    ifc_class: str
    object_type: Optional[str]
    name: str
    tag: str
    properties: Dict[str, Dict[str, str]]
    classification: Optional[Dict[str, str]]


@dataclass
class PartContext:
    part: StepPart
    source_hash: str
    assembly_path: str
    project_key: str
    metadata: Dict[str, str]


class MappingEngine:
    def __init__(self, config: ConversionConfig) -> None:
        self.config = config
        self.logger = get_logger()

    def normalize_name(self, name: str) -> str:
        normalized = name
        for rule in self.config.name_normalization:
            normalized = re.sub(rule.pattern, rule.replacement, normalized, flags=re.IGNORECASE)
        return normalized.strip()

    def stable_guid_seed(self, context: PartContext) -> str:
        seed = f"{context.source_hash}|{context.assembly_path}|{context.part.name}"
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def map_part(self, context: PartContext) -> PartMappingResult:
        name = self.normalize_name(context.part.name)
        assembly_path = context.assembly_path
        mapping = self._select_mapping(name, assembly_path)
        tag = self._short_hash(context.source_hash, assembly_path, name)
        properties = self._build_properties(mapping, context)
        classification = None
        if mapping and mapping.classification:
            classification = {
                "system": mapping.classification.system,
                "code": mapping.classification.code,
                "title": mapping.classification.title or mapping.classification.code,
            }
        return PartMappingResult(
            ifc_class=mapping.ifc_class if mapping else self.config.default_type,
            object_type=mapping.object_type if mapping else None,
            name=name,
            tag=tag,
            properties=properties,
            classification=classification,
        )

    def _select_mapping(self, name: str, assembly_path: str) -> Optional[TypeMappingRule]:
        for rule in self.config.type_mappings:
            if rule.match_name_regex and re.search(rule.match_name_regex, name, re.IGNORECASE):
                return rule
            if rule.match_assembly_prefix and assembly_path.startswith(rule.match_assembly_prefix):
                return rule
        return None

    def _build_properties(self, mapping: Optional[TypeMappingRule], context: PartContext) -> Dict[str, Dict[str, str]]:
        properties: Dict[str, Dict[str, str]] = {}
        for prop in (mapping.properties if mapping else []):
            value = self._resolve_value(prop, context)
            properties[prop.name] = {"value": value, "data_type": prop.data_type}
        return properties

    def _resolve_value(self, prop: PropertyMapping, context: PartContext) -> str:
        token_map = {
            "ProjectKey": context.project_key,
            "AssemblyPath": context.assembly_path,
            "OriginalName": context.part.name,
        }
        token_map.update(context.metadata)
        value = prop.value
        for token, token_value in token_map.items():
            value = value.replace(f"${{{token}}}", str(token_value))
        return value

    def _short_hash(self, source_hash: str, assembly_path: str, name: str) -> str:
        seed = f"{source_hash}|{assembly_path}|{name}"
        return hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
