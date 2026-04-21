import copy
import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List

REQUIRED_TOP_LEVEL_KEYS = (
    "shortCodes",
    "layers",
    "entityTypes",
    "systemCategory",
    "psetTemplate",
)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "reference" / "ifc_qa_config.default.json"
_CACHE_LOCK = Lock()
_CACHED_CONFIG: Dict[str, Any] | None = None


def validate_config_structure(config: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(config, dict):
        return ["Config must be a JSON object."]

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key not in config:
            errors.append(f"Missing required top-level key: {key}")

    list_keys = ("shortCodes", "layers", "entityTypes", "systemCategory")
    for key in list_keys:
        if key in config and not isinstance(config[key], list):
            errors.append(f"{key} must be an array.")

    if "psetTemplate" in config and not isinstance(config["psetTemplate"], dict):
        errors.append("psetTemplate must be an object.")

    return errors


def build_config_indexes(config: Dict[str, Any]) -> Dict[str, Any]:
    short_codes = config.get("shortCodes") or []
    layers = config.get("layers") or []
    entity_types = config.get("entityTypes") or []
    system_category = config.get("systemCategory") or []

    short_codes_by_entity: Dict[str, List[Dict[str, Any]]] = {}
    for row in short_codes:
        entity = (row.get("Natural_Language_Entity") or "").strip()
        if entity:
            short_codes_by_entity.setdefault(entity, []).append(row)

    layers_by_discipline: Dict[str, List[Dict[str, Any]]] = {}
    for row in layers:
        discipline = (row.get("Layer_Discipline") or "").strip()
        if discipline:
            layers_by_discipline.setdefault(discipline, []).append(row)

    entity_type_by_key: Dict[str, Dict[str, Any]] = {}
    entity_types_by_natural_language_entity: Dict[str, List[Dict[str, Any]]] = {}
    for row in entity_types:
        key = (row.get("IFC_Predefined_Type_Key") or "").strip()
        if key:
            entity_type_by_key[key] = row
        entity = (row.get("Natural_Language_Entity") or "").strip()
        if entity:
            entity_types_by_natural_language_entity.setdefault(entity, []).append(row)

    system_category_by_number: Dict[str, List[Dict[str, Any]]] = {}
    for row in system_category:
        number = (row.get("Classification_Number") or "").strip()
        if number:
            system_category_by_number.setdefault(number, []).append(row)

    return {
        "short_code_set": {row.get("Nomenclature_Short_Code") for row in short_codes if row.get("Nomenclature_Short_Code")},
        "short_codes_by_entity": short_codes_by_entity,
        "layer_set": {row.get("Layer") for row in layers if row.get("Layer")},
        "layers_by_discipline": layers_by_discipline,
        "entity_type_by_key": entity_type_by_key,
        "entity_types_by_natural_language_entity": entity_types_by_natural_language_entity,
        "system_category_value_set": {
            row.get("Classification_Value") for row in system_category if row.get("Classification_Value")
        },
        "system_category_by_number": system_category_by_number,
        "pset_template_map": config.get("psetTemplate") or {},
    }


def merge_config_override(default_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    merged = {
        key: copy.deepcopy(default_config.get(key))
        for key in REQUIRED_TOP_LEVEL_KEYS
    }
    if not isinstance(override_config, dict):
        return merged

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key in override_config:
            merged[key] = copy.deepcopy(override_config[key])

    errors = validate_config_structure(merged)
    if errors:
        raise ValueError("Invalid merged IFC QA config: " + "; ".join(errors))
    return merged


def load_default_config() -> Dict[str, Any]:
    global _CACHED_CONFIG
    with _CACHE_LOCK:
        if _CACHED_CONFIG is not None:
            return copy.deepcopy(_CACHED_CONFIG)

        if not _DEFAULT_CONFIG_PATH.exists():
            raise RuntimeError(f"Default IFC QA config not found at {_DEFAULT_CONFIG_PATH}")

        raw = json.loads(_DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        errors = validate_config_structure(raw)
        if errors:
            raise RuntimeError("Invalid default IFC QA config: " + "; ".join(errors))

        runtime_config = {key: raw[key] for key in REQUIRED_TOP_LEVEL_KEYS}
        runtime_config["_indexes"] = build_config_indexes(runtime_config)
        _CACHED_CONFIG = runtime_config
        return copy.deepcopy(_CACHED_CONFIG)
