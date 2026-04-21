import copy
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

LOGGER = logging.getLogger("ifc_qa.config")
REQUIRED_TOP_LEVEL_KEYS = ["shortCodes", "layers", "entityTypes", "systemCategory", "psetTemplate"]
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "reference" / "ifc_qa_config.default.json"

_DEFAULT_CONFIG: Dict[str, Any] | None = None


def validate_config_structure(config: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(config, dict):
        return ["Config payload must be a JSON object."]

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key not in config:
            errors.append(f"Missing top-level key: {key}")

    if "psetTemplate" in config and not isinstance(config.get("psetTemplate"), dict):
        errors.append("psetTemplate must be an object keyed by IFC_Entity_Occurrence_Type.")

    for key in ["shortCodes", "layers", "entityTypes", "systemCategory"]:
        if key in config and not isinstance(config.get(key), list):
            errors.append(f"{key} must be an array.")

    return errors


def _build_lookup_indexes(config: Dict[str, Any]) -> Dict[str, Any]:
    entity_types_by_nle: Dict[str, List[Dict[str, Any]]] = {}
    for row in config.get("entityTypes", []):
        key = (row.get("Natural_Language_Entity") or "").strip()
        if key:
            entity_types_by_nle.setdefault(key, []).append(row)

    return {
        "short_code_set": {
            (row.get("Nomenclature_Short_Code") or "").strip()
            for row in config.get("shortCodes", [])
            if (row.get("Nomenclature_Short_Code") or "").strip()
        },
        "layer_set": {
            (row.get("Layer") or "").strip()
            for row in config.get("layers", [])
            if (row.get("Layer") or "").strip()
        },
        "entity_type_by_key": {
            (row.get("IFC_Predefined_Type_Key") or "").strip(): row
            for row in config.get("entityTypes", [])
            if (row.get("IFC_Predefined_Type_Key") or "").strip()
        },
        "entity_types_by_natural_language_entity": entity_types_by_nle,
        "system_category_values": {
            (row.get("Classification_Value") or "").strip()
            for row in config.get("systemCategory", [])
            if (row.get("Classification_Value") or "").strip()
        },
        "pset_template_map": dict(config.get("psetTemplate", {})),
    }


def _load_default_from_disk() -> Dict[str, Any]:
    if not DEFAULT_CONFIG_PATH.exists():
        message = f"Missing IFC QA default config file: {DEFAULT_CONFIG_PATH}"
        LOGGER.error(message)
        raise FileNotFoundError(message)

    with DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    errors = validate_config_structure(config)
    if errors:
        message = f"Invalid IFC QA default config: {errors}"
        LOGGER.error(message)
        raise ValueError(message)

    config["_indexes"] = _build_lookup_indexes(config)
    return config


def get_default_config() -> Dict[str, Any]:
    global _DEFAULT_CONFIG
    if _DEFAULT_CONFIG is None:
        _DEFAULT_CONFIG = _load_default_from_disk()
    return copy.deepcopy(_DEFAULT_CONFIG)


def merge_config_overrides(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    merged = export_config(base_config)
    if not isinstance(override_config, dict):
        raise ValueError("override_config must be a JSON object")

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key in override_config:
            merged[key] = copy.deepcopy(override_config[key])

    errors = validate_config_structure(merged)
    if errors:
        raise ValueError("; ".join(errors))

    merged["_indexes"] = _build_lookup_indexes(merged)
    return merged


def export_config(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(config)
    payload.pop("_indexes", None)
    return payload
