from pathlib import Path

import app
from backend.ifc_qa.config_loader import (
    REQUIRED_TOP_LEVEL_KEYS,
    build_config_indexes,
    load_default_config,
    merge_config_override,
)


def test_default_config_file_exists_and_loads():
    path = Path("backend/ifc_qa/reference/ifc_qa_config.default.json")
    assert path.exists()
    cfg = load_default_config()
    assert isinstance(cfg, dict)


def test_required_top_level_keys_present():
    cfg = load_default_config()
    for key in REQUIRED_TOP_LEVEL_KEYS:
        assert key in cfg


def test_indexes_are_built_correctly_with_known_values():
    cfg = load_default_config()
    idx = build_config_indexes(cfg)
    assert "Fr-" in idx["short_code_set"]
    assert "S-EF2005--Substructure" in idx["layer_set"]
    assert "IfcDistributionControlElement-IfcActuatorType-ELECTRICACTUATOR" in idx["entity_type_by_key"]
    assert "Ss_15: Earthworks, remediation and temporary systems" in idx["system_category_value_set"]
    assert "IfcBeam-IfcBeamType" in idx["pset_template_map"]


def test_get_ifc_qa_config_returns_valid_json():
    payload = app.ifc_qa_config()
    for key in REQUIRED_TOP_LEVEL_KEYS:
        assert key in payload


def test_merge_override_replaces_top_level_keys():
    cfg = load_default_config()
    merged = merge_config_override(cfg, {"layers": [{"Layer": "X-Test-Layer"}]})
    assert merged["layers"] == [{"Layer": "X-Test-Layer"}]
    assert merged["shortCodes"] == cfg["shortCodes"]


def test_post_config_merge_endpoint_works():
    payload = app.ifc_qa_config_merge({"systemCategory": [{"Classification_Value": "X: Test"}]})
    assert payload["systemCategory"] == [{"Classification_Value": "X: Test"}]


def test_post_config_validate_endpoint_flags_missing_keys():
    payload = app.ifc_qa_config_validate({"shortCodes": []})
    assert payload["valid"] is False
    assert payload["errors"]
