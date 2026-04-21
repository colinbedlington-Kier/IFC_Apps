from backend.ifc_qa.config_loader import export_config, get_default_config, merge_config_overrides, validate_config_structure
from ifc_qa_service import evaluate_config_rules


def test_default_config_loads_with_required_keys_and_indexes():
    config = get_default_config()
    exported = export_config(config)

    assert validate_config_structure(exported) == []
    assert all(key in exported for key in ["shortCodes", "layers", "entityTypes", "systemCategory", "psetTemplate"])
    assert "_indexes" in config
    assert "short_code_set" in config["_indexes"]
    assert "layer_set" in config["_indexes"]
    assert "entity_type_by_key" in config["_indexes"]
    assert "system_category_values" in config["_indexes"]
    assert "pset_template_map" in config["_indexes"]


def test_merge_config_overrides_replaces_top_level_keys():
    base = get_default_config()
    merged = merge_config_overrides(
        base,
        {
            "shortCodes": [{"Natural_Language_Entity": "Space", "Nomenclature_Short_Code": "Sp-"}],
            "layers": [{"Layer": "A-TEST"}],
        },
    )

    assert len(merged["shortCodes"]) == 1
    assert merged["shortCodes"][0]["Nomenclature_Short_Code"] == "Sp-"
    assert len(merged["layers"]) == 1
    assert merged["layers"][0]["Layer"] == "A-TEST"
    assert "Sp-" in merged["_indexes"]["short_code_set"]
    assert "A-TEST" in merged["_indexes"]["layer_set"]


def test_extractor_rule_checks_use_lookup_indexes():
    config = get_default_config()
    checks = evaluate_config_rules(
        config,
        short_code="Fr-",
        layer="S-EF2005--Substructure",
        entity_type_key="IfcDistributionControlElement-IfcActuatorType-ELECTRICACTUATOR",
        system_category_value="Ss_15: Earthworks, remediation and temporary systems",
        pset_combo="IfcBeam-IfcBeamType",
        pset_pair=("COBie_Type", "AssetType"),
    )

    assert checks["short_code"] is True
    assert checks["layer"] is True
    assert checks["entity_type_key"] is True
    assert checks["system_category"] is True
    assert checks["pset_template"] is True
