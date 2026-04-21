import app


def test_get_ifc_qa_config_returns_default_json():
    payload = app.ifc_qa_default_config()
    assert "shortCodes" in payload
    assert "layers" in payload
    assert "entityTypes" in payload
    assert "systemCategory" in payload
    assert "psetTemplate" in payload


def test_validate_and_merge_ifc_qa_config_endpoints():
    invalid = app.ifc_qa_validate_config({"shortCodes": []})
    assert invalid["valid"] is False

    override = {
        "shortCodes": [{"Natural_Language_Entity": "Space", "Nomenclature_Short_Code": "Sp-"}],
        "layers": [{"Layer": "A-TEST"}],
        "entityTypes": [],
        "systemCategory": [],
        "psetTemplate": {},
    }
    merged = app.ifc_qa_merge_config(override)
    assert merged["valid"] is True
    assert merged["config"]["shortCodes"][0]["Nomenclature_Short_Code"] == "Sp-"
    assert merged["config"]["layers"][0]["Layer"] == "A-TEST"
