import ifcopenshell
from ifcopenshell.guid import new as new_guid

from app import (
    apply_predefined_type_changes,
    load_ifc2x3_pset_applicability_library,
    parse_name_parts,
    resolve_predefined_literal,
    resolve_type_and_predefined_for_name,
    rewrite_proxy_types,
    scan_predefined_types,
)


def test_parse_name_parts_space_case_and_ordinal():
    parsed = parse_name_parts("Waste Terminal_GullySump_Type01")
    assert parsed["raw_tokens"] == ["Waste Terminal", "GullySump", "Type01"]
    assert parsed["classish_raw"] == "Waste Terminal"
    assert parsed["predef_candidate_raw"] == "GullySump"
    assert parsed["ordinal_raw"] == "Type01"


def test_resolver_matches_pascal_and_space_case_classish_and_predef():
    pascal = resolve_type_and_predefined_for_name("WasteTerminal_GullySump_Type01", "IFC4")
    spaced = resolve_type_and_predefined_for_name("Waste Terminal_Gully Sump_Type01", "IFC4")

    assert pascal["resolved_type_class"] == "IfcWasteTerminalType"
    assert spaced["resolved_type_class"] == "IfcWasteTerminalType"
    assert pascal["resolved_predefined_type"] == "GULLYSUMP"
    assert spaced["resolved_predefined_type"] == "GULLYSUMP"


def test_resolver_fallback_when_classish_split_across_underscores():
    resolved = resolve_type_and_predefined_for_name(
        "Distribution_Chamber_Element_InspectionChamber_Type01", "IFC4"
    )
    assert resolved["resolved_type_class"] == "IfcDistributionChamberElementType"
    assert resolved["parsed_predef"] == "InspectionChamber"
    assert resolved["resolved_predefined_type"] == "INSPECTIONCHAMBER"


def test_enum_normalization_matches_spacing_and_underscore_variants():
    enum_items = ["GULLYSUMP", "USERDEFINED", "NOTDEFINED"]
    assert resolve_predefined_literal("GullySump", enum_items)["value"] == "GULLYSUMP"
    assert resolve_predefined_literal("Gully Sump", enum_items)["value"] == "GULLYSUMP"
    assert resolve_predefined_literal("GULLY_SUMP", enum_items)["value"] == "GULLYSUMP"


def test_ifc2x3_pset_library_loaded_and_contains_core_rows():
    lib = load_ifc2x3_pset_applicability_library()
    assert ("IfcWasteTerminalType", "gullysump") in lib
    assert lib[("IfcWasteTerminalType", "gullysump")]["pset_name"] == "Pset_WasteTerminalTypeGullySump"
    assert ("IfcDistributionChamberElementType", "inspectionchamber") in lib


def test_ifc2x3_scan_matches_pset_applicability_and_proposes_value(tmp_path):
    model = ifcopenshell.file(schema="IFC2X3")
    person = model.create_entity("IfcPerson")
    org = model.create_entity("IfcOrganization", Name="Org")
    pao = model.create_entity("IfcPersonAndOrganization", ThePerson=person, TheOrganization=org)
    app = model.create_entity("IfcApplication", ApplicationDeveloper=org, Version="1", ApplicationFullName="t", ApplicationIdentifier="t")
    model.create_entity(
        "IfcOwnerHistory",
        OwningUser=pao,
        OwningApplication=app,
        ChangeAction="ADDED",
        CreationDate=1,
    )
    _ = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")

    occ = model.create_entity("IfcFlowTerminal", GlobalId=new_guid(), Name="Act-1")
    typ = model.create_entity("IfcActuatorType", GlobalId=new_guid(), Name="Actuator_Common_Type01")
    model.create_entity("IfcRelDefinesByType", GlobalId=new_guid(), RelatedObjects=[occ], RelatingType=typ)

    in_path = tmp_path / "ifc2x3_pset_scan.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(str(in_path), class_filter=["IfcFlowTerminal"])
    assert len(rows) == 1
    row = rows[0]
    assert row["parsed_class"] == "Actuator"
    assert row["resolved_type_class"] == "IfcActuatorType"
    assert row["parsed_predef_token"] == "Common"
    assert row["match_source"] == "pset_applicability"
    assert row["matched_pset_name"] == "Pset_ActuatorTypeCommon"
    assert row["proposed_predefined_type"] == "Common"
    assert row["match_found"] is True


def test_ifc2x3_apply_pset_applicability_adds_pset_to_type(tmp_path):
    model = ifcopenshell.file(schema="IFC2X3")
    person = model.create_entity("IfcPerson")
    org = model.create_entity("IfcOrganization", Name="Org")
    pao = model.create_entity("IfcPersonAndOrganization", ThePerson=person, TheOrganization=org)
    app = model.create_entity("IfcApplication", ApplicationDeveloper=org, Version="1", ApplicationFullName="t", ApplicationIdentifier="t")
    model.create_entity(
        "IfcOwnerHistory",
        OwningUser=pao,
        OwningApplication=app,
        ChangeAction="ADDED",
        CreationDate=1,
    )
    _ = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")

    occ = model.create_entity("IfcFlowTerminal", GlobalId=new_guid(), Name="Act-1")
    typ = model.create_entity("IfcActuatorType", GlobalId=new_guid(), Name="Actuator_Common_Type01")
    model.create_entity("IfcRelDefinesByType", GlobalId=new_guid(), RelatedObjects=[occ], RelatingType=typ)

    in_path = tmp_path / "ifc2x3_pset_apply.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(str(in_path), class_filter=["IfcFlowTerminal"])
    row = rows[0]
    assert row["match_source"] == "pset_applicability"

    out_path, _, _ = apply_predefined_type_changes(str(in_path), [row])
    updated = ifcopenshell.open(out_path)
    updated_type = updated.by_guid(typ.GlobalId)
    
    try:
        psets = ifcopenshell.util.element.get_psets(updated_type, psets_only=True, include_inherited=False)
    except TypeError:
        psets = ifcopenshell.util.element.get_psets(updated_type, psets_only=True)
    assert "Pset_ActuatorTypeCommon" in psets


def test_target_selection_prefers_type_then_occurrence(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    _ = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")

    waste_occ = model.create_entity("IfcWasteTerminal", GlobalId=new_guid(), Name="Waste")
    waste_type = model.create_entity("IfcWasteTerminalType", GlobalId=new_guid(), Name="WasteTerminal_GullySump_Type01")
    model.create_entity("IfcRelDefinesByType", GlobalId=new_guid(), RelatedObjects=[waste_occ], RelatingType=waste_type)

    chamber_occ = model.create_entity(
        "IfcDistributionChamberElement",
        GlobalId=new_guid(),
        Name="Chamber",
        PredefinedType="NOTDEFINED",
    )

    in_path = tmp_path / "targets.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(
        str(in_path), class_filter=["IfcWasteTerminal", "IfcDistributionChamberElement"]
    )
    by_gid = {row["globalid"]: row for row in rows}

    assert by_gid[waste_occ.GlobalId]["target_source"] == "type"
    assert by_gid[waste_occ.GlobalId]["target_class"] == "IfcWasteTerminalType"
    assert by_gid[chamber_occ.GlobalId]["target_source"] == "occurrence"
    assert by_gid[chamber_occ.GlobalId]["target_class"] == "IfcDistributionChamberElement"


def test_scan_and_apply_mutates_correct_entity_and_rescan_is_stable(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    _ = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")

    waste_occ = model.create_entity("IfcFlowTerminal", GlobalId=new_guid(), Name="Waste-1")
    waste_type = model.create_entity(
        "IfcWasteTerminalType",
        GlobalId=new_guid(),
        Name="Waste Terminal_GullySump_Type01",
        PredefinedType="NOTDEFINED",
    )
    model.create_entity("IfcRelDefinesByType", GlobalId=new_guid(), RelatedObjects=[waste_occ], RelatingType=waste_type)

    in_path = tmp_path / "apply.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(str(in_path), class_filter=["IfcFlowTerminal"])
    row = rows[0]
    assert row["resolved_type_class"] == "IfcWasteTerminalType"
    assert row["proposed_predefined_type"] == "GULLYSUMP"
    assert row["target_source"] == "type"

    out_path, _, _ = apply_predefined_type_changes(str(in_path), [row])
    updated = ifcopenshell.open(out_path)
    updated_type = updated.by_guid(waste_type.GlobalId)
    assert updated_type.PredefinedType == "GULLYSUMP"

    _, rows_after = scan_predefined_types(str(out_path), class_filter=["IfcFlowTerminal"])
    assert rows_after[0]["proposed_predefined_type"] == "GULLYSUMP"


def test_rewrite_proxy_types_extracts_distribution_chamber_variants(tmp_path):
    in_path = tmp_path / "proxy_in.ifc"
    out_path = tmp_path / "proxy_out.ifc"
    in_path.write_text(
        "\n".join(
            [
                "ISO-10303-21;",
                "DATA;",
                "#10=IFCBUILDINGELEMENTPROXYTYPE('g',#2,'Distribution Chamber Element_InspectionChamber_Type01',$,.NOTDEFINED.);",
                "ENDSEC;",
                "END-ISO-10303-21;",
                "",
            ]
        )
    )

    rewrite_proxy_types(str(in_path), str(out_path))
    output = out_path.read_text()

    assert "IfcDistributionChamberElementType" in output
    assert ".INSPECTIONCHAMBER." in output
