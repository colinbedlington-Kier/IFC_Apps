import ifcopenshell
from ifcopenshell.guid import new as new_guid

from app import apply_predefined_type_changes, rewrite_proxy_types, scan_predefined_types


def build_predefined_model() -> ifcopenshell.file:
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    _ = project

    match_elem = model.create_entity("IfcPipeSegment", GlobalId=new_guid(), Name="Pipe-1")
    match_type = model.create_entity("IfcPipeSegmentType", GlobalId=new_guid(), Name="pipe segment_rigid")
    model.create_entity(
        "IfcRelDefinesByType",
        GlobalId=new_guid(),
        RelatedObjects=[match_elem],
        RelatingType=match_type,
    )

    no_match_elem = model.create_entity("IfcPipeSegment", GlobalId=new_guid(), Name="Pipe-2")
    no_match_type = model.create_entity("IfcPipeSegmentType", GlobalId=new_guid(), Name="UnknownType")
    model.create_entity(
        "IfcRelDefinesByType",
        GlobalId=new_guid(),
        RelatedObjects=[no_match_elem],
        RelatingType=no_match_type,
    )

    model.create_entity("IfcSite", GlobalId=new_guid(), Name="Site")
    return model


def test_predefined_type_scan_and_apply(tmp_path):
    model = build_predefined_model()
    in_path = tmp_path / "input.ifc"
    model.write(str(in_path))

    stats, rows = scan_predefined_types(str(in_path), class_filter=None)
    assert stats["rows"] >= 3
    target_rows = [row for row in rows if row["ifc_class"] == "IfcPipeSegment"]
    assert any(row["proposed_predefined_type"] == "NOTDEFINED" for row in target_rows)
    assert any(row["proposed_predefined_type"] == "USERDEFINED" for row in target_rows)
    assert any(row["proposed_predefined_type"] == "N/A" for row in rows if row["ifc_class"] == "IfcSite")

    out_path, _, _ = apply_predefined_type_changes(str(in_path), rows)
    updated = ifcopenshell.open(out_path)
    pipe_segments = {seg.Name: seg for seg in updated.by_type("IfcPipeSegment")}
    assert pipe_segments["Pipe-1"].PredefinedType == "NOTDEFINED"
    assert pipe_segments["Pipe-2"].PredefinedType == "USERDEFINED"


def test_predefined_type_scan_matches_pascal_and_space_case(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    _ = project

    pascal_elem = model.create_entity("IfcWasteTerminal", GlobalId=new_guid(), Name="Waste-Pascal")
    pascal_type = model.create_entity("IfcWasteTerminalType", GlobalId=new_guid(), Name="WasteTerminal_Gully")
    model.create_entity(
        "IfcRelDefinesByType",
        GlobalId=new_guid(),
        RelatedObjects=[pascal_elem],
        RelatingType=pascal_type,
    )

    spaced_elem = model.create_entity("IfcWasteTerminal", GlobalId=new_guid(), Name="Waste-Space")
    spaced_type = model.create_entity("IfcWasteTerminalType", GlobalId=new_guid(), Name="Waste Terminal_Gully")
    model.create_entity(
        "IfcRelDefinesByType",
        GlobalId=new_guid(),
        RelatedObjects=[spaced_elem],
        RelatingType=spaced_type,
    )

    in_path = tmp_path / "waste_terminal.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(str(in_path), class_filter=["IfcWasteTerminal"])
    rows_by_name = {row["globalid"]: row for row in rows}

    assert rows_by_name[pascal_elem.GlobalId]["match_found"] is True
    assert rows_by_name[spaced_elem.GlobalId]["match_found"] is True
    assert rows_by_name[pascal_elem.GlobalId]["proposed_predefined_type"] == "NOTDEFINED"
    assert rows_by_name[spaced_elem.GlobalId]["proposed_predefined_type"] == "NOTDEFINED"


def test_rewrite_proxy_types_extracts_predefined_from_multi_token_name(tmp_path):
    in_path = tmp_path / "proxy_in.ifc"
    out_path = tmp_path / "proxy_out.ifc"
    in_path.write_text(
        "\n".join(
            [
                "ISO-10303-21;",
                "DATA;",
                "#10=IFCBUILDINGELEMENTPROXYTYPE('g',#2,'WasteTerminal_GullySump_Type01',$,.NOTDEFINED.);",
                "ENDSEC;",
                "END-ISO-10303-21;",
                "",
            ]
        )
    )

    rewrite_proxy_types(str(in_path), str(out_path))
    output = out_path.read_text()

    assert "IFCWASTETERMINALTYPE" in output
    assert ".GULLYSUMP." in output


def test_predefined_type_scan_and_apply_targets_proxy_type(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    _ = project

    proxy_type = model.create_entity(
        "IfcBuildingElementProxyType",
        GlobalId=new_guid(),
        Name="WasteTerminal_GullySump_Type04",
        PredefinedType="NOTDEFINED",
    )
    proxy_occ = model.create_entity("IfcBuildingElementProxy", GlobalId=new_guid(), Name="Proxy-1")
    model.create_entity(
        "IfcRelDefinesByType",
        GlobalId=new_guid(),
        RelatedObjects=[proxy_occ],
        RelatingType=proxy_type,
    )

    in_path = tmp_path / "proxy_predef.ifc"
    model.write(str(in_path))

    _, rows = scan_predefined_types(str(in_path), class_filter=["IfcBuildingElementProxy"])
    assert len(rows) == 1
    row = rows[0]
    assert row["match_found"] is True
    assert row["proposed_predefined_type"] == "NOTDEFINED"
    assert row["predef_target_source"] == "type"
    assert row["predef_target_class"] == "IfcBuildingElementProxyType"

    out_path, _, _ = apply_predefined_type_changes(str(in_path), rows)
    updated = ifcopenshell.open(out_path)
    updated_type = updated.by_guid(proxy_type.GlobalId)
    assert updated_type.PredefinedType == "NOTDEFINED"
