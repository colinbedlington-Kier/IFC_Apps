import pandas as pd
import ifcopenshell
from ifcopenshell.guid import new as new_guid

from app import extract_to_excel, update_ifc_from_excel


def _build_model(schema: str, tmp_path):
    model = ifcopenshell.file(schema=schema)
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="P", LongName="PN-001")
    site = model.create_entity("IfcSite", GlobalId=new_guid(), Name="S")
    building = model.create_entity("IfcBuilding", GlobalId=new_guid(), Name="B")
    owner_history = model.create_entity("IfcOwnerHistory") if schema.upper() == "IFC2X3" else None
    rel_kwargs = {"GlobalId": new_guid(), "RelatingObject": project, "RelatedObjects": [site]}
    if owner_history is not None:
        rel_kwargs["OwnerHistory"] = owner_history
    model.create_entity("IfcRelAggregates", **rel_kwargs)
    rel_kwargs = {"GlobalId": new_guid(), "RelatingObject": site, "RelatedObjects": [building]}
    if owner_history is not None:
        rel_kwargs["OwnerHistory"] = owner_history
    model.create_entity("IfcRelAggregates", **rel_kwargs)
    proxy = model.create_entity("IfcBuildingElementProxy", GlobalId=new_guid(), Name="ProxyOcc")
    return model, building, proxy


def _read_workbook_sheets(xlsx):
    xls = pd.ExcelFile(xlsx)
    payload = {name: pd.read_excel(xls, name) for name in xls.sheet_names}
    xls.close()
    return payload


def _write_workbook_sheets(xlsx, sheets):
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)


def _find_en_entities_relations(model):
    found = []
    for rel in model.by_type("IfcRelAssociatesClassification"):
        cref = getattr(rel, "RelatingClassification", None)
        if cref is None or not cref.is_a("IfcClassificationReference"):
            continue
        src = getattr(cref, "ReferencedSource", None)
        src_name = (getattr(src, "Name", "") or getattr(cref, "Name", "") or "").strip().lower()
        if src_name != "uniclass en entities":
            continue
        value = (getattr(cref, "Identification", "") or getattr(cref, "ItemReference", "") or "").strip()
        name = (getattr(cref, "Name", "") or "").strip()
        found.append((rel, value, name))
    return found


def test_projectdata_en_entities_headers_project_row_are_read_and_written(tmp_path):
    model, _, _ = _build_model("IFC4", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated = tmp_path / "updated.ifc"
    model.write(str(src))

    extract_to_excel(str(src), str(xlsx))
    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "En_25_10_30"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Primary educational buildings"
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)

    update_ifc_from_excel(str(src), str(xlsx), str(updated), add_new="yes")
    reopened = ifcopenshell.open(str(updated))
    found = _find_en_entities_relations(reopened)
    assert len(found) == 1
    assert found[0][1] == "En_25_10_30"
    assert found[0][2] == "Primary educational buildings"


def test_blank_en_entities_is_noop(tmp_path):
    model, _, _ = _build_model("IFC4", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated = tmp_path / "updated.ifc"
    model.write(str(src))
    extract_to_excel(str(src), str(xlsx))

    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = " n/a "
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Primary educational buildings"
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)

    update_ifc_from_excel(str(src), str(xlsx), str(updated), add_new="yes")
    reopened = ifcopenshell.open(str(updated))
    assert len(_find_en_entities_relations(reopened)) == 0


def test_ifc2x3_en_entities_writes_valid_association(tmp_path):
    model, _, _ = _build_model("IFC2X3", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated = tmp_path / "updated.ifc"
    model.write(str(src))
    extract_to_excel(str(src), str(xlsx))

    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "EF_25"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Primary educational buildings"
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)

    update_ifc_from_excel(str(src), str(xlsx), str(updated), add_new="yes")
    reopened = ifcopenshell.open(str(updated))
    found = _find_en_entities_relations(reopened)
    assert len(found) == 1
    rel, value, name = found[0]
    assert value == "EF_25"
    assert name == "Primary educational buildings"
    assert rel.RelatedObjects and reopened.by_id(rel.RelatedObjects[0].id()) is not None


def test_ifc4_en_entities_writes_valid_association(tmp_path):
    model, _, _ = _build_model("IFC4", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated = tmp_path / "updated.ifc"
    model.write(str(src))
    extract_to_excel(str(src), str(xlsx))

    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "EF_40"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = ""
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)

    update_ifc_from_excel(str(src), str(xlsx), str(updated), add_new="yes")
    reopened = ifcopenshell.open(str(updated))
    found = _find_en_entities_relations(reopened)
    assert len(found) == 1
    assert found[0][1] == "EF_40"
    assert found[0][2] == "EF_40"


def test_rerun_updates_existing_en_entities_association(tmp_path):
    model, _, _ = _build_model("IFC4", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated1 = tmp_path / "updated1.ifc"
    updated2 = tmp_path / "updated2.ifc"
    model.write(str(src))
    extract_to_excel(str(src), str(xlsx))

    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "EF_A"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Name A"
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)
    update_ifc_from_excel(str(src), str(xlsx), str(updated1), add_new="yes")

    extract_to_excel(str(updated1), str(xlsx))
    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "EF_B"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Name B"
    sheets["ProjectData"] = project_df
    _write_workbook_sheets(xlsx, sheets)
    update_ifc_from_excel(str(updated1), str(xlsx), str(updated2), add_new="yes")

    reopened = ifcopenshell.open(str(updated2))
    found = _find_en_entities_relations(reopened)
    assert len(found) == 1
    assert found[0][1] == "EF_B"
    assert found[0][2] == "Name B"


def test_en_entities_logic_does_not_modify_target_entity(tmp_path):
    model, _, proxy = _build_model("IFC4", tmp_path)
    src = tmp_path / "src.ifc"
    xlsx = tmp_path / "roundtrip.xlsx"
    updated = tmp_path / "updated.ifc"
    model.write(str(src))
    extract_to_excel(str(src), str(xlsx))

    sheets = _read_workbook_sheets(xlsx)
    project_df = sheets["ProjectData"]
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnReference"] = "EF_99"
    project_df.loc[project_df["DataType"] == "Project", "UniclassEnName"] = "Name 99"
    sheets["ProjectData"] = project_df

    elements_df = sheets["Elements"]
    row = elements_df[elements_df["GlobalId"] == proxy.GlobalId].iloc[0]
    elements_df.loc[elements_df["RowKey"] == row["RowKey"], "TargetEntity"] = "IfcWall"
    elements_df.loc[elements_df["RowKey"] == row["RowKey"], "ApplyChange"] = "No"
    sheets["Elements"] = elements_df
    _write_workbook_sheets(xlsx, sheets)

    update_ifc_from_excel(str(src), str(xlsx), str(updated), add_new="yes")
    reopened = ifcopenshell.open(str(updated))
    assert reopened.by_guid(proxy.GlobalId).is_a() == "IfcBuildingElementProxy"
