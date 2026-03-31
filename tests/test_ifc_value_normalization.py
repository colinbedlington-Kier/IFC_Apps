import pandas as pd
import ifcopenshell
from ifcopenshell.guid import new as new_guid

import app
from app import extract_to_excel


class Wrapped:
    def __init__(self, value):
        self.wrappedValue = value


class CustomObject:
    def __str__(self):
        return "custom-value"


class AttrObject:
    def __init__(self, value):
        self.value = value


def _build_ifc_with_props(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    site = model.create_entity("IfcSite", GlobalId=new_guid(), Name="Site")
    building = model.create_entity("IfcBuilding", GlobalId=new_guid(), Name="Bldg")
    model.create_entity("IfcRelAggregates", GlobalId=new_guid(), RelatingObject=project, RelatedObjects=[site])
    model.create_entity("IfcRelAggregates", GlobalId=new_guid(), RelatingObject=site, RelatedObjects=[building])

    wall = model.create_entity("IfcWall", GlobalId=new_guid(), Name="WallName")
    p_ok = model.create_entity("IfcPropertySingleValue", Name="SafeProp", NominalValue=model.create_entity("IfcLabel", "ok"))
    p_broken = model.create_entity("IfcPropertySingleValue", Name="BrokenProp", NominalValue=model.create_entity("IfcLabel", "boom"))
    pset = model.create_entity("IfcPropertySet", GlobalId=new_guid(), Name="Pset_Test", HasProperties=[p_ok, p_broken])
    model.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId=new_guid(),
        RelatedObjects=[wall],
        RelatingPropertyDefinition=pset,
    )

    src_ifc = tmp_path / "mixed.ifc"
    model.write(str(src_ifc))
    return src_ifc


def test_normalize_ifc_value_shapes():
    assert app._normalize_ifc_value(None) is None
    assert app._normalize_ifc_value(Wrapped("A")) == "A"
    assert app._normalize_ifc_value((Wrapped("A"), 2, None)) == "A | 2 | "
    assert app._normalize_ifc_value([Wrapped("B"), 3]) == "B | 3"
    assert app._normalize_ifc_value(AttrObject("inner")) == "inner"
    assert app._normalize_ifc_value(CustomObject()) == "custom-value"


def test_extract_to_excel_continues_when_single_property_fails(tmp_path, monkeypatch):
    src_ifc = _build_ifc_with_props(tmp_path)
    out_xlsx = tmp_path / "out.xlsx"

    original = app._extract_nominal_value

    def _flaky_extract(prop):
        if getattr(prop, "Name", "") == "BrokenProp":
            raise ValueError("simulated parse error")
        return original(prop)

    monkeypatch.setattr(app, "_extract_nominal_value", _flaky_extract)

    extract_to_excel(str(src_ifc), str(out_xlsx), plan_payload={"include_sheets": ["Properties"]})

    props = pd.read_excel(out_xlsx, sheet_name="Properties")
    safe_row = props[props["Property"] == "SafeProp"].iloc[0]
    broken_row = props[props["Property"] == "BrokenProp"].iloc[0]

    assert safe_row["Value"] == "ok"
    assert pd.isna(broken_row["Value"]) or broken_row["Value"] in ("", None)
