import pandas as pd
import ifcopenshell
import ifcopenshell.util.element
from ifcopenshell.guid import new as new_guid

from app import extract_to_excel, update_ifc_from_excel


def _attach_pset(model, element, pset_name: str, values: dict) -> None:
    props = []
    for key, value in values.items():
        props.append(
            model.create_entity(
                "IfcPropertySingleValue",
                Name=key,
                NominalValue=model.create_entity("IfcLabel", str(value)),
            )
        )
    pset = model.create_entity("IfcPropertySet", GlobalId=new_guid(), Name=pset_name, HasProperties=props)
    model.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId=new_guid(),
        RelatedObjects=[element],
        RelatingPropertyDefinition=pset,
    )


def _build_model() -> ifcopenshell.file:
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj", LongName="PN-001")
    site = model.create_entity("IfcSite", GlobalId=new_guid(), Name="Site")
    building = model.create_entity("IfcBuilding", GlobalId=new_guid(), Name="Bldg")
    model.create_entity("IfcRelAggregates", GlobalId=new_guid(), RelatingObject=project, RelatedObjects=[site])
    model.create_entity("IfcRelAggregates", GlobalId=new_guid(), RelatingObject=site, RelatedObjects=[building])

    wall_type = model.create_entity("IfcWallType", GlobalId=new_guid(), Name="LegacyBlockType")
    wall = model.create_entity("IfcWall", GlobalId=new_guid(), Name="WallName", ObjectType="WallUserType")
    model.create_entity("IfcRelDefinesByType", GlobalId=new_guid(), RelatedObjects=[wall], RelatingType=wall_type)

    _attach_pset(
        model,
        wall,
        "Additional_Pset_GeneralCommon",
        {
            "IFC Name": "PreferredIfcName",
            "ExtObject": "IfcWall",
            "IFC_Enumeration": "IfcWall",
            "SystemName": "Drainage",
            "SystemDescription": "Drainage System",
            "SystemCategory": "Utilities",
            "ClassificationCode": "Pr_20",
            "Uniclass2015_Pr": "Pr_20_76",
            "Type (User Defined)": "WallUDT",
        },
    )

    point = model.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0))
    axis = model.create_entity("IfcAxis2Placement3D", Location=point)
    context = model.create_entity(
        "IfcGeometricRepresentationContext",
        ContextIdentifier="Body",
        ContextType="Model",
        CoordinateSpaceDimension=3,
        Precision=1e-5,
        WorldCoordinateSystem=axis,
    )
    item = model.create_entity("IfcPolyline", Points=[])
    shape = model.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="Curve2D",
        Items=[item],
    )
    wall.Representation = model.create_entity("IfcProductDefinitionShape", Representations=[shape])
    model.create_entity("IfcPresentationLayerAssignment", Name="A-WALL", AssignedItems=[shape])
    return model


def test_civil3d_profile_extract_and_update_round_trip(tmp_path):
    model = _build_model()
    src_ifc = tmp_path / "source.ifc"
    model.write(str(src_ifc))

    xlsx = tmp_path / "extract.xlsx"
    extract_to_excel(str(src_ifc), str(xlsx), plan_payload={"profile": "civil3d_extended"})

    xls = pd.ExcelFile(xlsx)
    project_df = pd.read_excel(xls, "ProjectData")
    elements_df = pd.read_excel(xls, "Elements")
    cobie_df = pd.read_excel(xls, "COBieMapping")
    properties_df = pd.read_excel(xls, "Properties")
    uniclass_pr_df = pd.read_excel(xls, "Uniclass_Pr")
    uniclass_ss_df = pd.read_excel(xls, "Uniclass_Ss")
    uniclass_ef_df = pd.read_excel(xls, "Uniclass_EF")

    assert "ProjectNumber" in project_df.columns
    assert project_df.loc[project_df["DataType"] == "Project", "ProjectNumber"].iloc[0] == "PN-001"

    guid = elements_df.iloc[0]["GlobalId"]
    assert cobie_df.iloc[0]["IFCElement.Name"] == "PreferredIfcName"
    assert cobie_df.iloc[0]["IFCElementType.Name"] == "PreferredIfcName"
    assert elements_df.iloc[0]["ExtObject"] == "IfcWall"

    project_df.loc[project_df["DataType"] == "Project", "ProjectNumber"] = "PN-UPDATED"
    elements_df.loc[elements_df["GlobalId"] == guid, "IFCPresentationLayer"] = "A-WALL-UPDATED"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        project_df.to_excel(writer, sheet_name="ProjectData", index=False)
        elements_df.to_excel(writer, sheet_name="Elements", index=False)
        properties_df.to_excel(writer, sheet_name="Properties", index=False)
        cobie_df.to_excel(writer, sheet_name="COBieMapping", index=False)
        uniclass_pr_df.to_excel(writer, sheet_name="Uniclass_Pr", index=False)
        uniclass_ss_df.to_excel(writer, sheet_name="Uniclass_Ss", index=False)
        uniclass_ef_df.to_excel(writer, sheet_name="Uniclass_EF", index=False)

    updated_ifc = tmp_path / "updated.ifc"
    update_ifc_from_excel(str(src_ifc), str(xlsx), str(updated_ifc), add_new="yes")
    reextract = tmp_path / "reextract.xlsx"
    extract_to_excel(str(updated_ifc), str(reextract), plan_payload={"profile": "civil3d_extended"})
    re_project_df = pd.read_excel(reextract, sheet_name="ProjectData")
    re_elements_df = pd.read_excel(reextract, sheet_name="Elements")

    assert re_project_df.loc[re_project_df["DataType"] == "Project", "ProjectNumber"].iloc[0] == "PN-UPDATED"
    assert "A-WALL-UPDATED" in re_elements_df.iloc[0]["IFCPresentationLayer"]
    assert re_elements_df.iloc[0]["ExtObject"] == "IfcWall"
