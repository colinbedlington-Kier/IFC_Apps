import ifcopenshell
import ifcopenshell.util.element
from ifcopenshell.guid import new as new_guid

from app import (
    apply_layer_changes,
    build_allowed_layers,
    build_layer_review,
    parse_allowed_layers_csv_text,
)


def build_layer_model(layer_value: str) -> ifcopenshell.file:
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    _ = project
    wall = model.create_entity("IfcWall", GlobalId=new_guid(), Name="Wall-1")

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
    shape = model.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="SweptSolid",
        Items=[],
    )
    model.create_entity("IfcPresentationLayerAssignment", Name=layer_value, AssignedItems=[shape])
    definition = model.create_entity("IfcProductDefinitionShape", Representations=[shape])
    wall.Representation = definition
    return model




def build_classified_model(uniclass_code: str, name: str) -> ifcopenshell.file:
    model = ifcopenshell.file(schema="IFC4")
    model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    wall = model.create_entity("IfcWall", GlobalId=new_guid(), Name="Wall-1")
    classification = model.create_entity("IfcClassification", Name="Uniclass 2015")
    class_ref = model.create_entity(
        "IfcClassificationReference",
        Identification=uniclass_code,
        Name=name,
        ReferencedSource=classification,
    )
    model.create_entity(
        "IfcRelAssociatesClassification",
        GlobalId=new_guid(),
        RelatedObjects=[wall],
        RelatingClassification=class_ref,
    )
    return model


def test_parse_allowed_layers_csv_text_and_merge():
    csv_text = "layer_code,layer_description\nZ-Ss7550,MechanicalAndElectricalServicesControlProducts\n"
    parsed = parse_allowed_layers_csv_text(csv_text)
    assert not parsed["errors"]
    assert parsed["rows"][0]["full_layer"] == "Z-Ss7550--MechanicalAndElectricalServicesControlProducts"

    merged = build_allowed_layers(csv_text, use_uploaded_only=False)
    assert "Z-Ss7550--MechanicalAndElectricalServicesControlProducts" in merged["full_values"]


def test_extract_and_apply_layer_changes(tmp_path):
    deep_layer = "Z-Ss755028--FireAndSmokeDetectionAndAlarmSystems"
    target_layer = "Z-Ss7550--MechanicalAndElectricalServicesControlProducts"
    model = build_layer_model(deep_layer)
    in_path = tmp_path / "input.ifc"
    model.write(str(in_path))

    review = build_layer_review(str(in_path), [target_layer])
    assert review["summary"]["layers_found"] == 1
    row = review["rows"][0]
    row["final_layer"] = target_layer
    row["apply_change"] = True

    out_path, _, _, summary = apply_layer_changes(str(in_path), [row], {"update_both": False})
    assert summary["changed"] == 1

    updated = ifcopenshell.open(out_path)
    wall = updated.by_type("IfcWall")[0]
    layers = ifcopenshell.util.element.get_layers(updated, wall)
    assert layers[0].Name == target_layer


def test_extract_uses_uniclass_ss_fallback_when_no_layers(tmp_path):
    model = build_classified_model("Ss_25_10", "Wall systems")
    in_path = tmp_path / "classified.ifc"
    model.write(str(in_path))

    review = build_layer_review(str(in_path), ["Z-Ss7550--MechanicalAndElectricalServicesControlProducts"])
    assert review["summary"]["source_mode"] == "uniclass_ss_fallback"
    assert review["summary"]["classification_candidates"] == 1
    assert review["rows"][0]["status"] == "classification_candidate"
    assert review["rows"][0]["existing_layer"].startswith("Ss_25_10")
