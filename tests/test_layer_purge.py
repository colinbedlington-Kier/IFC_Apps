import ifcopenshell
import ifcopenshell.util.element
from ifcopenshell.guid import new as new_guid

from app import (
    apply_layer_changes,
    compute_shallow_layer,
    parse_allowed_layers,
    propose_layer_mapping,
    scan_layers,
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
    layer = model.create_entity("IfcPresentationLayerAssignment", Name=layer_value)
    shape = model.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="SweptSolid",
        Items=[],
        LayerAssignments=[layer],
    )
    definition = model.create_entity("IfcProductDefinitionShape", Representations=[shape])
    wall.Representation = definition

    prop = model.create_entity(
        "IfcPropertySingleValue",
        Name="Layer",
        NominalValue=model.create_entity("IfcLabel", layer_value),
        Unit=None,
    )
    pset = model.create_entity("IfcPropertySet", GlobalId=new_guid(), Name="Pset_Test", HasProperties=[prop])
    model.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId=new_guid(),
        RelatedObjects=[wall],
        RelatingPropertyDefinition=pset,
    )
    return model


def test_parse_allowed_layers_and_shallowing():
    text = "One Of [Z-Ss7550--MechanicalAndElectricalServicesControlProducts, A-Ss1230--Example]"
    allowed = parse_allowed_layers(text)
    assert "Z-Ss7550--MechanicalAndElectricalServicesControlProducts" in allowed
    assert compute_shallow_layer("Z-Ss755028--FireAndSmokeDetectionAndAlarmSystems") == "Z-Ss7550--"
    mapping = propose_layer_mapping(
        "Z-Ss755028--FireAndSmokeDetectionAndAlarmSystems",
        allowed,
        {"Z-Ss755028--FireAndSmokeDetectionAndAlarmSystems": "Z-Ss7550--MechanicalAndElectricalServicesControlProducts"},
        True,
    )
    assert mapping["reason"] == "Explicit"
    assert mapping["target"] == "Z-Ss7550--MechanicalAndElectricalServicesControlProducts"


def test_apply_layer_changes_updates_ifc(tmp_path):
    deep_layer = "Z-Ss755028--FireAndSmokeDetectionAndAlarmSystems"
    target_layer = "Z-Ss7550--MechanicalAndElectricalServicesControlProducts"
    model = build_layer_model(deep_layer)
    in_path = tmp_path / "input.ifc"
    model.write(str(in_path))

    stats, rows = scan_layers(
        str(in_path),
        allowed_set=set(),
        explicit_map={deep_layer: target_layer},
        options={"auto_shallow": True},
    )
    assert stats["rows"] > 0
    out_path, _, _ = apply_layer_changes(str(in_path), [rows[0]], {"update_both": True})
    updated = ifcopenshell.open(out_path)
    wall = updated.by_type("IfcWall")[0]
    layers = ifcopenshell.util.element.get_layers(updated, wall)
    assert layers[0].Name == target_layer
    prop = updated.by_type("IfcPropertySingleValue")[0]
    assert prop.NominalValue.wrappedValue == target_layer
