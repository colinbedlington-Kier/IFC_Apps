import ifcopenshell
from ifcopenshell.guid import new as new_guid

from classification_writer import attach_classification, count_classification_relationships, find_classification_value
from expression_engine import ExpressionEngine
from field_access import FieldDescriptor, FieldKind, get_value, set_value


def make_basic_model():
    model = ifcopenshell.file(schema="IFC4")
    project = model.create_entity("IfcProject", GlobalId=new_guid(), Name="Proj")
    wall = model.create_entity("IfcWall", GlobalId=new_guid(), Name="Wall-1")
    return model, wall


def test_classification_writeback_preserves_existing():
    model, wall = make_basic_model()
    attach_classification(model, wall, "Existing System", "X-01")
    before = count_classification_relationships(wall)
    attach_classification(model, wall, "New System", "Y-02")
    after = count_classification_relationships(wall)
    assert find_classification_value(wall, "Existing System") == "X-01"
    assert find_classification_value(wall, "New System") == "Y-02"
    assert after == before + 1


def test_expression_engine_resolves_tokens():
    model, wall = make_basic_model()
    fd_prop = FieldDescriptor(kind=FieldKind.PROPERTY, pset_name="Pset_RoomCommon", property_name="RoomTag")
    set_value(model, wall, fd_prop, "A-101")
    attach_classification(model, wall, "Uniclass 2015", "EF_25_10")
    engine = ExpressionEngine(model)
    expr = "{Pset_RoomCommon.RoomTag}-{Name}-{Class.Uniclass 2015}"
    result = engine.evaluate(expr, wall)
    assert "A-101" in result
    assert "Wall-1" in result
    assert "EF_25_10" in result


def test_property_write_creates_pset_when_missing():
    model, wall = make_basic_model()
    fd_prop = FieldDescriptor(kind=FieldKind.PROPERTY, pset_name="Pset_Custom", property_name="Code")
    assert get_value(wall, fd_prop) is None
    set_value(model, wall, fd_prop, "C-001")
    assert get_value(wall, fd_prop) == "C-001"
