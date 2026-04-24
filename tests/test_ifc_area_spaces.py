from pathlib import Path

import app
from backend.ifc_area_spaces import Candidate, LayerSignal, ScanResult, get_ifcspace_layer_signals, is_area_space_candidate


class _FakeSpace:
    def __init__(self, type_name="IfcSpace", sid=42, global_id="GID", name="Name", long_name="", object_type=""):
        self._type_name = type_name
        self._sid = sid
        self.GlobalId = global_id
        self.Name = name
        self.LongName = long_name
        self.ObjectType = object_type
        self.Representation = None
        self.IsDefinedBy = []
        self.Decomposes = []
        self.ContainedInStructure = []

    def is_a(self, query=None):
        if query is None:
            return self._type_name
        return self._type_name == query

    def id(self):
        return self._sid


class _Nominal:
    def __init__(self, value):
        self.wrappedValue = value


class _Prop:
    def __init__(self, name, value):
        self.Name = name
        self.NominalValue = _Nominal(value)


class _Pset:
    def __init__(self, props):
        self.HasProperties = props


class _RelDef:
    def __init__(self, pset):
        self.RelatingPropertyDefinition = pset


def _space_with_property(name, value):
    space = _FakeSpace(name="Room")
    pset = _Pset([_Prop(name, value)])
    space.IsDefinedBy = [_RelDef(pset)]
    return space


def test_normal_room_ifcspace_not_flagged():
    room = _space_with_property("Category", "Room")
    assert is_area_space_candidate(room) is None


def test_area_like_values_flagged_case_insensitive():
    for value in ["A-AREA", "Area", "Rooms_Area", "aReA"]:
        space = _space_with_property("Information CAD Layer", value)
        candidate = is_area_space_candidate(space)
        assert candidate is not None
        assert candidate.matched_value == value


def test_information_cad_layer_name_flagged_even_without_prefix():
    space = _space_with_property("Information CAD Layer", "Area")
    candidate = is_area_space_candidate(space)
    assert candidate is not None
    assert candidate.matched_name == "Information CAD Layer"


def test_non_ifcspace_never_flagged():
    wall = _space_with_property("Information CAD Layer", "Area")
    wall._type_name = "IfcWall"
    assert is_area_space_candidate(wall) is None


def test_get_ifcspace_layer_signals_extracts_property_layer_signal():
    space = _space_with_property("Presentation Layer", "Area")
    signals = get_ifcspace_layer_signals(space)
    assert any(isinstance(signal, LayerSignal) and signal.source == "property_set" for signal in signals)


def test_area_spaces_scan_api_shape(tmp_path: Path, monkeypatch):
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    source_name = "areas.ifc"
    (root / source_name).write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")

    fake_result = ScanResult(
        source_file=source_name,
        total_spaces=3,
        candidates=[
            Candidate(
                global_id="ABC123",
                step_id=55,
                name="AreaSpace",
                long_name="",
                object_type="",
                matched_source="property_set",
                matched_name="Information CAD Layer",
                matched_value="Area",
                reason="property-layer-signal",
                has_representation=False,
                spatial_parent="",
            )
        ],
    )

    monkeypatch.setattr(app, "scan_ifc_for_area_spaces", lambda _: fake_result)

    payload = app.area_spaces_scan({"session_id": session_id, "file_names": [source_name]})
    assert payload["files_scanned"] == 1
    assert payload["results"][0]["source_file"] == source_name
    candidate = payload["results"][0]["candidates"][0]
    assert "global_id" in candidate
    assert "matched_source" in candidate
    assert "reason" in candidate


def test_purge_area_spaces_frontend_uses_active_session_and_shared_files_endpoint():
    root = Path(__file__).resolve().parent.parent
    purge_js = (root / "static" / "purge_area_spaces.js").read_text(encoding="utf-8")

    assert "getActiveSessionId" in purge_js
    assert "/api/session/${encodeURIComponent(sessionId)}/files?page=${encodeURIComponent(PAGE_NAME)}" in purge_js
    assert ".endsWith(\".ifc\")" in purge_js
    assert "No active session. Create/upload in Upload & Session first." in purge_js
    assert "Active session found, but no IFC files are available." in purge_js
    assert "Session has files, but none are .ifc files." in purge_js
    assert "localStorage.getItem(\"ifc_session_id\")" not in purge_js
