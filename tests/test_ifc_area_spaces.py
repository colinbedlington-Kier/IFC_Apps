import asyncio
import json
from pathlib import Path

import app

from backend.ifc_area_spaces import Candidate, LayerSignal, ScanResult, get_ifcspace_layer_signals, is_area_space_candidate, purge_area_spaces, scan_ifc_for_area_spaces


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

    monkeypatch.setattr(app, "scan_ifc_for_area_spaces", lambda _, **__: fake_result)

    payload = asyncio.run(app.area_spaces_scan({"session_id": session_id, "file_names": [source_name]}))
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
    assert "Area Spaces API is not mounted in this deployment. Check backend router registration." in purge_js
    assert "Processing failed or server restarted. The IFC may be too large or another heavy job may have exhausted memory." in purge_js


def test_scan_uses_ifcspace_only(monkeypatch, tmp_path: Path):
    source = tmp_path / "tiny.ifc"
    source.write_text(
        "ISO-10303-21;\n"
        "DATA;\n"
        "#10=IFCSPACE('GID10',#1,'Area 101',$,$,$,#50,'',$,.INTERNAL.,$);\n"
        "#50=IFCPRODUCTDEFINITIONSHAPE($,$,(#60));\n"
        "#60=IFCSHAPEREPRESENTATION(#2,'Body','SweptSolid',(#70));\n"
        "#80=IFCPRESENTATIONLAYERASSIGNMENT('A-AREA',$,(#70),$);\n"
        "ENDSEC;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("backend.ifc_area_spaces.ifcopenshell.open", lambda _: (_ for _ in ()).throw(RuntimeError("should not call open")))
    result = scan_ifc_for_area_spaces(source)
    assert result.total_spaces == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].reason == "streaming_text_match"
    assert result.candidates[0].confidence == "probable"


def test_scan_ifcopenshell_mode_only_when_enabled(monkeypatch, tmp_path: Path):
    class _FakeModel:
        def by_type(self, name):
            if name == "IfcSpace":
                return []
            return []

    source = tmp_path / "tiny.ifc"
    source.write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    monkeypatch.setenv("AREA_SPACE_SCAN_MODE", "ifcopenshell")
    observed = {}

    def _fake_open(path, **kwargs):
        observed["path"] = path
        observed["kwargs"] = kwargs
        return _FakeModel()

    monkeypatch.setattr("backend.ifc_area_spaces.ifcopenshell.open", _fake_open)
    result = scan_ifc_for_area_spaces(source)
    assert result.total_spaces == 0
    assert observed["kwargs"].get("lazy") is True


def test_scan_error_returns_json_not_crash(monkeypatch):
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / "areas.ifc").write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    monkeypatch.setattr(app, "scan_ifc_for_area_spaces", lambda _: (_ for _ in ()).throw(RuntimeError("boom")))

    resp = asyncio.run(app.area_spaces_scan({"session_id": session_id, "file_names": ["areas.ifc"]}))
    assert resp.status_code == 500
    payload = resp.body.decode("utf-8")
    assert "AREA_SPACE_SCAN_FAILED" in payload
    assert "\"ok\":false" in payload


def test_oversize_file_returns_413(monkeypatch):
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    source_name = "areas.ifc"
    (root / source_name).write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    monkeypatch.setattr(app, "AREA_SPACE_MAX_PURGE_FILE_MB", -0.1)
    resp = asyncio.run(
        app.area_spaces_purge(
            {
                "session_id": session_id,
                "selected_candidates": [{"source_file": "areas.ifc", "global_id": "A1"}],
                "file_names": ["areas.ifc"],
            }
        )
    )
    assert resp.status_code == 413


def test_scan_waits_for_job_semaphore(monkeypatch):
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / "areas.ifc").write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    fake_result = ScanResult(source_file="areas.ifc", total_spaces=0, candidates=[])
    monkeypatch.setattr(app, "scan_ifc_for_area_spaces", lambda _: fake_result)

    async def _run():
        await app.AREA_SPACE_JOB_SEMAPHORE.acquire()
        task = asyncio.create_task(app.area_spaces_scan({"session_id": session_id, "file_names": ["areas.ifc"]}))
        await asyncio.sleep(0.02)
        assert not task.done()
        app.AREA_SPACE_JOB_SEMAPHORE.release()
        result = await task
        assert result["ok"] is True

    asyncio.run(_run())


def test_purge_memory_high_returns_503(monkeypatch):
    session_id = app.SESSION_STORE.create()
    root = Path(app.SESSION_STORE.ensure(session_id))
    (root / "areas.ifc").write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    monkeypatch.setattr(app, "is_memory_high", lambda: True)
    resp = asyncio.run(
        app.area_spaces_purge(
            {
                "session_id": session_id,
                "selected_candidates": [{"source_file": "areas.ifc", "global_id": "A1"}],
                "file_names": ["areas.ifc"],
            }
        )
    )
    assert resp.status_code == 503
    payload = json.loads(resp.body.decode("utf-8"))
    assert payload["error"] == "INSUFFICIENT_MEMORY_FOR_SAFE_PURGE"


def test_purge_removes_selected_space_and_preserves_other(monkeypatch, tmp_path: Path):
    class _RelBoundary:
        def __init__(self, relating_space):
            self.RelatingSpace = relating_space
            self.RelatedBuildingElement = None

    class _FakeModel:
        def __init__(self, spaces):
            self._spaces = spaces
            self._boundaries = [_RelBoundary(spaces[0])]
            self.removed_ids = []

        def by_type(self, name):
            if name == "IfcSpace":
                return list(self._spaces)
            if name == "IfcRelSpaceBoundary":
                return list(self._boundaries)
            return []

        def remove(self, entity):
            if hasattr(entity, "id"):
                self.removed_ids.append(entity.id())

        def write(self, _):
            return None

    area = _space_with_property("Information CAD Layer", "Area")
    area._sid = 1
    area.GlobalId = "AREA-1"
    room = _space_with_property("Category", "Room")
    room._sid = 2
    room.GlobalId = "ROOM-2"
    model = _FakeModel([area, room])
    monkeypatch.setattr("backend.ifc_area_spaces.ifcopenshell.open", lambda _: model)
    source = tmp_path / "areas.ifc"
    source.write_text("ISO-10303-21;\nENDSEC;\n", encoding="utf-8")
    output = tmp_path / "areas.area-spaces-purged.ifc"
    purge_area_spaces(source, ["AREA-1"], output)
    assert 1 in model.removed_ids
    assert 2 not in model.removed_ids
