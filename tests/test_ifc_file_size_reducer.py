from pathlib import Path

import ifcopenshell
import pytest
from fastapi import HTTPException
from ifcopenshell.guid import new as new_guid

from app import (
    SESSION_STORE,
    api_reduce_file_size_analyse,
    api_reduce_file_size_run,
)


def _build_sample_ifc(path: Path) -> None:
    model = ifcopenshell.file(schema="IFC4")
    model.create_entity("IfcProject", GlobalId=new_guid(), Name="Project")
    site = model.create_entity("IfcSite", GlobalId=new_guid(), Name="Site")
    site.Representation = model.create_entity("IfcProductDefinitionShape", Representations=[])
    model.create_entity("IfcWall", GlobalId=new_guid(), Name="Wall-1")
    model.write(str(path))


def _session_with_ifc(tmp_path: Path) -> tuple[str, str]:
    session_id = SESSION_STORE.create()
    root = Path(SESSION_STORE.ensure(session_id))
    source_name = "sample.ifc"
    source_path = tmp_path / source_name
    _build_sample_ifc(source_path)
    (root / source_name).write_bytes(source_path.read_bytes())
    return session_id, source_name


def test_reduce_analyse_endpoint(tmp_path):
    session_id, source = _session_with_ifc(tmp_path)
    response = api_reduce_file_size_analyse({"session_id": session_id, "source_file": source})
    analysis = response["analysis"]
    assert analysis["schema"] == "IFC4"
    assert analysis["product_count"] >= 1
    assert "recommendation" in analysis


def test_reduce_run_compress_only(tmp_path):
    session_id, source = _session_with_ifc(tmp_path)
    response = api_reduce_file_size_run(
        {
            "session_id": session_id,
            "source_file": source,
            "mode": "compress_only",
            "export_ifczip": True,
            "output_prefix": "reduced",
        }
    )
    result = response["result"]
    names = [entry["name"] for entry in result["output_files"]]
    assert any(name.endswith(".ifc") for name in names)
    assert any(name.endswith(".ifczip") for name in names)
    assert result["summary_json_path"].endswith(".json")


def test_reduce_run_conservative_remove_site(monkeypatch, tmp_path):
    session_id, source = _session_with_ifc(tmp_path)
    seen = []

    def fake_recipe(input_path, output_path, recipe, arguments=None):
        seen.append(recipe)
        output_path.write_bytes(input_path.read_bytes())

    monkeypatch.setattr("backend.ifc_file_size_reducer.run_ifcpatch_recipe", fake_recipe)

    response = api_reduce_file_size_run(
        {
            "session_id": session_id,
            "source_file": source,
            "mode": "conservative_viewer_copy",
            "remove_site_representation": True,
            "output_prefix": "coord",
        }
    )
    assert response["status"] == "ok"
    assert "RemoveSiteRepresentation" in seen


def test_reduce_run_aggressive_requires_warning_ack(tmp_path):
    session_id, source = _session_with_ifc(tmp_path)
    with pytest.raises(HTTPException) as exc:
        api_reduce_file_size_run(
            {
                "session_id": session_id,
                "source_file": source,
                "mode": "aggressive_viewer_copy",
                "purge_data": True,
                "warning_acknowledged": False,
            }
        )
    assert exc.value.status_code == 400
    assert "warning" in str(exc.value.detail).lower()
