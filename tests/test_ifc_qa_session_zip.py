from pathlib import Path
import csv
import zipfile

import ifc_qa_service


def _fake_process_file(job_id, ifc_path, out_root, config, include):
    source = Path(ifc_path).name
    mappings = {
        "IFC Classification": f"IFC CLASSIFICATION - {source}.csv",
        "IFC Object Type": f"IFC OBJECT TYPE - {source}.csv",
        "IFC Project": f"IFC PROJECT - {source}.csv",
        "IFC Properties": f"IFC PROPERTIES - {source}.csv",
        "IFC Pset Template": f"IFC PSET TEMPLATE - {source}.csv",
        "IFC Spatial Structure": f"IFC SPATIAL - {source}.csv",
        "IFC System": f"IFC SYSTEM - {source}.csv",
    }
    for folder, name in mappings.items():
        p = out_root / folder / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("h1,h2\n1,2\n", encoding="utf-8")
    return [f"session://{job_id}/{source}", source, "codes", "IFC4", "2026-01-01T00:00:00Z"]


def test_fresh_run_creates_manifest_and_zip(tmp_path, monkeypatch):
    monkeypatch.setattr(ifc_qa_service, "_process_file", _fake_process_file)
    session_root = tmp_path / "session"
    session_root.mkdir()
    file_a = tmp_path / "A.ifc"
    file_a.write_text("IFC", encoding="utf-8")

    job_id = ifc_qa_service.REGISTRY.create(session_id="s1")
    ifc_qa_service.run_session_job(job_id, session_root, "s1", [("A.ifc", str(file_a))], {}, {}, mode="replace")

    summary = ifc_qa_service.read_session_summary(session_root, "s1")
    assert summary["model_count"] == 1
    assert summary["has_zip"] is True
    assert (session_root / "manifest.json").exists()
    assert (session_root / "IFC Output" / "IFC Models" / "IFC MODEL TABLE.csv").exists()


def test_add_to_zip_appends_and_duplicate_replaces(tmp_path, monkeypatch):
    monkeypatch.setattr(ifc_qa_service, "_process_file", _fake_process_file)
    session_root = tmp_path / "session"
    session_root.mkdir()
    file_a = tmp_path / "A.ifc"
    file_b = tmp_path / "B.ifc"
    file_a.write_text("IFC", encoding="utf-8")
    file_b.write_text("IFC", encoding="utf-8")

    first_job = ifc_qa_service.REGISTRY.create(session_id="s2")
    ifc_qa_service.run_session_job(first_job, session_root, "s2", [("A.ifc", str(file_a))], {}, {}, mode="replace")
    second_job = ifc_qa_service.REGISTRY.create(session_id="s2")
    ifc_qa_service.run_session_job(second_job, session_root, "s2", [("B.ifc", str(file_b)), ("A.ifc", str(file_a))], {}, {}, mode="append")

    model_table = session_root / "IFC Output" / "IFC Models" / "IFC MODEL TABLE.csv"
    with open(model_table, newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    sources = [r[1] for r in rows[1:]]
    assert sources == ["A.ifc", "B.ifc"]

    with zipfile.ZipFile(session_root / "IFC Output.zip", "r") as zf:
        members = set(zf.namelist())
    assert "IFC Output/IFC Models/IFC MODEL TABLE.csv" in members
    assert "IFC Output/IFC Project/IFC PROJECT - A.ifc.csv" in members
    assert "IFC Output/IFC Project/IFC PROJECT - B.ifc.csv" in members
