from __future__ import annotations

import argparse
import csv
import json
import os
import resource
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

import ifcopenshell

from backend.ifc_area_spaces import cleanup_space_relationships, get_memory_status, is_area_space_candidate


def _set_memory_limit() -> int:
    limit_mb = int(os.getenv("AREA_SPACE_PURGE_CHILD_MEMORY_MB", "8192"))
    limit_bytes = limit_mb * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    return limit_mb


def _write_csv_report(path: Path, rows: List[Dict[str, Any]]) -> None:
    headers = [
        "source_file",
        "step_id",
        "global_id",
        "name",
        "long_name",
        "object_type",
        "matched_source",
        "matched_name",
        "matched_value",
        "reason",
        "has_representation",
        "spatial_parent",
        "status",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _log(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload), file=sys.stderr)


def run_worker(source_ifc: Path, output_ifc: Path, selected_json: Path, audit_csv: Path) -> Dict[str, Any]:
    _log("worker_start", source_ifc=source_ifc.name)
    limit_mb = _set_memory_limit()
    _log("memory_limit_set", memory_limit_mb=limit_mb)

    selected_tokens = {str(token).strip() for token in json.loads(selected_json.read_text(encoding="utf-8")) if str(token).strip()}
    if not selected_tokens:
        raise ValueError("No selected GlobalIds/STEP ids provided")

    _log("ifc_open_start", source_ifc=source_ifc.name)
    opened = time.perf_counter()
    model = ifcopenshell.open(str(source_ifc))
    _log("ifc_open_complete", duration_ms=int((time.perf_counter() - opened) * 1000), memory=get_memory_status())

    spaces = model.by_type("IfcSpace")
    candidates_found = 0
    selected_ids: set[int] = set()
    candidate_rows = []
    for space in spaces:
        candidate = is_area_space_candidate(space)
        if candidate is None:
            continue
        candidates_found += 1
        candidate_rows.append(candidate)
        if candidate.global_id in selected_tokens or str(candidate.step_id) in selected_tokens:
            selected_ids.add(int(candidate.step_id))

    to_remove = [space for space in spaces if int(space.id()) in selected_ids]
    remove_ids = {int(space.id()) for space in to_remove}
    _log("selected_spaces_resolved", selected_count=len(remove_ids), candidates_found=candidates_found)

    rel_start = time.perf_counter()
    _log("relationships_cleanup_start", selected_count=len(remove_ids))
    removed_relationships = cleanup_space_relationships(model, remove_ids)
    _log("relationships_cleanup_complete", count=removed_relationships, duration_ms=int((time.perf_counter() - rel_start) * 1000), memory=get_memory_status())

    for space in to_remove:
        model.remove(space)

    output_ifc.parent.mkdir(parents=True, exist_ok=True)
    write_start = time.perf_counter()
    _log("ifc_write_start", output_ifc=output_ifc.name, memory=get_memory_status())
    model.write(str(output_ifc))
    _log("ifc_write_complete", duration_ms=int((time.perf_counter() - write_start) * 1000), memory=get_memory_status())

    rows: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        payload = asdict(candidate)
        payload["source_file"] = source_ifc.name
        payload["status"] = "purged" if candidate.step_id in remove_ids else "not_selected"
        rows.append(payload)
    _write_csv_report(audit_csv, rows)

    return {
        "ok": True,
        "source_file": source_ifc.name,
        "total_spaces": len(spaces),
        "candidates_found": candidates_found,
        "selected_for_purge": len(selected_tokens),
        "purged_count": len(remove_ids),
        "output_ifc": output_ifc.name,
        "report_csv": audit_csv.name,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-ifc", required=True)
    parser.add_argument("--output-ifc", required=True)
    parser.add_argument("--selected-json", required=True)
    parser.add_argument("--audit-csv", required=True)
    args = parser.parse_args()
    try:
        result = run_worker(Path(args.source_ifc), Path(args.output_ifc), Path(args.selected_json), Path(args.audit_csv))
        print(json.dumps(result, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
