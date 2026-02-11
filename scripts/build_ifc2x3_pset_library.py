#!/usr/bin/env python3
"""Build IFC2X3 pset applicability lookup for type-token matching."""

from __future__ import annotations

import json
from pathlib import Path

import ifcopenshell.util.pset as pset_util


OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "ifc2x3_pset_applicability.json"


def main() -> None:
    pset_template = pset_util.get_template("IFC2X3")
    records = []

    for template_file in pset_template.templates:
        for pset in template_file.by_type("IfcPropertySetTemplate"):
            pset_name = getattr(pset, "Name", "") or ""
            applicable = getattr(pset, "ApplicableEntity", "") or ""
            if not pset_name.startswith("Pset_"):
                continue
            if not applicable.startswith("Ifc") or not applicable.endswith("Type"):
                continue

            class_stem = applicable[3:]  # remove Ifc
            pset_stem = pset_name[len("Pset_") :]
            if not pset_stem.startswith(class_stem):
                continue

            applicable_value = pset_stem[len(class_stem) :].strip()
            if not applicable_value:
                continue

            records.append(
                {
                    "pset_name": pset_name,
                    "ifc_class": applicable,
                    "applicable_type_value": applicable_value,
                }
            )

    records.sort(key=lambda r: (r["ifc_class"], r["applicable_type_value"], r["pset_name"]))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"Wrote {len(records)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
