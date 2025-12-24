import os
import datetime
import traceback
from typing import Dict, Any, List, Tuple

import ifcopenshell
import gradio as gr


# -----------------------------
# Core cleaning logic (unchanged)
# -----------------------------
def clean_ifc_file(
    in_path: str,
    out_path: str,
    prefix: str = "InfoDrainage",
    case_insensitive: bool = True,
    delete_psets_with_prefix: bool = True,
    delete_properties_in_other_psets: bool = True,
    drop_empty_psets: bool = True,
    also_remove_loose_props: bool = True,
) -> Dict[str, Any]:
    """
    Open IFC, remove property sets and/or properties whose Name starts with `prefix`,
    then save to out_path. Returns a report dict.
    """
    def starts_with(s: str) -> bool:
        if s is None:
            return False
        if case_insensitive:
            return s.lower().startswith(prefix.lower())
        return s.startswith(prefix)

    report = {
        "input": os.path.basename(in_path),
        "output": os.path.basename(out_path),
        "prefix": prefix,
        "case_insensitive": case_insensitive,
        "removed": {
            "IfcPropertySet": 0,
            "IfcRelDefinesByProperties": 0,
            "emptied_psets": 0,
            "IfcPropertySingleValue": 0,
            "IfcComplexProperty": 0,
            "IfcPropertyEnumeratedValue": 0,
            "IfcPropertyReferenceValue": 0,
            "IfcPropertyListValue": 0,
            "IfcPropertyTableValue": 0,
            "loose_properties": 0,
        },
        "notes": [],
        "status": "started",
    }

    f = ifcopenshell.open(in_path)

    # 1) Identify Psets to delete
    psets_to_delete = set()
    if delete_psets_with_prefix:
        for pset in f.by_type("IfcPropertySet"):
            try:
                if starts_with(getattr(pset, "Name", None)):
                    psets_to_delete.add(pset)
            except Exception:
                pass

    # 2) In other psets, remove matching properties
    prop_types = [
        "IfcPropertySingleValue",
        "IfcComplexProperty",
        "IfcPropertyEnumeratedValue",
        "IfcPropertyReferenceValue",
        "IfcPropertyListValue",
        "IfcPropertyTableValue",
    ]
    prop_removed_count = {t: 0 for t in prop_types}
    emptied_pset_count = 0

    if delete_properties_in_other_psets:
        for pset in f.by_type("IfcPropertySet"):
            try:
                # Skip psets already flagged for deletion
                if pset in psets_to_delete:
                    continue

                props = list(getattr(pset, "HasProperties", []) or [])
                if not props:
                    # Consider drop_empty_psets handling below
                    continue

                to_keep = []
                for p in props:
                    nm = getattr(p, "Name", None)
                    if nm and starts_with(nm):
                        # remove the property entity
                        try:
                            kind = p.is_a()
                        except Exception:
                            kind = None
                        try:
                            f.remove(p)
                            if kind in prop_removed_count:
                                prop_removed_count[kind] += 1
                        except Exception:
                            # If removal fails, just don't keep it linked
                            if kind in prop_removed_count:
                                prop_removed_count[kind] += 1
                    else:
                        to_keep.append(p)

                # Update links on the pset to reflect removals
                try:
                    pset.HasProperties = tuple(to_keep)
                except Exception:
                    # some backends might not allow direct assignment; best effort
                    pass

                # Mark empty psets for deletion if requested
                if drop_empty_psets and len(getattr(pset, "HasProperties", []) or []) == 0:
                    psets_to_delete.add(pset)
                    emptied_pset_count += 1
            except Exception:
                pass

    # 3) Delete relations pointing to target psets, then delete the psets
    rel_del_count = 0
    pset_del_count = 0

    for pset in list(psets_to_delete):
        inverses = []
        try:
            inverses = f.get_inverse(pset)
        except Exception:
            pass

        for rel in list(inverses or []):
            try:
                if rel.is_a("IfcRelDefinesByProperties") and rel.RelatingPropertyDefinition == pset:
                    f.remove(rel)
                    rel_del_count += 1
            except Exception:
                pass

        try:
            f.remove(pset)
            pset_del_count += 1
        except Exception:
            pass

    # 4) Optionally remove any loose properties named with prefix (rare, but possible)
    loose_removed = 0
    if also_remove_loose_props:
        for t in prop_types:
            for p in list(f.by_type(t)):
                nm = getattr(p, "Name", None)
                if nm and starts_with(nm):
                    try:
                        f.remove(p)
                        prop_removed_count[t] += 1
                        loose_removed += 1
                    except Exception:
                        pass

    # 5) Save
    try:
        f.write(out_path)
        status = "success"
    except Exception as e:
        status = "save_failed"
        report["notes"].append(f"Save error: {e!r}")

    # Fill report
    report["removed"]["IfcPropertySet"] = pset_del_count
    report["removed"]["IfcRelDefinesByProperties"] = rel_del_count
    report["removed"]["emptied_psets"] = emptied_pset_count
    report["removed"]["loose_properties"] = loose_removed
    for t, c in prop_removed_count.items():
        report["removed"][t] = c

    report["status"] = status
    return report


# -----------------------------
# Gradio interface logic
# -----------------------------
def clean_ifc_files_interface(
    files: List[gr.File],
    prefix: str,
    case_insensitive: bool,
    delete_psets_with_prefix: bool,
    delete_properties_in_other_psets: bool,
    drop_empty_psets: bool,
    also_remove_loose_props: bool,
) -> Tuple[str, List[str]]:
    """
    Wrapper called by Gradio. Takes uploaded files and options,
    returns: (report_text, list_of_output_paths)
    """
    if not files:
        return "Please upload at least one IFC file.", []

    prefix = prefix.strip() or "InfoDrainage"

    report_lines: List[str] = []
    output_paths: List[str] = []

    for fobj in files:
        # In Gradio, fobj.name is a path to the temp file on disk
        in_path = fobj.name
        orig_name = os.path.basename(in_path)

        base, _ext = os.path.splitext(orig_name)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"{base}_cleaned_{ts}.ifc"
        out_path = os.path.join(os.path.dirname(in_path), out_name)

        try:
            report = clean_ifc_file(
                in_path=in_path,
                out_path=out_path,
                prefix=prefix,
                case_insensitive=case_insensitive,
                delete_psets_with_prefix=delete_psets_with_prefix,
                delete_properties_in_other_psets=delete_properties_in_other_psets,
                drop_empty_psets=drop_empty_psets,
                also_remove_loose_props=also_remove_loose_props,
            )
        except Exception as e:
            report = {
                "input": orig_name,
                "output": None,
                "status": "error",
                "removed": {
                    "IfcPropertySet": 0,
                    "IfcRelDefinesByProperties": 0,
                    "emptied_psets": 0,
                    "IfcPropertySingleValue": 0,
                    "IfcComplexProperty": 0,
                    "IfcPropertyEnumeratedValue": 0,
                    "IfcPropertyReferenceValue": 0,
                    "IfcPropertyListValue": 0,
                    "IfcPropertyTableValue": 0,
                    "loose_properties": 0,
                },
                "notes": [repr(e), traceback.format_exc()],
            }

        # Build human-readable report text
        report_lines.append(
            f"=== {report['input']} -> {report.get('output') or '(no file)'} ==="
        )
        report_lines.append(f"Status: {report['status']}")
        if "prefix" in report:
            report_lines.append(f"Prefix: {report.get('prefix')}")
        if "case_insensitive" in report:
            report_lines.append(f"Case-insensitive: {report.get('case_insensitive')}")
        report_lines.append("Removed counts:")
        for k, v in report["removed"].items():
            report_lines.append(f"  - {k}: {v}")
        if report.get("notes"):
            report_lines.append("Notes:")
            for n in report["notes"]:
                report_lines.append(f"  * {n}")
        report_lines.append("")

        if report["status"] == "success":
            output_paths.append(out_path)

    return "\n".join(report_lines), output_paths


with gr.Blocks(title="IFC Cleaner") as demo:
    gr.Markdown(
        """
        # IFC Cleaner

        Upload one or more IFC files and remove property sets / properties
        whose names start with a given prefix (default: `InfoDrainage`).
        """
    )

    with gr.Row():
        with gr.Column(scale=1):
            files_input = gr.Files(label="Upload IFC file(s)", file_types=[".ifc"])
            prefix_txt = gr.Textbox(
                value="InfoDrainage",
                label="Prefix",
                info="Property set / property names starting with this will be removed.",
            )
            case_box = gr.Checkbox(
                value=True,
                label="Case-insensitive match",
            )
            pset_box = gr.Checkbox(
                value=True,
                label="Delete Psets with prefix",
            )
            prop_box = gr.Checkbox(
                value=True,
                label="Delete matching Props in other Psets",
            )
            drop_box = gr.Checkbox(
                value=True,
                label="Drop Psets that become empty",
            )
            loose_box = gr.Checkbox(
                value=True,
                label="Also remove loose properties",
            )

            run_btn = gr.Button("Clean IFC(s)")

        with gr.Column(scale=1):
            report_output = gr.Textbox(
                label="Cleaning report",
                lines=20,
            )
            cleaned_files_output = gr.Files(
                label="Download cleaned IFC file(s)",
            )

    run_btn.click(
        fn=clean_ifc_files_interface,
        inputs=[
            files_input,
            prefix_txt,
            case_box,
            pset_box,
            prop_box,
            drop_box,
            loose_box,
        ],
        outputs=[report_output, cleaned_files_output],
    )

if __name__ == "__main__":
    demo.launch()
