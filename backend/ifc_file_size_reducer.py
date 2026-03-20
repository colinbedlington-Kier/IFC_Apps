from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ifcopenshell

try:  # pragma: no cover - runtime dependency availability
    import ifcpatch
except Exception:  # pragma: no cover
    ifcpatch = None


class IfcFileSizeReducerError(Exception):
    pass


def _count_types(model: ifcopenshell.file, type_names: List[str]) -> int:
    total = 0
    for type_name in type_names:
        total += len(model.by_type(type_name))
    return total


def _materialize_ifc_input(source_path: Path, working_dir: Path) -> Tuple[Path, Dict[str, Any]]:
    lower = source_path.name.lower()
    meta: Dict[str, Any] = {"input_is_ifczip": lower.endswith(".ifczip")}
    if lower.endswith(".ifc"):
        temp_ifc = working_dir / f"{source_path.stem}.ifc"
        shutil.copy2(source_path, temp_ifc)
        meta["unpacked_from_ifczip"] = False
        return temp_ifc, meta
    if lower.endswith(".ifczip"):
        with zipfile.ZipFile(source_path, "r") as zf:
            candidates = [name for name in zf.namelist() if name.lower().endswith(".ifc")]
            if not candidates:
                raise IfcFileSizeReducerError("IFCZIP does not contain an IFC file")
            member = candidates[0]
            extracted = working_dir / Path(member).name
            with zf.open(member, "r") as src, open(extracted, "wb") as dst:
                shutil.copyfileobj(src, dst)
        meta["unpacked_from_ifczip"] = True
        meta["ifczip_member"] = member
        return extracted, meta
    raise IfcFileSizeReducerError("Input must be .ifc or .ifczip")


def analyze_ifc_file(source_path: Path) -> Dict[str, Any]:
    if not source_path.exists() or not source_path.is_file():
        raise IfcFileSizeReducerError("Selected source file does not exist")

    with tempfile.TemporaryDirectory(prefix="ifc_reduce_analyse_") as tmpdir:
        ifc_path, input_meta = _materialize_ifc_input(source_path, Path(tmpdir))
        model = ifcopenshell.open(str(ifc_path))

        products = model.by_type("IfcProduct")
        sites = model.by_type("IfcSite")
        site_has_representation = any(getattr(site, "Representation", None) is not None for site in sites)

        counts = {
            "IfcPropertySet": len(model.by_type("IfcPropertySet")),
            "IfcElementQuantity": len(model.by_type("IfcElementQuantity")),
            "IfcRelDefinesByProperties": len(model.by_type("IfcRelDefinesByProperties")),
            "IfcStyledItem": len(model.by_type("IfcStyledItem")),
            "IfcPresentationLayerAssignment": len(model.by_type("IfcPresentationLayerAssignment")),
            "IfcTypeProduct": len(model.by_type("IfcTypeProduct")),
            "IfcMaterial*": _count_types(
                model,
                [
                    "IfcMaterial",
                    "IfcMaterialLayer",
                    "IfcMaterialLayerSet",
                    "IfcMaterialLayerSetUsage",
                    "IfcMaterialConstituent",
                    "IfcMaterialConstituentSet",
                    "IfcMaterialProfile",
                    "IfcMaterialProfileSet",
                    "IfcMaterialProfileSetUsage",
                    "IfcMaterialList",
                ],
            ),
        }

        style_density = counts["IfcStyledItem"] / max(1, len(products))
        pset_density = (counts["IfcPropertySet"] + counts["IfcElementQuantity"]) / max(1, len(products))
        representation_heavy = style_density > 2 or counts["IfcPresentationLayerAssignment"] > len(products)
        recommended_mode = "conservative_viewer_copy"
        recommendation_reason = "Balanced reduction for coordination/viewing copies."

        if source_path.suffix.lower() == ".ifc":
            recommended_mode = "compress_only"
            recommendation_reason = "Safest transfer/storage reduction is IFCZIP packaging without semantic changes."
        if site_has_representation and source_path.stat().st_size > 50 * 1024 * 1024:
            recommended_mode = "conservative_viewer_copy"
            recommendation_reason = "Large file with site representation: conservative removal of site representation may help."
        if pset_density > 4:
            recommendation_reason += " Aggressive mode can reduce metadata-heavy files but is destructive."

        entity_count = sum(1 for _ in model)
        return {
            "source_file": source_path.name,
            "schema": model.schema,
            "current_file_size_bytes": source_path.stat().st_size,
            "entity_count": entity_count,
            "product_count": len(products),
            "representation_heavy_indicators": {
                "style_density_per_product": round(style_density, 3),
                "presentation_layers": counts["IfcPresentationLayerAssignment"],
                "representation_heavy": representation_heavy,
            },
            "site": {
                "ifcsite_count": len(sites),
                "has_representation": site_has_representation,
            },
            "rough_counts": counts,
            "recommendation": {
                "default_mode": recommended_mode,
                "reason": recommendation_reason,
                "safe_first_option": "compress_only",
                "aggressive_optional": pset_density > 2 or style_density > 2,
            },
            "input_meta": input_meta,
        }


def run_ifcpatch_recipe(input_path: Path, output_path: Path, recipe: str, arguments: Optional[List[Any]] = None) -> None:
    if ifcpatch is None:
        raise IfcFileSizeReducerError("ifcpatch is unavailable in this environment")
    payload = {
        "input": str(input_path),
        "output": str(output_path),
        "recipe": recipe,
        "arguments": arguments or [],
    }
    ifcpatch.execute(payload)


def _safe_name(prefix: str, stem: str, suffix: str) -> str:
    clean_prefix = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in (prefix or "reduced"))
    return f"{clean_prefix}_{stem}{suffix}"


def run_reduction(session_root: Path, source_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    source_path = session_root / os.path.basename(source_name)
    if not source_path.exists():
        raise IfcFileSizeReducerError("Selected source file not found in session")

    mode = str(payload.get("mode") or "conservative_viewer_copy")
    options = {
        "export_ifczip": bool(payload.get("export_ifczip", False)),
        "remove_site_representation": bool(payload.get("remove_site_representation", False)),
        "purge_data": bool(payload.get("purge_data", False)),
        "optimise_model": bool(payload.get("optimise_model", False)),
        "split_by_storey": bool(payload.get("split_by_storey", False)),
        "warning_acknowledged": bool(payload.get("warning_acknowledged", False)),
    }

    if mode == "aggressive_viewer_copy" and (options["purge_data"] or options["optimise_model"]) and not options["warning_acknowledged"]:
        raise IfcFileSizeReducerError("Aggressive options require warning acknowledgment")
    if mode != "aggressive_viewer_copy":
        options["purge_data"] = False
        options["optimise_model"] = False

    prefix = str(payload.get("output_prefix") or "reduced")
    log: List[Dict[str, Any]] = []
    output_files: List[Dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="ifc_reduce_run_") as tmpdir:
        tmp = Path(tmpdir)
        working_ifc, input_meta = _materialize_ifc_input(source_path, tmp)
        baseline_model = ifcopenshell.open(str(working_ifc))
        product_count_before = len(baseline_model.by_type("IfcProduct"))
        schema = baseline_model.schema

        current_path = tmp / "working.ifc"
        shutil.copy2(working_ifc, current_path)

        try:
            if options["remove_site_representation"]:
                next_path = tmp / "step_remove_site.ifc"
                run_ifcpatch_recipe(current_path, next_path, "RemoveSiteRepresentation")
                current_path = next_path
                log.append({"step": "RemoveSiteRepresentation", "status": "ok"})

            if options["purge_data"]:
                next_path = tmp / "step_purge_data.ifc"
                run_ifcpatch_recipe(current_path, next_path, "PurgeData")
                current_path = next_path
                log.append({"step": "PurgeData", "status": "ok", "warning": "destructive"})

            if options["optimise_model"]:
                next_path = tmp / "step_optimise.ifc"
                run_ifcpatch_recipe(current_path, next_path, "Optimise")
                current_path = next_path
                log.append({"step": "Optimise", "status": "ok", "warning": "lossy_and_slow"})

            if options["split_by_storey"]:
                split_dir = tmp / "split_storeys"
                split_dir.mkdir(parents=True, exist_ok=True)
                run_ifcpatch_recipe(current_path, split_dir / "manifest.ifc", "SplitByBuildingStorey")
                produced = sorted(split_dir.glob("*.ifc"))
                if not produced:
                    raise IfcFileSizeReducerError("Split by storey did not produce any IFC files")
                for idx, piece in enumerate(produced, start=1):
                    target_name = _safe_name(prefix, f"{source_path.stem}_storey_{idx}", ".ifc")
                    target = session_root / target_name
                    shutil.copy2(piece, target)
                    output_files.append({"name": target_name, "size": target.stat().st_size})
                manifest_name = _safe_name(prefix, f"{source_path.stem}_storey_manifest", ".json")
                manifest_path = session_root / manifest_name
                manifest_payload = {
                    "source": source_path.name,
                    "parts": [entry["name"] for entry in output_files],
                }
                manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
                output_files.append({"name": manifest_name, "size": manifest_path.stat().st_size})
                log.append({"step": "SplitByBuildingStorey", "status": "ok", "parts": len(produced)})
            else:
                reduced_ifc_name = _safe_name(prefix, source_path.stem, ".ifc")
                reduced_ifc_path = session_root / reduced_ifc_name
                shutil.copy2(current_path, reduced_ifc_path)
                output_files.append({"name": reduced_ifc_name, "size": reduced_ifc_path.stat().st_size})
                if options["export_ifczip"]:
                    zip_name = _safe_name(prefix, source_path.stem, ".ifczip")
                    zip_path = session_root / zip_name
                    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(reduced_ifc_path, arcname=f"{source_path.stem}.ifc")
                    output_files.append({"name": zip_name, "size": zip_path.stat().st_size})

            reduced_ifc_for_validation = next((session_root / item["name"] for item in output_files if item["name"].lower().endswith(".ifc")), None)
            warnings: List[str] = []
            reopened_successfully = False
            product_count_after = product_count_before
            if reduced_ifc_for_validation and reduced_ifc_for_validation.exists():
                try:
                    reopened = ifcopenshell.open(str(reduced_ifc_for_validation))
                    reopened_successfully = True
                    product_count_after = len(reopened.by_type("IfcProduct"))
                except Exception as exc:
                    warnings.append(f"Post-write validation failed: {exc}")
            else:
                warnings.append("No IFC output available for post-write validation")

            original_size = source_path.stat().st_size
            reduced_size = sum(item["size"] for item in output_files if item["name"].lower().endswith((".ifc", ".ifczip")))
            bytes_saved = original_size - reduced_size
            percent_reduction = round((bytes_saved / original_size) * 100.0, 2) if original_size else 0.0

            result = {
                "source_file": source_path.name,
                "output_files": output_files,
                "original_size_bytes": original_size,
                "reduced_size_bytes": reduced_size,
                "bytes_saved": bytes_saved,
                "percent_reduction": percent_reduction,
                "schema": schema,
                "mode_used": mode,
                "selected_options": options,
                "validation": {
                    "reopened_successfully": reopened_successfully,
                    "product_count_before": product_count_before,
                    "product_count_after": product_count_after,
                    "warnings": warnings,
                },
                "log": log,
                "input_meta": input_meta,
            }

            summary_name = _safe_name(prefix, f"{source_path.stem}_summary", ".json")
            summary_path = session_root / summary_name
            summary_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
            result["summary_json_path"] = summary_name
            if warnings:
                log.append({"step": "validation", "status": "warning", "warnings": warnings})
            return result
        except Exception as exc:
            log.append({"step": "error", "status": "failed", "detail": str(exc)})
            partial = {
                "source_file": source_path.name,
                "output_files": output_files,
                "schema": schema,
                "mode_used": mode,
                "selected_options": options,
                "validation": {
                    "reopened_successfully": False,
                    "product_count_before": product_count_before,
                    "product_count_after": product_count_before,
                    "warnings": [str(exc)],
                },
                "log": log,
                "input_meta": input_meta,
            }
            raise IfcFileSizeReducerError(json.dumps(partial)) from exc
