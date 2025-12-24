# === Colab-ready: IFC Storey BaseQuantities + Root/CRS Z adjust (with counter-shift) ===
# Works with Revit/Tekla and Civil 3D (CRS-aware). Adds BaseQuantities NetHeight/GrossHeight.
# Note: In CRS mode many viewers compute "global elevation" as (Local + CRS). If you want the
# storey's visible elevation to change in CRS mode, enable the checkbox in the UI below.

# If running in Colab, you can install deps with:
# !pip -q install ifcopenshell gradio

import os
import tempfile
import traceback
import ifcopenshell
from ifcopenshell.guid import new as new_guid
import gradio as gr

# =========================
# Helpers
# =========================

def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def sanitize_filename(base: str) -> str:
    # robust replacement of characters illegal in most OSes (includes backslash)
    for c in '<>:"/\\|?*':
        base = base.replace(c, "_")
    return base

_SI_PREFIX_TO_M = {None: 1.0, "MILLI": 1e-3, "CENTI": 1e-2, "DECI": 1e-1, "KILO": 1e3}

def model_length_unit_in_m(model) -> float:
    """Return the length of ONE model unit in meters."""
    try:
        projs = model.by_type("IfcProject")
        if not projs:
            return 1.0
        ua = getattr(projs[0], "UnitsInContext", None)
        if not ua:
            return 1.0
        for u in ua.Units or []:
            # IfcSIUnit LENGTHUNIT
            if u.is_a("IfcSIUnit") and getattr(u, "UnitType", None) == "LENGTHUNIT":
                if getattr(u, "Name", None) == "METRE":
                    pref = getattr(u, "Prefix", None)
                    return _SI_PREFIX_TO_M.get(pref, 1.0)
            # IfcConversionBasedUnit length
            if u.is_a("IfcConversionBasedUnit") and getattr(u, "UnitType", None) == "LENGTHUNIT":
                mu = getattr(u, "ConversionFactor", None)
                try:
                    v = getattr(mu, "ValueComponent", None)
                    if v is not None:
                        return float(v)
                except Exception:
                    pass
        return 1.0
    except Exception:
        return 1.0

def to_model_units_length(value, input_unit_code, model) -> float:
    """Convert from UI m/mm to the model's project length units."""
    if value in (None, ""):
        return None
    val_m = float(value) if input_unit_code == "m" else float(value) / 1000.0
    mu = model_length_unit_in_m(model)
    if mu <= 0:
        mu = 1.0
    return val_m / mu

def ui_to_meters(value, units_code) -> float:
    if value in (None, ""):
        return None
    return float(value) if units_code == "m" else float(value) / 1000.0

def meters_to_model_units(val_m, model) -> float:
    mu = model_length_unit_in_m(model)
    if mu <= 0:
        mu = 1.0
    return float(val_m) / mu

# =========================
# IFC helpers
# =========================

def get_first_owner_history(model):
    oh = model.by_type("IfcOwnerHistory")
    return oh[0] if oh else None

def find_storeys(model):
    rows = []
    for s in model.by_type("IfcBuildingStorey"):
        name = getattr(s, "Name", "") or ""
        elev = getattr(s, "Elevation", None)
        label = f"#{s.id()} • {name or '(no name)'} • Elev={elev if isinstance(elev,(int,float)) else '—'}"
        rows.append((s.id(), label, s, elev))
    rows.sort(key=lambda r: (r[3] if isinstance(r[3], (int, float)) else float('inf'), r[1]))
    return rows

# Quantities (BaseQuantities on storey)

def get_existing_elq(model, storey):
    for rel in model.by_type("IfcRelDefinesByProperties"):
        try:
            if storey in (rel.RelatedObjects or ()):
                elq = rel.RelatingPropertyDefinition
                if elq and elq.is_a("IfcElementQuantity") and elq.Name == "BaseQuantities":
                    return elq, rel
        except Exception:
            pass
    return None, None

def find_qtylength(elq, name):
    if not elq:
        return None
    for q in elq.Quantities or []:
        if q.is_a("IfcQuantityLength") and q.Name == name:
            return q
    return None

def ensure_qtylength(model, elq, name, value_model_units, description=None):
    q = find_qtylength(elq, name)
    if q:
        q.LengthValue = float(value_model_units)
        if description is not None:
            q.Description = description
        return q
    q = model.create_entity(
        "IfcQuantityLength",
        Name=name,
        Description=description if description else None,
        Unit=None,
        LengthValue=float(value_model_units),
    )
    elq.Quantities = tuple((elq.Quantities or ())) + (q,)
    return q

def create_or_update_storey_basequantities(
    model,
    storey,
    gross_val_ui=None,
    net_val_ui=None,
    input_unit_code="m",
    method_of_measurement=None,
    mirror_to_qto=False,
):
    owner_history = get_first_owner_history(model)
    elq, rel = get_existing_elq(model, storey)

    if elq is None:
        if model.schema.lower().startswith("ifc2x"):
            elq = model.create_entity(
                "IfcElementQuantity",
                GlobalId=new_guid(),
                OwnerHistory=owner_history,
                Name="BaseQuantities",
                Quantities=(),
            )
            rel = model.create_entity(
                "IfcRelDefinesByProperties",
                GlobalId=new_guid(),
                OwnerHistory=owner_history,
                RelatedObjects=(storey,),
                RelatingPropertyDefinition=elq,
            )
        else:
            elq = model.create_entity(
                "IfcElementQuantity",
                GlobalId=new_guid(),
                Name="BaseQuantities",
                Quantities=(),
            )
            rel = model.create_entity(
                "IfcRelDefinesByProperties",
                GlobalId=new_guid(),
                RelatedObjects=(storey,),
                RelatingPropertyDefinition=elq,
            )

    if gross_val_ui is not None:
        gross_model = to_model_units_length(gross_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "GrossHeight", gross_model)
    if net_val_ui is not None:
        net_model = to_model_units_length(net_val_ui, input_unit_code, model)
        ensure_qtylength(model, elq, "NetHeight", net_model)

    if mirror_to_qto:
        qto_elq = None
        for rel2 in model.by_type("IfcRelDefinesByProperties"):
            try:
                if storey in (rel2.RelatedObjects or (())):
                    elq2 = rel2.RelatingPropertyDefinition
                    if elq2 and elq2.is_a("IfcElementQuantity") and elq2.Name == "Qto_BuildingStoreyBaseQuantities":
                        qto_elq = elq2
                        break
            except Exception:
                pass
        if qto_elq is None:
            owner_history = get_first_owner_history(model)
            if model.schema.lower().startswith("ifc2x"):
                qto_elq = model.create_entity(
                    "IfcElementQuantity",
                    GlobalId=new_guid(),
                    OwnerHistory=owner_history,
                    Name="Qto_BuildingStoreyBaseQuantities",
                    Quantities=(),
                )
                model.create_entity(
                    "IfcRelDefinesByProperties",
                    GlobalId=new_guid(),
                    OwnerHistory=owner_history,
                    RelatedObjects=(storey,),
                    RelatingPropertyDefinition=qto_elq,
                )
            else:
                qto_elq = model.create_entity(
                    "IfcElementQuantity",
                    GlobalId=new_guid(),
                    Name="Qto_BuildingStoreyBaseQuantities",
                    Quantities=(),
                )
                model.create_entity(
                    "IfcRelDefinesByProperties",
                    GlobalId=new_guid(),
                    RelatedObjects=(storey,),
                    RelatingPropertyDefinition=qto_elq,
                )
        if gross_val_ui is not None:
            ensure_qtylength(model, qto_elq, "GrossHeight", to_model_units_length(gross_val_ui, input_unit_code, model))
        if net_val_ui is not None:
            ensure_qtylength(model, qto_elq, "NetHeight", to_model_units_length(net_val_ui, input_unit_code, model))

    return elq, rel

# Placement helpers

def ascend_to_root_local_placement(lp):
    cur = lp
    guard = 0
    while cur and getattr(cur, "PlacementRelTo", None) is not None and guard < 200:
        cur = cur.PlacementRelTo
        guard += 1
    return cur

def get_location_cartesian_point(lp):
    if not lp:
        return None
    rel = getattr(lp, "RelativePlacement", None)
    if not rel or not rel.is_a("IfcAxis2Placement3D"):
        return None
    loc = getattr(rel, "Location", None)
    if loc and loc.is_a("IfcCartesianPoint"):
        return loc
    return None

# CRS helpers

def get_all_map_conversions(model):
    # IfcMapConversion is not present in IFC2X3 schema
    if model.schema == 'IFC2X3':
        return []

    seen, out = set(), []
    # direct entities in the file
    for mc in model.by_type("IfcMapConversion") or []:
        try:
            if mc and mc.id() not in seen:
                out.append(mc)
                seen.add(mc.id())
        except Exception:
            pass
    # via contexts (HasCoordinateOperation may be a single op OR a tuple)
    for ctx in model.by_type("IfcGeometricRepresentationContext") or []:
        try:
            ops = getattr(ctx, "HasCoordinateOperation", None)
            if not ops:
                continue
            iterable = ops if isinstance(ops, (list, tuple)) else [ops]
            for op in iterable:
                if op and op.is_a("IfcMapConversion") and op.id() not in seen:
                    out.append(op)
                    seen.add(op.id())
        except Exception:
            pass
    return out

# Element counter-shift

def countershift_product_local_points(model, delta_model):
    """
    Counter-shift product local placements by -Δ (model units), but SAFELY:
    - Do NOT mutate shared IfcCartesianPoint objects in place (which can affect parent placements).
    - Instead, clone a new point per product and assign it to the product's Axis2Placement3D.Location.
    """
    c = 0
    for prod in model.by_type("IfcProduct"):
        # Skip high-level containers and spaces
        if prod.is_a("IfcProject") or prod.is_a("IfcSite") or prod.is_a("IfcBuilding") or prod.is_a("IfcBuildingStorey") or prod.is_a("IfcSpace"):
            continue

        lp = getattr(prod, "ObjectPlacement", None)
        if not lp:
            continue
        rel = getattr(lp, "RelativePlacement", None)
        if not (rel and rel.is_a("IfcAxis2Placement3D")):
            continue
        loc = getattr(rel, "Location", None)
        if not (loc and loc.is_a("IfcCartesianPoint")):
            continue

        coords = list(loc.Coordinates)
        if len(coords) >= 3 and coords[2] is not None:
            try:
                # CLONE the point instead of mutating it (avoid touching shared placement points)
                new_z = float(coords[2]) - float(delta_model)
                new_pt = model.create_entity(
                    "IfcCartesianPoint",
                    Coordinates=(
                        float(coords[0]) if coords[0] is not None else 0.0,
                        float(coords[1]) if coords[1] is not None else 0.0,
                        new_z,
                    ),
                )
                rel.Location = new_pt
                c += 1
            except Exception:
                # keep going if any odd entity shows up
                pass
    return c


# =========================
# Parse IFC (for UI)
# =========================

app_state = {"ifc_path": None, "ifc_fname": None}

def resolve_path(file_obj):
    if file_obj is None:
        return None
    if isinstance(file_obj, str):
        return file_obj
    p = getattr(file_obj, 'name', None)
    if p:
        return p
    if isinstance(file_obj, dict) and 'name' in file_obj:
        return file_obj['name']
    return None

def parse_ifc(file_obj):
    try:
        src_path = resolve_path(file_obj)
        if not src_path or not os.path.exists(src_path):
            return ("No file uploaded or path not found.", gr.update(choices=[], value=None))
        size = os.path.getsize(src_path)
        tmp_copy = tempfile.NamedTemporaryFile(delete=False, suffix=".ifc")
        with open(src_path, 'rb') as src, open(tmp_copy.name, 'wb') as dst:
            dst.write(src.read())
        model = ifcopenshell.open(tmp_copy.name)
        app_state['ifc_path'] = tmp_copy.name
        app_state['ifc_fname'] = os.path.basename(src_path)

        storeys = find_storeys(model)
        if not storeys:
            return ("Parsed IFC, but found no IfcBuildingStorey.", gr.update(choices=[], value=None))

        choices = [(lbl, sid) for (sid, lbl, _ent, _elev) in storeys]
        unit_m = model_length_unit_in_m(model)
        unit_label = "m" if abs(unit_m - 1.0) < 1e-12 else ("mm" if abs(unit_m - 1e-3) < 1e-12 else f"{unit_m} m/unit")
        mc_list = get_all_map_conversions(model)
        crs_note = f"; {len(mc_list)} IfcMapConversion found" if mc_list else ""
        status = f"Uploaded {sanitize_filename(app_state['ifc_fname'])} ({human_size(size)}) — found {len(choices)} storey level(s).\nModel length unit: {unit_label}{crs_note}"
        return (status, gr.update(choices=choices, value=choices[0][1]))
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Error parsing IFC: {e}\n\n```\n{tb}\n```"
        return (msg, gr.update(choices=[], value=None))

# =========================
# Apply (CRS-aware root Z + counter-shift + BaseQuantities)
# =========================

def apply_all(storey_id, units_code, gross_val, net_val, mom_txt, mirror, target_z, countershift_geometry, use_crs_mode, update_all_mcs, show_diag, crs_set_storey_elev):
    try:
        if app_state['ifc_path'] is None:
            return ("Please upload and parse an IFC first.", None)
        model = ifcopenshell.open(app_state['ifc_path'])
        storey = model.by_id(int(storey_id)) if storey_id else None
        if not storey:
            return ("Selected storey not found.", None)

        # 1) Quantities on storey
        gross_maybe = gross_val if gross_val not in (None, "") else None
        net_maybe   = net_val   if net_val   not in (None, "") else None
        if gross_maybe is not None or net_maybe is not None:
            create_or_update_storey_basequantities(
                model, storey,
                gross_val_ui=gross_maybe,
                net_val_ui=net_maybe,
                input_unit_code=units_code,
                method_of_measurement=(mom_txt or None),
                mirror_to_qto=bool(mirror)
            )

        # 2) CRS adjust OR root placement adjust (compute Δ before writing)
        delta_model = 0.0
        used_path = "root-local"
        diag_lines = []

        mc_list = []
        if use_crs_mode and target_z not in (None, ""):
            all_mcs = get_all_map_conversions(model)
            if all_mcs:
                mc_list = all_mcs if update_all_mcs else [all_mcs[0]]

        if mc_list:
            # CRS path: OrthogonalHeight is in meters (CRS units)
            new_m = ui_to_meters(target_z, units_code)
            old_m_first = float(getattr(mc_list[0], "OrthogonalHeight", 0.0) or 0.0)
            delta_m = new_m - old_m_first  # compute BEFORE writing
            for idx, mc in enumerate(mc_list):
                old_m = float(getattr(mc, "OrthogonalHeight", 0.0) or 0.0)
                mc.OrthogonalHeight = float(new_m)
                if show_diag:
                    diag_lines.append(f"CRS[{idx}] {old_m} m → {new_m} m (Δ={new_m-old_m} m)")
            # Optionally also set the storey's Elevation ABSOLUTELY to the target (in model units)
            if crs_set_storey_elev:
                target_mu = to_model_units_length(target_z, units_code, model)
                old_storey_elev = float(getattr(storey, "Elevation", 0.0) or 0.0)
                delta_model = meters_to_model_units(delta_m, model)
                storey.Elevation = float(target_mu)  # absolute, not old + Δ
                if show_diag:
                    diag_lines.append(f"Storey.Elevation (CRS mode ABS) {old_storey_elev} mu → {storey.Elevation} mu (target_mu={target_mu} mu, Δ={delta_model} mu)")
            else:
                # Keep storey.Elevation unchanged in CRS mode
                delta_model = meters_to_model_units(delta_m, model)
                # Keep storey.Elevation unchanged in CRS mode
                delta_model = meters_to_model_units(delta_m, model)
            used_path = "crs-mapconversion(all)" if (update_all_mcs and len(mc_list) > 1) else "crs-mapconversion"
        else:
            if target_z not in (None, ""):
                root_lp = ascend_to_root_local_placement(storey.ObjectPlacement)
                root_pt = get_location_cartesian_point(root_lp)
                if root_pt is None:
                    return ("Could not find root CartesianPoint for the storey's placement.", None)
                coords = list(root_pt.Coordinates)
                if len(coords) < 3:
                    return ("Root CartesianPoint has no Z coordinate.", None)
                old_z = float(coords[2]) if coords[2] is not None else 0.0
                new_z = to_model_units_length(target_z, units_code, model)
                delta_model = new_z - old_z
                coords[2] = new_z
                root_pt.Coordinates = tuple(coords)
                # In root-local mode, set storey elevation directly (model units)
                old_storey_elev = float(getattr(storey, "Elevation", 0.0) or 0.0)
                storey.Elevation = float(new_z)
                if show_diag:
                    diag_lines.append(f"RootLP {old_z} mu → {new_z} mu (Δ={delta_model} mu)")
                    diag_lines.append(f"Storey.Elevation (Root mode) {old_storey_elev} mu → {storey.Elevation} mu")
                used_path = "root-local"

        # 3) Counter-shift element placements by −Δ (model units)
        shifted = 0
        if countershift_geometry and abs(delta_model) > 0:
            shifted = countershift_product_local_points(model, delta_model)

        # 4) Save
        base = os.path.splitext(sanitize_filename(app_state['ifc_fname'] or 'model.ifc'))[0]
        suffix = "_gsb_adjusted_crs" if used_path.startswith("crs-mapconversion") else "_gsb_adjusted"
        out_path = os.path.join(tempfile.gettempdir(), f"{base}{suffix}.ifc")
        model.write(out_path)

        # 5) UI summary
        mu_m = model_length_unit_in_m(model)
        mu_label = "m" if abs(mu_m - 1.0) < 1e-12 else ("mm" if abs(mu_m - 1e-3) < 1e-12 else f"{mu_m} m/unit")
        parts = [
            "Done ✅",
            f"Schema: {model.schema}",
            f"Model length unit: {mu_label}",
            f"Mode: {'CRS (IfcMapConversion)' if used_path.startswith('crs-mapconversion') else 'Root LocalPlacement'}",
            f"Target Z = {target_z if target_z not in (None,'') else ''} {units_code}",
            f"Δ applied (model units) = {delta_model}",
            (f"Counter-shifted {shifted} product placements by −Δ (kept world positions)." if shifted else None)
        ]
        # Extra diagnostics for Civil 3D
        try:
            site = (model.by_type('IfcSite') or [None])[0]
            site_ref = float(getattr(site, 'RefElevation', 0.0) or 0.0) if site else 0.0
            parts.append(f"Site.RefElevation = {site_ref} mu")
            parts.append(f"Storey.Elevation = {float(getattr(storey,'Elevation',0.0) or 0.0)} mu")
            mcs = get_all_map_conversions(model)
            if mcs:
                parts.append(f"MapConversion.OrthogonalHeight = {float(getattr(mcs[0],'OrthogonalHeight',0.0) or 0.0)} m")
        except Exception:
            pass
        if show_diag and diag_lines:
            parts.append("Diagnostics:")
            parts.extend([" • " + d for d in diag_lines])
        if gross_maybe is not None:
            parts.append(f" • GrossHeight (UI {units_code}) = {gross_maybe}")
        if net_maybe is not None:
            parts.append(f" • NetHeight  (UI {units_code}) = {net_maybe}")
        if mom_txt:
            parts.append(f" • MethodOfMeasurement = '{mom_txt}'")
        parts.append(f"Output: {os.path.basename(out_path)}")
        return ("\n".join([p for p in parts if p]), out_path)

    except Exception as e:
        tb = traceback.format_exc()
        return (f"Error while applying changes: {e}\n\n```\n{tb}\n```", None)


# =========================
# Gradio UI
# =========================

with gr.Blocks(title="IFC Storey BaseQuantities + Root/CRS GSB adjust (Solibri-friendly)") as demo:
    gr.Markdown("## Add/Update BaseQuantities + Set root/global bottom Z (root or CRS) and counter-shift element placements (no geometry move)")
    with gr.Row():
        ifc_file = gr.File(label="IFC file", file_types=[".ifc", ".IFC"], type="filepath")
        parse_btn = gr.Button("Parse IFC", variant="primary")
    status = gr.Markdown("_No file uploaded._")
    storey_dd = gr.Dropdown(label="Storey", choices=[], value=None)
    with gr.Row():
        units = gr.Dropdown(choices=[("meters (m)","m"),("millimeters (mm)","mm")], value="m", label="Units")
        gross = gr.Number(value=None, label="GrossHeight")
        net   = gr.Number(value=None, label="NetHeight")
    mom = gr.Textbox(value="", label="MoM (optional)")
    mirror_chk = gr.Checkbox(value=False, label="Also mirror values to Qto_BuildingStoreyBaseQuantities (optional)")

    gr.Markdown("### Global Bottom / Root Z")
    with gr.Row():
        target_z = gr.Number(value=None, label="Set TARGET Z (e.g., 328.8 for m or 328800 for mm)")
        countershift_geometry = gr.Checkbox(value=True, label="Counter-shift element placements to keep world positions")
    with gr.Row():
        use_crs_mode   = gr.Checkbox(value=True, label="CRS-aware: adjust IfcMapConversion.OrthogonalHeight when present")
        update_all_mcs = gr.Checkbox(value=True, label="When CRS-aware: update ALL MapConversions found")
        crs_set_storey_elev = gr.Checkbox(value=True, label="CRS mode: also set Storey.Elevation by Δ (makes viewers show target)")
        show_diag      = gr.Checkbox(value=True, label="Show diagnostics (units, CRS heights, Δ)")

    apply_btn = gr.Button("Apply & Save", variant="primary")
    result = gr.Markdown(visible=True)
    download = gr.File(label="Download IFC", interactive=False)

    parse_btn.click(fn=parse_ifc, inputs=[ifc_file], outputs=[status, storey_dd])
    apply_btn.click(
        fn=apply_all,
        inputs=[storey_dd, units, gross, net, mom, mirror_chk, target_z, countershift_geometry, use_crs_mode, update_all_mcs, show_diag, crs_set_storey_elev],
        outputs=[result, download],
    )

# Launch
if 'google.colab' in str(getattr(__import__('sys'), 'modules', {})):
    demo.launch(debug=True, show_api=False)
else:
    demo.launch(debug=True, server_name='0.0.0.0', server_port=7860)