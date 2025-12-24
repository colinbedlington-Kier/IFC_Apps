import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element
import pandas as pd
import gradio as gr
import tempfile
import os
import re

# --- Hardcoded COBie Mapping ---
COBIE_MAPPING = {
    'COBie_Specification': {'scope': 'T', 'props': [
        ('NominalLength', 'Length'),
        ('NominalWidth', 'Length'),
        ('NominalHeight', 'Length'),
        ('Shape', 'Text'),
        ('Size', 'Text'),
        ('Color', 'Text'),
        ('Finish', 'Text'),
        ('Grade', 'Text'),
        ('Material', 'Text'),
        ('Constituents', 'Text'),
        ('Features', 'Text'),
        ('AccessibilityPerformance', 'Text'),
        ('CodePerformance', 'Text'),
        ('SustainabilityPerformance', 'Text'),
    ]},
    'COBie_Component': {'scope': 'I', 'props': [
        ('COBie', 'Boolean'),
        ('InstallationDate', 'Text'),
        ('WarrantyStartDate', 'Text'),
        ('TagNumber', 'Text'),
        ('AssetIdentifier', 'Text'),
        ('Space', 'Text'),
        ('CreatedBy', 'Text'),
        ('CreatedOn', 'Text'),
        ('Name', 'Text'),
        ('Description', 'Text'),
        ('Area', 'Area'),
        ('Length', 'Length'),
    ]},
    'COBie_Asset': {'scope': 'T', 'props': [
        ('AssetType', 'Text'),
    ]},
    'COBie_Warranty': {'scope': 'T', 'props': [
        ('WarrantyDurationParts', 'Real'),
        ('WarrantyGuarantorLabor', 'Text'),
        ('WarrantyDurationLabor', 'Real'),
        ('WarrantyDurationDescription', 'Text'),
        ('WarrantyDurationUnit', 'Text'),
        ('WarrantyGuarantorParts', 'Text'),
    ]},
    'Pset_ManufacturerOccurence': {'scope': 'I', 'props': [
        ('SerialNumber', 'Text'),
        ('BarCode', 'Text'),
    ]},
    'COBie_ServiceLife': {'scope': 'T', 'props': [
        ('ServiceLifeDuration', 'Real'),
        ('DurationUnit', 'Text'),
    ]},
    'COBie_EconomicalImpactValues': {'scope': 'T', 'props': [
        ('ReplacementCost', 'Real'),
    ]},
    'COBie_Type': {'scope': 'T', 'props': [
        ('COBie', 'Boolean'),
        ('CreatedBy', 'Text'),
        ('CreatedOn', 'Text'),
        ('Name', 'Text'),
        ('Description', 'Text'),
        ('Category', 'Text'),
        ('Area', 'Area'),
        ('Length', 'Length'),
    ]},
    'COBie_System': {'scope': 'I', 'props': [
        ('Name', 'Text'),
        ('Description', 'Text'),
        ('Category', 'Text'),
    ]},
    'Classification_General': {'scope': 'T', 'props': [
        ('Classification.Uniclass.Pr.Number', 'Text'),
        ('Classification.Uniclass.Pr.Description', 'Text'),
        ('Classification.Uniclass.Ss.Number', 'Text'),
        ('Classification.Uniclass.Ss.Description', 'Text'),
        ('Classification.NRM1.Number', 'Text'),
        ('Classification.NRM1.Description', 'Text'),
    ]},
    'Pset_ManufacturerTypeInformation': {'scope': 'T', 'props': [
        ('Manufacturer', 'Text'),
        ('ModelNumber', 'Text'),
        ('ModelReference', 'Text'),
    ]},
    'PPset_DoorCommon': {'scope': 'T', 'props': [
        ('FireRating', 'Text'),
    ]},
    'Pset_BuildingCommon': {'scope': 'T', 'props': [
        ('NumberOfStoreys', 'Text'),
    ]},
    'COBie_Space': {'scope': 'T', 'props': [
        ('RoomTag', 'Text'),
    ]},
    'COBie_BuildingCommon_UK': {'scope': 'T', 'props': [
        ('UPRN', 'Text'),
    ]},
    'Additional_Pset_BuildingCommon': {'scope': 'T', 'props': [
        ('BlockConstructionType', 'Text'),
        ('MaximumBlockHeight', 'Text'),
    ]},
    'Additional_Pset_SystemCommon': {'scope': 'T', 'props': [
        ('SystemCategory', 'Text'),
        ('SystemDescription', 'Text'),
        ('SystemName', 'Text'),
    ]},
}

# --- Utility ---

def path_of(f):
    return f if isinstance(f, str) else getattr(f, "name", f)

def clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
    return v

# --- IFC structure helpers ---

def ensure_aggregates(parent, child, ifc):
    """Ensure `child` is aggregated under `parent` (IfcRelAggregates)."""
    rel = None
    for r in parent.IsDecomposedBy or []:
        if r.is_a("IfcRelAggregates"):
            rel = r
            break
    if rel is None:
        ifc.create_entity(
            "IfcRelAggregates",
            GlobalId=ifcopenshell.guid.new(),
            RelatingObject=parent,
            RelatedObjects=[child],
        )
    else:
        if child not in rel.RelatedObjects:
            rel.RelatedObjects = list(rel.RelatedObjects) + [child]

# --- NEW: helpers for dynamic RequiredForCOBie lists ---

RE_SPLIT_LIST = re.compile(r"[;,|\n]+|\s{2,}")

def parse_required_pairs(raw):
    """
    Accepts a string like:
      'Pset_ManufacturerTypeInformation.Manufacturer; COBie_Type.Name | COBie_Component.TagNumber'
    Returns a list of ('Pset', 'Property') tuples.
    """
    if not raw or not isinstance(raw, str):
        return []
    items = [t.strip() for t in RE_SPLIT_LIST.split(raw) if t.strip()]
    pairs = []
    for token in items:
        if "." in token:
            pset, prop = token.split(".", 1)
            pset, prop = pset.strip(), prop.strip()
            if pset and prop:
                pairs.append((pset, prop))
    return pairs

def get_pset_value(elem, pset_name, prop_name):
    """
    Try value on the occurrence first, then fall back to the element's Type (if any).
    """
    psets = ifcopenshell.util.element.get_psets(elem)
    if pset_name in psets and prop_name in psets[pset_name]:
        return psets[pset_name][prop_name]

    # Fallback to Type psets
    type_obj = None
    for rel in elem.IsDefinedBy or []:
        if rel.is_a("IfcRelDefinesByType"):
            type_obj = rel.RelatingType
            break
    if type_obj is not None:
        type_psets = ifcopenshell.util.element.get_psets(type_obj)
        if pset_name in type_psets and prop_name in type_psets[pset_name]:
            return type_psets[pset_name][prop_name]
    return ""

# --- Extract IFC data to Excel ---

def extract_to_excel(ifc_file):
    ifc_path = path_of(ifc_file)
    ifc = ifcopenshell.open(ifc_path)

    # --- Project Data sheet ---
    project_data = []
    project = ifc.by_type("IfcProject")[0]
    site = ifc.by_type("IfcSite")[0] if ifc.by_type("IfcSite") else None
    building = ifc.by_type("IfcBuilding")[0] if ifc.by_type("IfcBuilding") else None

    project_data.append({
        "DataType": "Project",
        "Name": getattr(project, "Name", ""),
        "Description": getattr(project, "Description", ""),
        "Phase": getattr(project, "Phase", ""),
    })
    if site:
        project_data.append({
            "DataType": "Site",
            "Name": getattr(site, "Name", ""),
            "Description": getattr(site, "Description", ""),
            "Phase": "",
        })
    else:
        # Include an empty Site row to allow creating one from Excel
        project_data.append({"DataType": "Site", "Name": "", "Description": "", "Phase": ""})
    if building:
        project_data.append({
            "DataType": "Building",
            "Name": getattr(building, "Name", ""),
            "Description": getattr(building, "Description", ""),
            "Phase": "",
        })
    project_df = pd.DataFrame(project_data)

    # --- Elements sheet ---
    element_data = []
    for elem in ifc.by_type("IfcElement"):
        elem_name = getattr(elem, "Name", "")
        elem_type = getattr(elem, "ObjectType", "")
        elem_desc = getattr(elem, "Description", "")
        type_obj = None
        for rel in ifc.get_inverse(elem):
            if rel.is_a("IfcRelDefinesByType"):
                type_obj = rel.RelatingType
        type_name = type_obj.Name if type_obj else ""
        element_data.append([
            elem.GlobalId,
            elem.is_a(),
            elem_name,
            elem_type,
            type_name,
            elem_desc
        ])
    elements_df = pd.DataFrame(
        element_data,
        columns=["GlobalId", "Class", "OccurrenceName", "OccurrenceType", "TypeName", "TypeDescription"]
    )

    # --- Properties sheet ---
    prop_data = []
    for elem in ifc.by_type("IfcElement"):
        for definition in elem.IsDefinedBy or []:
            if definition.is_a("IfcRelDefinesByProperties"):
                pset = definition.RelatingPropertyDefinition
                if pset.is_a("IfcPropertySet"):
                    for prop in pset.HasProperties:
                        val = None
                        if prop.is_a("IfcPropertySingleValue"):
                            if prop.NominalValue:
                                val = prop.NominalValue.wrappedValue
                        elif prop.is_a("IfcPropertyEnumeratedValue"):
                            if prop.EnumerationValues:
                                val = ", ".join(str(v.wrappedValue) for v in prop.EnumerationValues)
                        prop_data.append([
                            elem.GlobalId,
                            elem.is_a(),
                            pset.Name,
                            prop.Name,
                            val,
                        ])
    props_df = pd.DataFrame(prop_data, columns=["GlobalId", "Class", "PropertySet", "Property", "Value"])

    # --- COBie Mapping sheet ---
    cobie_cols = ["GlobalId", "IFCElement.Name", "IFCElementType.Name"]

    # NEW: discover extra columns from Additional_Pset_GeneralCommon lists
    dynamic_pairs = set()
    for elem in ifc.by_type("IfcElement"):
        psets_elem = ifcopenshell.util.element.get_psets(elem)
        # check on occurrence
        add_pset = psets_elem.get("Additional_Pset_GeneralCommon", {})
        dynamic_pairs.update(parse_required_pairs(add_pset.get("RequiredForCOBie", "")))
        dynamic_pairs.update(parse_required_pairs(add_pset.get("RequiredForCOBieComponent", "")))

        # also check on Type (many teams store these lists on the Type)
        type_obj = None
        for rel in elem.IsDefinedBy or []:
            if rel.is_a("IfcRelDefinesByType"):
                type_obj = rel.RelatingType
                break
        if type_obj is not None:
            psets_type = ifcopenshell.util.element.get_psets(type_obj)
            add_pset_t = psets_type.get("Additional_Pset_GeneralCommon", {})
            dynamic_pairs.update(parse_required_pairs(add_pset_t.get("RequiredForCOBie", "")))
            dynamic_pairs.update(parse_required_pairs(add_pset_t.get("RequiredForCOBieComponent", "")))

    # Build column list from the hardcoded mapping + discovered pairs
    mapping_pairs = []
    if COBIE_MAPPING:
        for pset, info in COBIE_MAPPING.items():
            for pname, _ in info["props"]:
                mapping_pairs.append((pset, pname))

    # Merge & de-duplicate while keeping a stable order (hardcoded first, then dynamic sorted)
    all_pairs = mapping_pairs + sorted(dynamic_pairs - set(mapping_pairs))
    for pset, pname in all_pairs:
        cobie_cols.append(f"{pset}.{pname}")

    cobie_rows = []
    for elem in ifc.by_type("IfcElement"):
        type_name = ""
        for rel in ifc.get_inverse(elem):
            if rel.is_a("IfcRelDefinesByType"):
                if rel.RelatingType:
                    type_name = getattr(rel.RelatingType, "Name", "")
                    break

        row = {
            "GlobalId": elem.GlobalId,
            "IFCElement.Name": getattr(elem, "Name", ""),
            "IFCElementType.Name": type_name
        }

        # Fill all columns (hardcoded + dynamic) with occurrence-first, type-fallback
        for pset, pname in all_pairs:
            key = f"{pset}.{pname}"
            row[key] = get_pset_value(elem, pset, pname)

        cobie_rows.append(row)

    cobie_df = pd.DataFrame(cobie_rows, columns=cobie_cols)

    # --- Uniclass (Pr & Ss) sheets ---
    def extract_uniclass(elem, target_name, is_ifc2x3):
        reference = ""
        name = ""
        for rel in elem.HasAssociations or []:
            if rel.is_a("IfcRelAssociatesClassification"):
                classification_ref = rel.RelatingClassification
                if classification_ref and classification_ref.is_a("IfcClassificationReference"):
                    if is_ifc2x3:
                        if getattr(classification_ref, "Name", "") == target_name:
                            return getattr(classification_ref, "ItemReference", ""), getattr(classification_ref, "Name", "")
                    else:
                        src = getattr(classification_ref, "ReferencedSource", None)
                        if src and getattr(src, "Name", "") == target_name:
                            return getattr(classification_ref, "ItemReference", ""), getattr(classification_ref, "Name", "")
        return reference, name

    is_ifc2x3 = ifc.schema == "IFC2X3"
    pr_rows, ss_rows = [], []
    for elem in ifc.by_type("IfcElement"):
        pr_ref, pr_name = extract_uniclass(elem, "Uniclass Pr Products", is_ifc2x3)
        ss_ref, ss_name = extract_uniclass(elem, "Uniclass Ss Systems", is_ifc2x3)
        pr_rows.append({"GlobalId": elem.GlobalId, "Reference": pr_ref, "Name": pr_name})
        ss_rows.append({"GlobalId": elem.GlobalId, "Reference": ss_ref, "Name": ss_name})

    uniclass_pr_df = pd.DataFrame(pr_rows)
    uniclass_ss_df = pd.DataFrame(ss_rows)

    out_path = os.path.join(tempfile.gettempdir(), "extracted.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        project_df.to_excel(writer, sheet_name="ProjectData", index=False)
        elements_df.to_excel(writer, sheet_name="Elements", index=False)
        props_df.to_excel(writer, sheet_name="Properties", index=False)
        cobie_df.to_excel(writer, sheet_name="COBieMapping", index=False)
        uniclass_pr_df.to_excel(writer, sheet_name="Uniclass_Pr", index=False)
        uniclass_ss_df.to_excel(writer, sheet_name="Uniclass_Ss", index=False)
    return out_path

# --- Update IFC from Excel ---

def update_ifc_from_excel(ifc_file, excel_file, update_mode="update", add_new="no"):
    ifc_path = path_of(ifc_file)
    xls_path = path_of(excel_file)
    ifc = ifcopenshell.open(ifc_path)
    xls = pd.ExcelFile(xls_path)
    elements_df = pd.read_excel(xls, "Elements")
    props_df = pd.read_excel(xls, "Properties")
    cobie_df = pd.read_excel(xls, "COBieMapping")
    project_df = pd.read_excel(xls, "ProjectData")
    # New tabs (fallback safe):
    try:
        uniclass_pr_df = pd.read_excel(xls, "Uniclass_Pr")
    except Exception:
        uniclass_pr_df = None
    try:
        uniclass_ss_df = pd.read_excel(xls, "Uniclass_Ss")
    except Exception:
        uniclass_ss_df = None

    # --- Project / Site / Building updates ---
    project = ifc.by_type("IfcProject")[0]
    site = ifc.by_type("IfcSite")[0] if ifc.by_type("IfcSite") else None
    building = ifc.by_type("IfcBuilding")[0] if ifc.by_type("IfcBuilding") else None

    for _, row in project_df.iterrows():
        dt = row["DataType"]
        if dt == "Project":
            if pd.notna(row.get("Name")):
                project.Name = clean_value(row["Name"]) or project.Name
            if pd.notna(row.get("Description")):
                project.Description = clean_value(row["Description"]) or project.Description
            if pd.notna(row.get("Phase")):
                project.Phase = clean_value(row["Phase"]) or project.Phase
        elif dt == "Site":
            name = clean_value(row.get("Name"))
            desc = clean_value(row.get("Description"))
            if site is None and (add_new == "yes" or name or desc):
                site = ifc.create_entity("IfcSite", GlobalId=ifcopenshell.guid.new(), Name=name or "Site")
            if site is not None:
                if name is not None:
                    site.Name = name
                if desc is not None:
                    site.Description = desc
                # Ensure Project > Site aggregation
                ensure_aggregates(project, site, ifc)
                # Also ensure Building is under Site (if a building exists)
                if building is not None:
                    ensure_aggregates(site, building, ifc)
        elif dt == "Building":
            if building is None and add_new == "yes":
                building = ifc.create_entity(
                    "IfcBuilding",
                    GlobalId=ifcopenshell.guid.new(),
                    Name=clean_value(row.get("Name")) or "Building",
                )
            if building is not None:
                if pd.notna(row.get("Name")):
                    building.Name = clean_value(row["Name"]) or building.Name
                if pd.notna(row.get("Description")):
                    building.Description = clean_value(row["Description"]) or building.Description
                # If a site exists, make sure Building is aggregated by Site
                if site is not None:
                    ensure_aggregates(site, building, ifc)
                else:
                    # Fallback: aggregate directly under Project
                    ensure_aggregates(project, building, ifc)

    # --- Elements updates ---
    for _, row in elements_df.iterrows():
        elem = ifc.by_guid(row["GlobalId"]) if pd.notna(row.get("GlobalId")) else None
        if not elem:
            continue
        if pd.notna(row.get("OccurrenceName")):
            elem.Name = clean_value(row["OccurrenceName"]) or elem.Name
        if pd.notna(row.get("OccurrenceType")):
            elem.ObjectType = clean_value(row["OccurrenceType"]) or elem.ObjectType
        if pd.notna(row.get("TypeDescription")):
            elem.Description = clean_value(row["TypeDescription"]) or elem.Description
        if pd.notna(row.get("TypeName")):
            type_name = str(clean_value(row["TypeName"]))
            type_obj = None
            for rel in ifc.get_inverse(elem):
                if rel.is_a("IfcRelDefinesByType"):
                    type_obj = rel.RelatingType
            if not type_obj and add_new == "yes":
                # Create a matching type class when possible
                type_class = (
                    elem.is_a() + "Type"
                    if elem.is_a().endswith("Element") or elem.is_a().endswith("Door")
                    else "IfcTypeObject"
                )
                try:
                    type_obj = ifcopenshell.api.run("type.create_type", ifc, ifc_class=type_class, name=type_name)
                except Exception:
                    type_obj = ifc.create_entity(
                        "IfcBuildingElementType",
                        GlobalId=ifcopenshell.guid.new(),
                        Name=type_name,
                    )
                ifcopenshell.api.run("type.assign_type", ifc, related_objects=[elem], relating_type=type_obj)
            elif type_obj:
                type_obj.Name = type_name

    # --- COBie Mapping updates (generic Pset.Property writer) ---
    if cobie_df is not None:
        # Precompute the set of mapping keys we already know about
        mapping_keys = set()
        if COBIE_MAPPING is not None:
            for pset, info in COBIE_MAPPING.items():
                for pname, _ in info["props"]:
                    mapping_keys.add(f"{pset}.{pname}")

        # Identify any column that looks like Pset.Property (skip the first three meta columns)
        candidate_cols = [
            c
            for c in cobie_df.columns
            if c not in ("GlobalId", "IFCElement.Name", "IFCElementType.Name") and "." in c
        ]

        for _, row in cobie_df.iterrows():
            guid = row.get("GlobalId")
            if pd.isna(guid):
                continue
            elem = ifc.by_guid(guid)
            if not elem:
                continue

            for col in candidate_cols:
                if pd.isna(row.get(col)):
                    continue
                val = row[col]
                pset, pname = col.split(".", 1)
                pset, pname = pset.strip(), pname.strip()

                # Ensure pset exists (respect add_new flag)
                psets = ifcopenshell.util.element.get_psets(elem)
                if pset not in psets and add_new == "no":
                    continue

                # Find/create the actual IfcPropertySet entity
                pset_entity = None
                for rel in elem.IsDefinedBy or []:
                    if (
                        rel.is_a("IfcRelDefinesByProperties")
                        and rel.RelatingPropertyDefinition
                        and getattr(rel.RelatingPropertyDefinition, "Name", "") == pset
                    ):
                        pset_entity = rel.RelatingPropertyDefinition
                        break
                if pset_entity is None and add_new == "yes":
                    pset_entity = ifcopenshell.api.run("pset.add_pset", ifc, product=elem, name=pset)

                if pset_entity:
                    try:
                        ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset_entity, properties={pname: val})
                    except Exception as e:
                        print(f"⚠️ Could not set {pset}.{pname} for {guid}: {e}")

    # --- Uniclass updates (Pr & Ss) ---
    def set_uniclass(df, source_name):
        if df is None:
            return
        # Find or create the classification source (use IfcClassification in all schemas)
        cls_src = None
        for c in ifc.by_type("IfcClassification"):
            if getattr(c, "Name", "") == source_name:
                cls_src = c
                break
        if cls_src is None and add_new == "yes":
            cls_src = ifc.create_entity(
                "IfcClassification",
                Name=source_name,
                Source="https://www.thenbs.com/our-tools/uniclass-2015",
                Edition="2015",
            )
        for _, r in df.iterrows():
            guid = r.get("GlobalId")
            if pd.isna(guid):
                continue
            elem = ifc.by_guid(guid)
            if not elem:
                continue
            ref = clean_value(r.get("Reference"))
            nm = clean_value(r.get("Name"))
            if ref is None and nm is None:
                continue
            # Check for existing association of this source
            existing_ref = None
            for rel in elem.HasAssociations or []:
                if rel.is_a("IfcRelAssociatesClassification"):
                    cref = rel.RelatingClassification
                    if cref and cref.is_a("IfcClassificationReference"):
                        src = getattr(cref, "ReferencedSource", None)
                        if src and getattr(src, "Name", "") == source_name:
                            existing_ref = cref
                            break
            if existing_ref:
                if ref is not None:
                    existing_ref.ItemReference = str(ref)
                if nm is not None:
                    existing_ref.Name = str(nm)
            elif add_new == "yes" and (ref is not None or nm is not None) and cls_src is not None:
                cref = ifc.create_entity(
                    "IfcClassificationReference",
                    ItemReference=str(ref) if ref is not None else None,
                    Name=str(nm) if nm is not None else None,
                )
                cref.ReferencedSource = cls_src
                ifc.create_entity(
                    "IfcRelAssociatesClassification",
                    GlobalId=ifcopenshell.guid.new(),
                    RelatedObjects=[elem],
                    RelatingClassification=cref,
                )

    set_uniclass(uniclass_pr_df, "Uniclass Pr Products")
    set_uniclass(uniclass_ss_df, "Uniclass Ss Systems")

    out_path = os.path.join(tempfile.gettempdir(), "updated.ifc")
    ifc.write(out_path)
    return out_path

# --- Gradio UI ---

def apply_changes(ifc_file, excel_file, mode, add_new):
    return update_ifc_from_excel(ifc_file, excel_file, update_mode=mode, add_new=add_new)

with gr.Blocks() as demo:
    gr.Markdown(
        "## IFC Excel Extractor / Updater with COBie Mapping + Uniclass (Pr & Ss) + Dynamic RequiredForCOBie"
    )

    ifc_input = gr.File(label="Upload IFC", type="filepath")
    excel_input = gr.File(label="Upload Excel", type="filepath")

    with gr.Row():
        mode = gr.Radio(["update", "refresh"], value="update", label="Update Mode")
        add_new = gr.Radio(["no", "yes"], value="no", label="Add New Parameters/Entities?")

    with gr.Row():
        extract_btn = gr.Button("Extract to Excel")
        apply_btn = gr.Button("Apply Excel Changes to IFC")

    excel_out = gr.File(label="Download Excel")
    ifc_out = gr.File(label="Download Updated IFC")

    extract_btn.click(fn=extract_to_excel, inputs=ifc_input, outputs=excel_out)
    apply_btn.click(fn=apply_changes, inputs=[ifc_input, excel_input, mode, add_new], outputs=ifc_out)

    gr.Markdown(
        "Sheets: **ProjectData**, **Elements**, **Properties**, **COBieMapping**, **Uniclass_Pr**, **Uniclass_Ss**"
    )

if __name__ == "__main__":
    demo.launch()
