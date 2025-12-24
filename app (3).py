import re
import os
import gradio as gr

# Try IfcOpenShell for schema enums
try:
    import ifcopenshell
    IFCOPEN = True
except ImportError:
    ifcopenshell = None
    IFCOPEN = False


# ---------------------------------------------------------
# Build enum library from IFC schema (IfcOpenShell)
# ---------------------------------------------------------
def build_enum_library(model):
    enums = {}
    if not IFCOPEN or model is None:
        return enums

    try:
        schema = model.wrapped_data.schema
    except Exception:
        try:
            schema = model.wrapped_data.schema()
        except Exception:
            return enums

    try:
        for t in schema.types():
            try:
                is_enum = t.is_enumeration_type()
            except AttributeError:
                is_enum = getattr(t, "is_enumeration", lambda: False)()
            if not is_enum:
                continue

            name = t.name()
            values = set()
            try:
                for it in t.enumeration_items():
                    values.add(it.name())
            except Exception:
                pass
            if values:
                enums[name] = values
    except Exception as e:
        print("Warning while building enum library:", e)

    return enums


# Fallback enums if schema lookup fails completely
FALLBACK_ENUM_LIBRARY = {
    "IfcWasteTerminalTypeEnum": {
        "FLOORTRAP",
        "FLOORWASTE",
        "GULLYSUMP",
        "GULLYTRAP",
        "GREASEINTERCEPTOR",
        "OILINTERCEPTOR",
        "PETROLINTERCEPTOR",
        "ROOFDRAIN",
        "WASTEDISPOSALUNIT",
        "WASTETRAP",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcPipeSegmentTypeEnum": {
        "CULVERT",
        "FLEXIBLESEGMENT",
        "GUTTER",
        "RIGIDSEGMENT",
        "SPOOL",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcDistributionChamberElementTypeEnum": {
        "FORMEDDUCT",
        "INSPECTIONCHAMBER",
        "INSPECTIONPIT",
        "MANHOLE",
        "METERCHAMBER",
        "SUMP",
        "TRENCH",
        "VALVECHAMBER",
        "USERDEFINED",
        "NOTDEFINED",
    },
    "IfcTankTypeEnum": {
        "PREFORMED",
        "SECTIONAL",
        "EXPANSION",
        "PRESSUREVESSEL",
        "FEEDANDEXPANSION",
        "USERDEFINED",
        "NOTDEFINED",
    },
}


# ---------------------------------------------------------
# TYPE library: first token in TypeName → IFC type + occ entity
# ---------------------------------------------------------
# Examples:
#   Waste Terminal_WasteTrap_Type01
#   Pipe Segment_RigidSegment_Type01
#   Distribution Chamber Element_InspectionChamber_Type03
#   Tank_Preformed_Type01

TYPE_LIBRARY = {
    # Waste terminals: FlowTerminal + WasteTerminalType
    "waste terminal": {
        "type_entity": "IFCWASTETERMINALTYPE",
        "enum_set": "IfcWasteTerminalTypeEnum",
        "occ_entity": "IFCFLOWTERMINAL",
    },
    # Chambers: dedicated occurrence + type in IFC2x3
    "distribution chamber element": {
        "type_entity": "IFCDISTRIBUTIONCHAMBERELEMENTTYPE",
        "enum_set": "IfcDistributionChamberElementTypeEnum",
        "occ_entity": "IFCDISTRIBUTIONCHAMBERELEMENT",
    },
    # Pipe segments: FlowSegment + PipeSegmentType (IFC2x3)
    "pipe segment": {
        "type_entity": "IFCPIPESEGMENTTYPE",
        "enum_set": "IfcPipeSegmentTypeEnum",
        "occ_entity": "IFCFLOWSEGMENT",
    },
    # Alias: just "Pipe_..."
    "pipe": {
        "type_entity": "IFCPIPESEGMENTTYPE",
        "enum_set": "IfcPipeSegmentTypeEnum",
        "occ_entity": "IFCFLOWSEGMENT",
    },
    # Tanks: FlowStorageDevice + TankType (IFC2x3)
    "tank": {
        "type_entity": "IFCTANKTYPE",
        "enum_set": "IfcTankTypeEnum",
        "occ_entity": "IFCFLOWSTORAGEDEVICE",
    },
}

# Forced predefined enums (e.g. pipes → always RIGIDSEGMENT)
FORCED_PREDEFINED = {
    "ifcpipesegmenttype": "RIGIDSEGMENT",
}


def parse_type_tokens(type_name: str):
    """Split 'Class_Predefined_TypeXX' logic."""
    parts = type_name.split("_")
    class_token = parts[0].strip().lower() if parts else ""
    predef_raw = parts[1].strip() if len(parts) > 1 else ""
    return class_token, predef_raw


def enum_from_token(raw: str, enum_set: str, enumlib: dict) -> str:
    """Map textual token to an IFC2x3 enum literal, or USERDEFINED."""
    if not raw:
        return "USERDEFINED"
    candidate = raw.replace(" ", "").upper()
    values = enumlib.get(enum_set, set())
    return candidate if candidate in values else "USERDEFINED"


# ---------------------------------------------------------
# Core processing: TYPE objects + occurrences
# ---------------------------------------------------------
def process_ifc(ifc_file):
    if ifc_file is None:
        return None, "No file uploaded."

    in_path = ifc_file.name
    with open(in_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    # Build enum library
    enumlib = {}
    model = None
    if IFCOPEN:
        try:
            model = ifcopenshell.open(in_path)
            enumlib = build_enum_library(model)
        except Exception as e:
            print("Warning: could not open IFC with IfcOpenShell:", e)

    if not enumlib:
        enumlib = FALLBACK_ENUM_LIBRARY.copy()

    # Stats
    stats = {
        "proxy_types_total": 0,
        "building_types_total": 0,
        "proxy_types_converted": 0,
        "building_types_converted": 0,
        "left_as_proxy_type": 0,
        "left_as_building_type": 0,
        "occurrences_converted": 0,
    }

    # Map type id → occurrence entity (e.g. "#745022" → "IFCFLOWSEGMENT")
    typeid_to_occ_entity = {}

    # Regex for IFCBUILDINGELEMENTPROXYTYPE
    proxy_type_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTPROXYTYPE"
        r"\('(?P<guid>[^']*)',"
        r"(?P<own>[^,]*),"
        r"'(?P<name>[^']*)',"
        r"(?P<mid>.*),"
        r"\.(?P<enum>\w+)\.\);",
        re.IGNORECASE,
    )

    # Regex for IFCBUILDINGELEMENTTYPE (no PredefinedType in IFC2x3)
    building_type_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTTYPE"
        r"\('(?P<guid>[^']*)',"
        r"(?P<own>[^,]*),"
        r"'(?P<name>[^']*)',"
        r"(?P<mid>.*)\);",
        re.IGNORECASE,
    )

    # PASS 1: retype TYPE objects and fill typeid_to_occ_entity
    updated_lines = []

    for line in lines:
        # --- Case 1: ProxyType ---
        m_proxy = proxy_type_re.match(line)
        if m_proxy:
            stats["proxy_types_total"] += 1
            g = m_proxy.groupdict()
            ws = g["ws"]
            type_id = g["id"]
            guid = g["guid"]
            owner = g["own"]
            type_name = g["name"]
            mid = g["mid"]

            class_token, predef_raw = parse_type_tokens(type_name)
            class_norm = class_token.lower()

            lib_entry = None
            if class_norm in TYPE_LIBRARY:
                lib_entry = TYPE_LIBRARY[class_norm]
            elif "pipe" in type_name.lower():
                lib_entry = TYPE_LIBRARY["pipe"]

            if not lib_entry:
                stats["left_as_proxy_type"] += 1
                updated_lines.append(line)
                continue

            target_type = lib_entry["type_entity"]
            enum_set = lib_entry["enum_set"]

            forced = FORCED_PREDEFINED.get(target_type.lower())
            if forced:
                enum_val = forced
            else:
                enum_val = enum_from_token(predef_raw, enum_set, enumlib)

            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["proxy_types_converted"] += 1

            typeid_to_occ_entity[type_id] = lib_entry["occ_entity"]
            continue

        # --- Case 2: BuildingElementType ---
        m_build = building_type_re.match(line)
        if m_build:
            stats["building_types_total"] += 1
            g = m_build.groupdict()
            ws = g["ws"]
            type_id = g["id"]
            guid = g["guid"]
            owner = g["own"]
            type_name = g["name"]
            mid = g["mid"]

            class_token, predef_raw = parse_type_tokens(type_name)
            class_norm = class_token.lower()

            lib_entry = None
            if class_norm in TYPE_LIBRARY:
                lib_entry = TYPE_LIBRARY[class_norm]
            elif "pipe" in type_name.lower():
                lib_entry = TYPE_LIBRARY["pipe"]

            if not lib_entry:
                stats["left_as_building_type"] += 1
                updated_lines.append(line)
                continue

            target_type = lib_entry["type_entity"]
            enum_set = lib_entry["enum_set"]

            forced = FORCED_PREDEFINED.get(target_type.lower())
            if forced:
                enum_val = forced
            else:
                enum_val = enum_from_token(predef_raw, enum_set, enumlib)

            # BUILDINGELEMENTTYPE has 9 args; we add PredefinedType as 10th
            new_line = (
                f"{ws}{type_id}={target_type}('{guid}',{owner},"
                f"'{type_name}',{mid},.{enum_val}.);"
            )
            updated_lines.append(new_line)
            stats["building_types_converted"] += 1

            typeid_to_occ_entity[type_id] = lib_entry["occ_entity"]
            continue

        # --- Anything else: pass through ---
        updated_lines.append(line)

    # PASS 2: read IfcRelDefinesByType and map occurrences → entity
    occid_to_entity = {}

    rel_def_type_re = re.compile(
        r"^(?P<ws>\s*)#(?P<relid>\d+)=IFCRELDEFINESBYTYPE\("
        r"'(?P<guid>[^']*)',"
        r"(?P<owner>[^,]*),"
        r"(?P<name>[^,]*),"
        r"(?P<desc>[^,]*),"
        r"\((?P<objs>[^)]*)\),"
        r"(?P<typeid>#\d+)\);",
        re.IGNORECASE,
    )

    for line in updated_lines:
        m = rel_def_type_re.match(line)
        if not m:
            continue

        d = m.groupdict()
        type_id = d["typeid"]
        if type_id not in typeid_to_occ_entity:
            continue  # type not converted / not in our library

        occ_entity = typeid_to_occ_entity[type_id]
        objs_raw = d["objs"]
        obj_ids = [o.strip() for o in objs_raw.split(",") if o.strip()]
        for oid in obj_ids:
            occid_to_entity[oid] = occ_entity

    # PASS 3: rewrite occurrence entity names for mapped ids
    final_lines = []

    occ_re = re.compile(
        r"^(?P<ws>\s*)(?P<id>#\d+)=IFCBUILDINGELEMENTPROXY\(",
        re.IGNORECASE,
    )

    for line in updated_lines:
        m = occ_re.match(line)
        if not m:
            final_lines.append(line)
            continue

        ws = m.group("ws")
        occ_id = m.group("id")
        if occ_id not in occid_to_entity:
            final_lines.append(line)
            continue

        new_entity = occid_to_entity[occ_id]
        rest = line[m.end():]  # everything after '('
        new_line = f"{ws}{occ_id}={new_entity}({rest}"
        final_lines.append(new_line)
        stats["occurrences_converted"] += 1

    # Write output IFC
    base = os.path.basename(in_path)
    root, ext = os.path.splitext(base)
    out_path = root + "_typed" + ext

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(final_lines)

    summary = (
        f"Input file:  {base}\n"
        f"Output file: {out_path}\n\n"
        f"Proxy types (IFCBUILDINGELEMENTPROXYTYPE) found: {stats['proxy_types_total']}\n"
        f"  → converted to specific IFC types: {stats['proxy_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTPROXYTYPE: {stats['left_as_proxy_type']}\n\n"
        f"Building types (IFCBUILDINGELEMENTTYPE) found: {stats['building_types_total']}\n"
        f"  → converted to specific IFC types: {stats['building_types_converted']}\n"
        f"  → left as IFCBUILDINGELEMENTTYPE: {stats['left_as_building_type']}\n\n"
        f"Occurrences converted from IfcBuildingElementProxy to typed entities: "
        f"{stats['occurrences_converted']}\n\n"
        "Occurrences are retyped only when an IfcRelDefinesByType exists and the "
        "referenced type could be mapped. Mapping is IFC2x3-compliant: waste terminals "
        "→ IfcFlowTerminal/IfcWasteTerminalType, pipe segments → "
        "IfcFlowSegment/IfcPipeSegmentType, tanks → "
        "IfcFlowStorageDevice/IfcTankType, distribution chambers → "
        "IfcDistributionChamberElement/IfcDistributionChamberElementType.\n"
    )

    return out_path, summary


# ---------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------
demo = gr.Interface(
    fn=process_ifc,
    inputs=gr.File(label="Upload IFC2x3 file (.ifc)", file_types=[".ifc"]),
    outputs=[
        gr.File(label="Processed IFC file"),
        gr.Textbox(label="Processing summary", lines=18),
    ],
    title="IFC2x3 Type + Occurrence Mapper (Civil 3D export)",
    description=(
        "Retypes IFCBUILDINGELEMENTTYPE / IFCBUILDINGELEMENTPROXYTYPE into domain types "
        "based on TypeName patterns (Waste Terminal, Pipe Segment, Tank, Distribution "
        "Chamber Element), sets valid PredefinedType enums, and retypes related "
        "IFCBUILDINGELEMENTPROXY instances to IFC2x3 domain entities using "
        "IfcRelDefinesByType."
    ),
)

demo.launch()
