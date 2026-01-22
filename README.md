---
title: IFC Toolkit Hub
emoji: 🛰️
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: "5.49.1"
app_file: app.py
pinned: false
---

Check out the configuration reference at https://huggingface.co/docs/hub/spaces-config-reference

## IFC Toolkit Hub viewer

The new **Viewer** page lets you load IFC session uploads directly in the browser with [IFC.js](https://ifcjs.github.io/info/) (powered by `web-ifc`). Processing stays client-side so models never leave your machine after the initial upload.

### Supported inputs

- `.ifc` and `.ifczip` session files (uploaded from the Uploads page)
- Best results with models up to ~150–200 MB; much larger files may work but can be slow in the browser depending on GPU and memory.

### Usage

1. Upload an IFC on the **Uploads** page (the session ID is stored in `localStorage` under `ifc_session_id`).
2. Open **Viewer** from the navigation bar and pick a session file.
3. Use the toolbar to refresh files, load the selection, fit the model to view, toggle edges, and add/remove a section plane.
4. Click elements in the scene to see their properties in the sidebar, and expand the spatial tree to browse layers.

## Model Checking (Editable)

The **Model Checking (Editable)** page adds a spreadsheet-driven validation UI that reads the DfE model checking workbook (sheet `09-IFC-SPF Model Checking Reqs`). Checks are grouped into logical sections (Project, Site/Building, Storeys, Spaces, Object Types, and Object Occurrences) and rendered as editable, virtualised tables.

- Select an IFC from your session, choose an optional RIBA stage filter, and load sections to view current values.
- Inline edits stay client-side until **Apply changes** writes them back to a copy of the IFC; the updated file is downloadable from the same session.
- Required/enum validations surface directly in the table, with per-row and per-section issue counts.
- Classification edits use proper `IfcClassification` / `IfcClassificationReference` / `IfcRelAssociatesClassification` relations and never remove unrelated assignments.
- Generated classification values can be derived from expressions such as `{Pset_RoomCommon.RoomTag}-{Name}`; toggle the “Use generated” control per cell to apply them.

### Mapping & expressions

- Default mappings live in `config/check_field_mappings.json` (seeded for IfcProject Name/Description/Phase, IfcBuildingStorey heights, IfcSpace room tags and areas, and Uniclass/DfE ADS systems).
- Expressions are stored in `config/check_expressions.json` and can be edited through **Admin / Mapping**.
- Unmapped checks are listed on the Admin screen; assign a field type (attribute/property/quantity/classification/predefined type) and save to persist for future sessions.

### Developer notes

- Core helpers live in `check_definitions_loader.py`, `field_access.py`, `classification_writer.py`, `expression_engine.py`, and `validation.py`.
- Tests cover classification write-back, expression token resolution, and property creation; run `pytest` to validate.

## Presentation Layer Purge

The **Presentation Layer Purge** page scans IFC uploads for overly-specific Uniclass Ss layer tokens, proposes a shallower target, and lets you apply approved mappings to a new IFC copy.

### Usage

1. Upload an IFC on the **Upload & Session** page.
2. Open **Presentation Layer Purge** from the navigation bar.
3. Paste an Allowed Layers “One Of [ ... ]” list or import a `.txt`/`.csv` file.
4. Edit explicit overrides, toggle auto-shallowing, and (optionally) enable updating both presentation layers and `Layer` properties.
5. Scan, filter the results, and apply the approved rows to download the updated IFC plus JSON/CSV change logs.

## Proxy → Types enhancements

The **Proxy → Types** page now includes a **PredefinedType Fixer** panel that can run against any selected IfcClasses:

- Uses the existing type-name matching logic to set `PredefinedType` to `NOTDEFINED` when a match is found or `USERDEFINED` otherwise.
- Skips entities without a `PredefinedType` attribute (reported as N/A).
- Supports dry-run mode to review proposed changes before exporting a new IFC plus change logs.

## Running locally

```bash
uvicorn app:app --reload
```
