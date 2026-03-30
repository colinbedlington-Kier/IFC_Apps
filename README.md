---
title: IFC Toolkit Hub
emoji: 🛰️
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 8000
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

## Presentation Layer Alignment

The **Presentation Layer Alignment** page scans IFC uploads for overly-specific Uniclass Ss layer tokens, proposes a shallower target, and lets you apply approved mappings to a new IFC copy.

### Usage

1. Upload an IFC on the **Upload & Session** page.
2. Open **Presentation Layer Alignment** from the navigation bar.
3. Paste an Allowed Layers “One Of [ ... ]” list or import a `.txt`/`.csv` file (the tool also seeds a built-in allowed list specific to IFC Tools validation targets).
4. Edit explicit overrides, toggle auto-shallowing, and (optionally) enable updating both presentation layers and `Layer` properties.
5. Scan, filter the results, and apply the approved rows to download the updated IFC plus JSON/CSV change logs.

## Proxy → Types enhancements

The **Proxy → Types** page now includes a **PredefinedType Fixer** panel that can run against any selected IfcClasses:

- Uses the existing type-name matching logic to set `PredefinedType` to `NOTDEFINED` when a match is found or `USERDEFINED` otherwise.
- Skips entities without a `PredefinedType` attribute (reported as N/A).
- Supports dry-run mode to review proposed changes before exporting a new IFC plus change logs.


## COBie QC tool (integrated)

COBie QC is now a built-in IFC Toolkit page available at **/tools/cobieqc** (no Gradio runtime).

### Runtime requirements

- Java runtime is required because the backend executes `CobieQcReporter.jar`.
- Railway/web runtimes must bind to `0.0.0.0:$PORT` (with local fallback `8000`).
- A lightweight health endpoint is available at `GET /health` for platform healthchecks.
- The reporter JAR is not committed in this repository. Configure its location with `COBIEQC_JAR_PATH`, or place it in one of:
  - `vendor/cobieqc/CobieQcReporter.jar` (repo-local)
  - `COBieQC/CobieQcReporter/CobieQcReporter.jar` (legacy in-repo path)
  - `/app/COBieQC/CobieQcReporter/CobieQcReporter.jar`
  - `/app/CobieQcReporter/CobieQcReporter.jar`
  - `/opt/COBieQC/CobieQcReporter/CobieQcReporter.jar`
- COBieQC resources (`xsl_xml`) are discovered from similar repo-local and absolute paths, or from `COBIEQC_RESOURCE_DIR`.
- `COBIEQC_DATA_DIR` can be used to point both JAR/resources discovery and bootstrap copy destination to a stable runtime path.
- Optional bootstrap source overrides are available:
  - `COBIEQC_JAR_SOURCE=/path/to/CobieQcReporter.jar`
  - `COBIEQC_RESOURCE_SOURCE=/path/to/xsl_xml`
- Hugging Face Docker Spaces install Java from `packages.txt` (`openjdk-21-jre-headless`).
- The repo `Dockerfile` (used for local/containerized runs) also installs `openjdk-21-jre-headless`.
- Reports are generated into per-job data directories (`$IFC_APP_DATA_DIR/jobs/cobieqc/<job_id>/`) instead of `COBieQC/reports/`.

### API endpoints

- `POST /api/tools/cobieqc/run` — multipart upload (`file=.xlsx`, `stage=D|C`) and returns a queued `job_id`.
- `GET /api/tools/cobieqc/jobs/{job_id}` — job status, progress, and log tail.
- `GET /api/tools/cobieqc/jobs/{job_id}/result` — result metadata and preview HTML.
- `GET /api/tools/cobieqc/jobs/{job_id}/download` — download generated report HTML.
- `GET /api/tools/cobieqc/health` — Java/runtime diagnostic.

## IFC File Size Reducer

The **IFC File Size Reducer** tool is available at **/tools/reduce-file-size** and only uses existing session files (no direct upload in-tool).

### Modes

- **Compress Only**: “Creates a smaller packaged copy without changing model content.”
- **Conservative Viewer Copy**: “Applies limited reductions suitable for coordination / viewing copies.”
- **Aggressive Viewer Copy**: “Removes more metadata and may affect downstream use. For read-only copies only.”
- **Split by Storey**: “Reduces scope by creating separate files per storey.”

### Endpoints

- `POST /api/ifc-tools/reduce-file-size/analyse`
- `POST /api/ifc-tools/reduce-file-size/run`

### Limitations and warnings

- `Optimise` is treated as expert-only and disabled by default; it is lossy and documented as very slow.
- `PurgeData` is destructive and intended for stripped-down viewer copies.
- IFCZIP is usually the safest first option when the goal is transfer/storage reduction only.
- Split-by-storey depends on available IfcPatch recipe support in the runtime.

## Running locally

```bash
uvicorn app:app --reload
```

## IFC extractor async jobs

The IFC Data Extractor now uses database-backed queued jobs to keep heavy extraction off the web request path.

### New endpoints

- `POST /api/ifc/jobs`
- `GET /api/ifc/jobs/{id}`
- `GET /api/ifc/jobs/{id}/result`
- `POST /api/ifc/jobs/{id}/cancel`

Backwards-compatible endpoint retained:

- `POST /api/session/{session_id}/data-extractor/start` (returns `jobId` and deprecation note)

### Environment variables

- `DATABASE_URL` (required for queue)
- `IFC_MAX_TOTAL_BYTES` (default `500000000`)
- `IFC_MAX_FILES_PER_JOB` (default `5`)
- `IFC_JOB_TIMEOUT_SECONDS` (default `1200`)
- `IFC_WORKER_CONCURRENCY` (default `1`)
- `IFC_OUTPUT_BUCKET` (default `ifc-outputs`)

### Railway process commands

- Web: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Worker: `python -m backend.ifc_worker`
