# Memory-Safe IFC Processing (Railway)

## Likely OOM cause

The original request path could hold multiple large objects at once:

- uploaded file bytes in memory,
- parsed `pandas` workbook objects,
- parsed `ifcopenshell` model graph,
- output buffers,
- plus Java heap during COBieQC.

On Railway Hobby-sized memory budgets, this can exceed container limits during large IFC/Excel updates.

## Safeguards added

- **Stage-level memory instrumentation** with RSS (when `psutil` is installed) around:
  - upload received,
  - workbook load,
  - IFC open,
  - row iteration/update,
  - IFC export,
  - COBieQC launch,
  - response complete.
- **Single-flight heavy job guard** to prevent concurrent heavy IFC mutation tasks per replica.
- **Upload and file-size protective limits** via env vars:
  - `MAX_UPLOAD_BYTES`
  - `MAX_IFC_BYTES`
  - `MAX_EXCEL_BYTES`
- **Heavy-job timeout guard** via `HEAVY_JOB_TIMEOUT_SECONDS`.
- **COBieQC Java heap cap** via `COBIEQC_JAVA_XMX_MB` (default `512`) plus `-Xms128m`.
- **COBieQC bootstrap hardening**:
  - uses folder-based resource validation (`xsl_xml` with expected XML/XSL files),
  - avoids ZIP download/extract paths for XML/XSL resources,
  - falls back cleanly to disabled state if resources are unavailable.
- **Streaming upload writes** to disk for IFC QA/COBieQC/session uploads to avoid large request-body copies in RAM.
- **Explicit cleanup/gc hooks** after heavy Excel→IFC update processing.

## Recommended Railway sizing

- **Hobby**: suitable for light/medium IFC files and single-flight jobs.
- **Pro**: recommended for large models, sustained IFC QA usage, or frequent COBieQC runs.

If your workload regularly exceeds `MAX_IFC_BYTES`, upgrade memory or move heavy work off the web process.

## Hobby vs Pro guidance

Use **Hobby** when:
- IFC files are small/moderate,
- concurrency expectations are low,
- occasional 429 retry behavior is acceptable.

Use **Pro** when:
- large IFCs are common,
- lower latency under load is required,
- you need more headroom for Python + Java in the same service.

## Future architecture option

For strongest isolation, move heavy IFC/QA/COBieQC tasks to a **worker service**:

- API service: lightweight request validation + job enqueue.
- Worker service: memory-intensive processing with dedicated resources.
- Shared object storage for inputs/outputs.

This decouples user-facing API reliability from batch-processing peaks.
