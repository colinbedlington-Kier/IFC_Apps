# IFC → Excel Extraction Investigation Report

## Current extraction pipeline summary (before refactor)

1. `/api/session/{session_id}/excel/extract` called `extract_to_excel` directly.
2. `extract_to_excel` opened IFC and built **all sheets unconditionally** (ProjectData, Elements, Properties, COBieMapping, Uniclass_Pr/Ss/EF).
3. The function repeatedly traversed relationships and repeatedly called `ifcopenshell.util.element.get_psets` per element and per field.
4. UI had no preview stage; clicking “Extract to Excel” always ran full extraction.

## Where time was spent (high-level)

Based on static code-path profiling design and new stage timers, highest-cost areas were:

1. Repeated pset resolution in COBie field population (`get_pset_value` called inside nested loops).
2. Repeated type-resolution scans (`IfcRelDefinesByType`) and repeated container traversal for spatial fields.
3. Full properties and classifications extraction even when user only needs subset outputs.
4. Eager DataFrame creation for all sheets regardless of user intent.

## Logic changes vs older/faster approach

Legacy extraction paths used table-scoped writers (for CSV in data extractor/QA) and selective table generation, while current Excel extraction path used an always-on, all-sheets workflow. The newer path added richer outputs but lost selectivity and introduced repeated helper calls in deep loops.

## Bottlenecks ranked by impact

1. **Repeated pset/type lookup in COBie loop** (very high impact).
2. **No staged extraction/selection before export** (high impact on large models).
3. **Spatial/classification extraction always executed** (medium-high impact).
4. **No instrumentation to isolate stage costs** (medium operational impact).
5. **No preview endpoint for cheap model discovery** (medium UX/perf impact).

## Implemented optimisations

1. Added staged timer instrumentation (`StageTimer`) for model load/index/tables/classification/COBie/excel write.
2. Introduced `ExtractionPlan` for selective export (sheets/classes/psets/flags).
3. Added preview scan function `scan_model_for_excel_preview` and endpoint `/excel/scan`.
4. Added extraction caches for:
   - element type lookup
   - psets per element
   - psets per type
   - spatial container path per element
5. Refactored extraction into `extract_to_excel_with_plan` with targeted sheet generation.
6. Updated frontend flow to support:
   - Scan model
   - review classes/psets summary
   - select scope before export

## Trade-offs / compatibility concerns

- If users deselect sheets/fields, resulting workbook is intentionally smaller than legacy full workbook.
- Preview scan still opens IFC and walks elements once; this is expected but significantly lighter than full workbook generation.
- Cache is in-memory session-scoped metadata (not full IFC object persistence) to keep implementation safe and maintainable.

## Answers to required investigation questions

1. **What exact logic changed between older and newer flow?**
   - Newer Excel flow became always-full extraction with repeated helper lookups and no selection stage.
2. **Is slowdown IFCOpenShell itself or usage?**
   - Primarily usage pattern (repeated expensive calls and broad extraction), not IFCOpenShell alone.
3. **Which stages are avoidably expensive?**
   - COBie pset resolution loops, unconditional properties/classifications extraction, repeated spatial traversal.
4. **What can be deferred until user selection is known?**
   - Properties table, classification sheets, COBie dynamic fields, and class-scoped rows.
5. **What data should be previewed cheaply vs extracted later?**
   - Cheap preview: schema/model counts/classes/pset names/property names/quantity sets/classification system names.
   - Full extraction: sheet rows and values only for selected outputs.
6. **What cached/intermediate structures should be introduced?**
   - Type lookup cache, pset caches (occurrence + type), spatial cache, and scan metadata cache for UI planning.

