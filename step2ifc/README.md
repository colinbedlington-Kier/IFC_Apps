# STEP → IFC Conversion Toolkit

Production-ready conversion workflow for STEP files into IFC (default IFC4), implemented as a reusable Python library and CLI.

## Features
- STEP ingest via `pythonocc-core` with XDE assembly traversal where available.
- Deterministic GUIDs derived from source hash + assembly path + part name.
- Configurable mapping rules (regex/assembly prefix), classification, and property sets.
- Geometry validation/healing, bounding box/volume/centroid metrics.
- IFC authoring with hierarchical Project → Site → Building → Storey structure.
- QC report output in JSON and human-readable text.
- Structured JSONL audit logging.

## Installation

```bash
pip install -e .
```

> **Dependencies**: Requires Python 3.11+, `pythonocc-core` and `ifcopenshell`. Optional validation uses `ifcopenshell[validation]`.

## CLI Usage

```bash
step2ifc convert \
  --in "input.step" \
  --out "output.ifc" \
  --schema IFC4 \
  --units mm \
  --project "MyProject" \
  --site "Site" \
  --building "Building A" \
  --storey "Level 00" \
  --geom brep \
  --mesh-deflection 0.5 \
  --mesh-angle 0.5 \
  --default-type IfcBuildingElementProxy \
  --merge-by-name \
  --split-by-assembly \
  --class-map classmap.yaml
```

## Sample Workflow
1. **Ingest**: `StepReader` loads STEP and discovers assemblies/parts using XDE.
2. **Validate**: `GeometryProcessor` validates and repairs shapes.
3. **Mapping**: `MappingEngine` maps names/assemblies to IFC types and property sets.
4. **Conversion**: `IfcWriter` creates IFC project hierarchy and elements.
5. **Enrichment**: Pset_Source + mapped Psets + classification assignments.
6. **QC**: JSON and text QC report with volumes, bounding boxes, failures, and validation.

## Configuration (YAML)
See `classmap.yaml` for a full example. Key sections:
- `name_normalization`: regex replace rules for consistent names.
- `type_mappings`: mapping by name regex or assembly prefix.
- `properties.defaults`: default values used by computed properties.

### Computed Property Example
```yaml
properties:
  defaults:
    Zone: "Z01"

type_mappings:
  - match_name_regex: ".*VALVE.*"
    ifc_class: "IfcValve"
    properties:
      - name: "System"
        value: "${ProjectKey}-${Zone}"
```

## QC Output
Running conversion produces:
- `output.ifc`
- `output.qc.json`
- `output.qc.txt`
- `output.log.jsonl`

## Library Example
```python
from pathlib import Path
from step2ifc.config import ConversionConfig
from step2ifc.mapping import MappingEngine
from step2ifc.io_step import StepReader
from step2ifc.geometry import GeometryProcessor
from step2ifc.ifc_writer import IfcWriter

config = ConversionConfig()
reader = StepReader()
parts = reader.read(Path("input.step"))
```

## Troubleshooting
- **Missing pythonocc-core**: install from conda or wheel packages for your OS.
- **Invalid solids**: review QC report for repaired/invalid parts; they are skipped or meshed.
- **IFC validation**: install `ifcopenshell[validation]` for deeper checks.
