# Civil 3D extended Excel round-trip profile

This repository now supports a `civil3d_extended` extraction profile for the IFC → Excel → IFC workflow.

## What it does

- Adds a profile-driven extraction mode (`plan_payload.profile = "civil3d_extended"`).
- Surfaces Civil 3D / DfE fields directly in `Elements` and `COBieMapping`, including:
  - `ExtObject`
  - `IFCPresentationLayer`
  - `ClassificationCode`
  - `IFC_Enumeration`
  - `SystemName`, `SystemDescription`, `SystemCategory`
  - `Type (User Defined)`
  - Uniclass-related fields
- Adds `ProjectNumber` in `ProjectData` and maps it to project metadata (`IfcProject.LongName`).

## Mapping priority rules

The extractor now applies explicit source priority:

1. General names:
   - `IFC Name`
   - `Name`
   - legacy type/block fallback
2. `IFCElementType.Name`:
   - `IFC Name`
   - `Type (User Defined)`
   - `Name`
   - legacy type/block fallback
3. Class mapping candidate diagnostics:
   - `ExtObject`
   - `IFC_Enumeration`
   - existing occurrence/object type value
   - fallback `IfcBuildingElementProxy`

## Validation and diagnostics

- Pre-import validation runs before Excel updates and fails fast on key data issues (for example missing `ProjectNumber` or missing IDs).
- Logging has been added around:
  - name/type priority source selection,
  - class mapping candidate decisions,
  - project metadata write operations,
  - Civil 3D field write-back failures.

## Known write-back limitation

`IFCPresentationLayer` write-back depends on representation item availability in each element/type representation graph. When there are no writable representation items, the layer is logged but cannot be reapplied.
