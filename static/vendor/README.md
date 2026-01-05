This directory hosts vendored front-end dependencies for the IFC viewer to avoid CDN reliance in production deployments.

Expected contents:

- `web-ifc-viewer/IFCViewerAPI.js` (build from `web-ifc-viewer@1.0.172`).
- `three/three.module.js` (build from `three@0.164.1`).
- `web-ifc/` WASM + workers (from `web-ifc@0.0.50`), typically including `web-ifc.wasm`, `web-ifc-worker.js`, and supporting files.

Populate these files before building production artifacts so the viewer can initialize without internet access.
