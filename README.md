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
