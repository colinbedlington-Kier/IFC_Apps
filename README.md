---
title: IFC Toolkit Hub
emoji: 🧭
colorFrom: blue
colorTo: purple
sdk: docker
app_file: app.py
pinned: false
license: unknown
short_description: Unified FastAPI-powered IFC toolkit with multi-tool navigation.
---

A single Hugging Face Space that hosts multiple IFC utilities behind SEI-inspired pages. Upload IFC or Excel files once per session, run any of the tools, and download results. The server drops temporary session data when you close the page or after it expires.

## Tools (each on its own page)
- **Uploads**: add IFC/Excel files once per session and download generated artifacts.
- **Cleaner**: remove InfoDrainage-prefixed property sets and properties.
- **Excel Sync**: extract IFC data to Excel and apply workbook changes back into IFC.
- **Global Z**: adjust storey elevations, BaseQuantities, and MapConversions while counter-shifting placements.
- **Type Mapper**: retype IFCBUILDINGELEMENTPROXY instances and proxy types to discipline-specific IFC2x3 entities.
- **Levels**: list, create, move, and delete storeys; move elements across levels while keeping placements intact.

## Deployment and auto-update
- The Space runs via FastAPI + Uvicorn (see `Dockerfile`).
- A GitHub Actions workflow (`.github/workflows/hf-sync.yml`) uploads the repository to a Hugging Face Space on every push to `main` (or manually via workflow dispatch). Set repository secrets `HF_TOKEN` (a write token) and `HF_SPACE` (e.g., `org/space-name`) to enable the sync.
