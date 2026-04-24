from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

LOGGER = logging.getLogger("ifc_app.area_spaces")


def _error_payload(error: str, message: str, stage: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "message": message,
        "stage": stage,
    }


def build_area_spaces_router(
    *,
    scan_handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any] | JSONResponse]],
    purge_handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any] | JSONResponse]],
    files_handler: Callable[[str], Dict[str, Any] | JSONResponse],
) -> APIRouter:
    router = APIRouter(tags=["area-spaces"])

    @router.get("/api/ifc/area-spaces/session-files")
    def area_spaces_session_files(session_id: str):
        try:
            return files_handler(session_id)
        except Exception as exc:
            LOGGER.exception("area_spaces_session_files_failed")
            return JSONResponse(status_code=500, content=_error_payload("AREA_SPACE_SCAN_FAILED", str(exc), "session_files"))

    @router.post("/api/ifc/area-spaces/scan")
    async def area_spaces_scan(payload: Dict[str, Any] = Body(...)):
        try:
            return await scan_handler(payload)
        except Exception as exc:
            LOGGER.exception("area_spaces_scan_failed")
            return JSONResponse(status_code=500, content=_error_payload("AREA_SPACE_SCAN_FAILED", str(exc), "scan_spaces"))

    @router.post("/api/ifc/area-spaces/purge")
    async def area_spaces_purge(payload: Dict[str, Any] = Body(...)):
        try:
            return await purge_handler(payload)
        except Exception as exc:
            LOGGER.exception("area_spaces_purge_failed")
            return JSONResponse(status_code=500, content=_error_payload("AREA_SPACE_PURGE_FAILED", str(exc), "purge"))

    return router
