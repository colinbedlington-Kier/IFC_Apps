from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


class JsonlHandler(logging.Handler):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")


_LOGGER: Optional[logging.Logger] = None


def configure_logging(log_path: Optional[Path] = None) -> logging.Logger:
    logger = logging.getLogger("step2ifc")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(console)

    if log_path:
        logger.addHandler(JsonlHandler(log_path))
    return logger


def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = configure_logging()
    return _LOGGER


def log_event(logger: logging.Logger, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
    record = logging.LogRecord(
        name=logger.name,
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.extra = payload or {}
    logger.handle(record)


def log_inference(logger: logging.Logger, message: str, confidence: float, payload: Optional[Dict[str, Any]] = None) -> None:
    data = {"confidence": confidence}
    if payload:
        data.update(payload)
    log_event(logger, message, data)
