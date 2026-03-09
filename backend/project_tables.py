import json
import os
from typing import Any, Dict, List, Optional


def _normalize_tables(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def load_project_tables_by_slug(config_raw: Optional[str] = None) -> Dict[str, List[str]]:
    raw = config_raw if config_raw is not None else os.getenv("PROJECT_DATABASES_JSON", "")
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    mapping: Dict[str, List[str]] = {}
    if isinstance(parsed, dict):
        for slug, cfg in parsed.items():
            if not isinstance(slug, str) or not slug.strip():
                continue
            tables: List[str] = []
            if isinstance(cfg, dict):
                tables = _normalize_tables(cfg.get("tables"))
                if not tables:
                    tables = _normalize_tables(cfg.get("table") or cfg.get("sql_table"))
            if tables:
                mapping[slug.strip()] = tables
    elif isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            slug = str(item.get("slug") or "").strip()
            if not slug:
                continue
            tables = _normalize_tables(item.get("tables"))
            if not tables:
                tables = _normalize_tables(item.get("table") or item.get("sql_table"))
            if tables:
                mapping[slug] = tables

    return mapping


def get_tables_for_project_slug(project_slug: Optional[str], config_raw: Optional[str] = None) -> Optional[List[str]]:
    slug = (project_slug or "").strip()
    if not slug:
        return None
    return load_project_tables_by_slug(config_raw=config_raw).get(slug)

