import json
from pathlib import Path
from typing import Dict, Tuple

MAPPINGS_PATH = Path("config/check_field_mappings.json")
EXPRESSIONS_PATH = Path("config/check_expressions.json")


def _load_json(path: Path) -> Dict:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_mapping_config() -> Dict:
    return _load_json(MAPPINGS_PATH)


def load_expression_config() -> Dict:
    return _load_json(EXPRESSIONS_PATH)


def save_mapping_for_check(check_id: str, payload: Dict) -> Dict:
    data = load_mapping_config()
    by_id = data.setdefault("by_check_id", {})
    by_id[check_id] = payload
    with open(MAPPINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return data


def save_expression_for_check(check_id: str, expression: str) -> Dict:
    data = load_expression_config()
    by_id = data.setdefault("by_check_id", {})
    by_id[check_id] = expression
    with open(EXPRESSIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return data
