from pathlib import Path
from typing import Any, Dict, List, Tuple

from ifc_qa_service import start_session_job


def start_ifc_qa_session_job(
    session_root: Path,
    session_id: str,
    file_records: List[Tuple[str, str]],
    options: Dict[str, Any],
    config: Dict[str, Any],
    mode: str,
) -> str:
    return start_session_job(session_root, session_id, file_records, options, config, mode)

