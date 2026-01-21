from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple
import re


NOISE_TOKENS = {
    "REV",
    "R",
    "V",
    "V1",
    "V2",
    "COPY",
    "ASSY",
    "ASSEMBLY",
    "PART",
    "ITEM",
}

DOMAIN_TOKENS = {
    "VALVE",
    "DUCT",
    "PIPE",
    "ELBOW",
    "TEE",
    "COUPLING",
    "FLANGE",
    "DIFFUSER",
    "GRILLE",
    "BOLT",
    "NUT",
    "WASHER",
    "SCREW",
    "PLATE",
    "PANEL",
    "SHEET",
    "BEAM",
    "MEMBER",
}


@dataclass
class InferenceResult:
    ifc_class: str
    object_type: str
    confidence: float
    candidates: List[Dict[str, float]]
    geometry_archetype: str


def normalize_tokens(name: str, assembly_path: str) -> List[str]:
    combined = f"{name} {assembly_path}".upper()
    combined = re.sub(r"[^A-Z0-9]+", " ", combined)
    tokens = [token for token in combined.split() if token and token not in NOISE_TOKENS]
    return tokens


def geometry_archetype(bbox: Tuple[float, float, float, float, float, float]) -> str:
    xmin, ymin, zmin, xmax, ymax, zmax = bbox
    dims = sorted([abs(xmax - xmin), abs(ymax - ymin), abs(zmax - zmin)])
    if dims[2] == 0:
        return "unknown"
    thin_ratio = dims[0] / dims[2]
    if thin_ratio < 0.05:
        return "plate_like"
    if dims[1] > 0 and dims[2] / dims[1] > 4 and dims[1] / dims[0] < 1.3:
        return "pipe_like"
    if dims[2] / max(dims[0], 1e-6) > 8:
        return "member_like"
    if dims[2] < 50 and dims[1] < 20:
        return "fastener_like"
    return "unknown"


def infer_class(tokens: List[str], archetype: str) -> InferenceResult:
    scores: Dict[str, float] = {}
    def bump(key: str, value: float) -> None:
        scores[key] = scores.get(key, 0.0) + value

    token_set = set(tokens)
    if "VALVE" in token_set:
        bump("IfcValve", 0.9)
    if token_set.intersection({"DIFFUSER", "GRILLE"}):
        bump("IfcAirTerminal", 0.8)
    if "DUCT" in token_set or "AHU" in token_set:
        bump("IfcDuctSegment", 0.7)
    if token_set.intersection({"PIPE", "ELBOW", "TEE", "COUPLING"}):
        bump("IfcPipeSegment", 0.6)
    if token_set.intersection({"ELBOW", "TEE", "COUPLING"}):
        bump("IfcPipeFitting", 0.8)
    if token_set.intersection({"BOLT", "NUT", "WASHER", "SCREW"}):
        bump("IfcMechanicalFastener", 0.8)
    if token_set.intersection({"PLATE", "PANEL", "SHEET"}):
        bump("IfcPlate", 0.6)
    if token_set.intersection({"BEAM", "MEMBER"}):
        bump("IfcMember", 0.6)

    if archetype == "pipe_like":
        bump("IfcPipeSegment", 0.3)
        bump("IfcPipeFitting", 0.2)
    if archetype == "plate_like":
        bump("IfcPlate", 0.3)
    if archetype == "member_like":
        bump("IfcMember", 0.3)
    if archetype == "fastener_like":
        bump("IfcMechanicalFastener", 0.3)

    if not scores:
        scores["IfcBuildingElementProxy"] = 0.4

    sorted_candidates = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_class, top_score = sorted_candidates[0]
    confidence = min(1.0, top_score)
    if confidence < 0.6:
        top_class = "IfcBuildingElementProxy"
    candidates = [
        {"ifc_class": cls, "score": round(score, 3)} for cls, score in sorted_candidates[:3]
    ]
    object_type = top_class.replace("Ifc", "")
    return InferenceResult(
        ifc_class=top_class,
        object_type=object_type,
        confidence=confidence,
        candidates=candidates,
        geometry_archetype=archetype,
    )


def token_signature(tokens: List[str]) -> str:
    domain = sorted({token for token in tokens if token in DOMAIN_TOKENS})
    if not domain:
        return "UNSPECIFIED"
    return "_".join(domain)
