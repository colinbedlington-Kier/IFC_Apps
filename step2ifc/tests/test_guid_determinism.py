from pathlib import Path

from step2ifc.config import ConversionConfig
from step2ifc.mapping import MappingEngine, PartContext
from step2ifc.io_step import StepPart


def test_guid_seed_deterministic():
    config = ConversionConfig()
    mapping = MappingEngine(config)
    part = StepPart(name="Valve-01", label_path="Assembly/Valve-01", shape=object())
    context = PartContext(
        part=part,
        source_hash="abc123",
        assembly_path=part.label_path,
        project_key="Project",
        metadata={},
    )
    seed_one = mapping.stable_guid_seed(context)
    seed_two = mapping.stable_guid_seed(context)
    assert seed_one == seed_two
