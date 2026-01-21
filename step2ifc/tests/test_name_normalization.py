from step2ifc.config import ConversionConfig, NameNormalizationRule
from step2ifc.mapping import MappingEngine


def test_name_normalization_rules():
    config = ConversionConfig(
        name_normalization=[
            NameNormalizationRule(pattern="\\s+", replacement=" "),
            NameNormalizationRule(pattern="^ASM_", replacement=""),
        ]
    )
    mapping = MappingEngine(config)
    assert mapping.normalize_name("ASM_  Part  01") == "Part 01"
