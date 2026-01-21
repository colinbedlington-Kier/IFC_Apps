from pathlib import Path

from step2ifc.config import ConversionConfig


def test_yaml_parsing(tmp_path: Path):
    content = """
name_normalization:
  - pattern: "\\s+"
    replacement: " "
type_mappings:
  - match_name_regex: ".*VALVE.*"
    ifc_class: "IfcValve"
properties:
  defaults:
    Zone: "Z01"
"""
    config_path = tmp_path / "classmap.yaml"
    config_path.write_text(content, encoding="utf-8")

    config = ConversionConfig.load(config_path)
    assert config.name_normalization
    assert config.type_mappings[0].ifc_class == "IfcValve"
    assert config.metadata_defaults["Zone"] == "Z01"
