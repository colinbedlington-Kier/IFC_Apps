from pathlib import Path

import app
from cobieqc_service import runner

REQUIRED_FILES = [
    "SpaceReport.css",
    "iso_svrl_for_xslt2.xsl",
    "COBieExcelTemplate.xml",
    "COBieRules.sch",
    "iso_schematron_skeleton_for_saxon.xsl",
    "SVRL_HTML_altLocation.xslt",
    "COBieRules_Functions.xsl",
    "_SVRL_HTML_altLocation.xslt",
]


def test_health_endpoint_returns_200_and_status_ok():
    health_route = next(route for route in app.app.routes if getattr(route, "path", None) == "/health")
    assert health_route.status_code in (None, 200)
    payload = app.health()
    assert payload['status'] == 'ok'
    assert payload['service'] == 'ifc-tools'
    assert 'cobieqc' in payload
    assert 'jar_ready' in payload['cobieqc']
    assert 'resources_ready' in payload['cobieqc']
    assert 'missing_files' in payload['cobieqc']


def test_resolve_server_host_port_uses_env_port(monkeypatch):
    monkeypatch.setenv('PORT', '43123')
    monkeypatch.setenv('HOST', '0.0.0.0')
    host, port = app.resolve_server_host_port()
    assert host == '0.0.0.0'
    assert port == 43123


def test_cobieqc_runtime_degrades_when_assets_missing(monkeypatch, tmp_path):
    missing_root = tmp_path / 'missing-assets'
    monkeypatch.setenv('COBIEQC_DATA_DIR', str(missing_root))
    monkeypatch.setenv('COBIEQC_JAR_PATH', str(missing_root / 'CobieQcReporter.jar'))
    monkeypatch.setenv('COBIEQC_RESOURCE_DIR', str(missing_root / 'xsl_xml'))

    diag = runner.get_cobieqc_runtime_diagnostics()
    assert diag['enabled'] is False
    assert diag['jar_exists'] is False
    assert diag['resource_dir_exists'] is False


def test_cobieqc_runtime_uses_env_specified_paths(monkeypatch, tmp_path):
    cobie_root = tmp_path / 'cobieqc'
    jar_path = cobie_root / 'CobieQcReporter.jar'
    resource_dir = cobie_root / 'xsl_xml'
    resource_dir.mkdir(parents=True)
    jar_path.write_bytes(b'jar-binary-placeholder')
    for filename in REQUIRED_FILES:
        (resource_dir / filename).write_text('x', encoding='utf-8')

    monkeypatch.setenv('COBIEQC_JAR_PATH', str(jar_path))
    monkeypatch.setenv('COBIEQC_RESOURCE_DIR', str(resource_dir))

    diag = runner.get_cobieqc_runtime_diagnostics()
    assert diag['enabled'] is True
    assert diag['jar_exists'] is True
    assert Path(diag['jar_path']) == jar_path.resolve()
    assert diag['resource_dir_exists'] is True
    assert Path(diag['resource_dir']) == resource_dir.resolve()
