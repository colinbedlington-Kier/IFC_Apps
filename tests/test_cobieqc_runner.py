from cobieqc_service import runner


def test_effective_jvm_args_include_safe_defaults(monkeypatch):
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_OPTS", raising=False)
    monkeypatch.setenv("COBIEQC_JAVA_XMS", "256m")
    monkeypatch.setenv("COBIEQC_JAVA_XMX", "512m")
    monkeypatch.setenv("COBIEQC_JAVA_MAX_RAM_PERCENT", "50")
    monkeypatch.setenv("COBIEQC_JAVA_CONTAINER_FLAGS", "-XX:+UseContainerSupport")

    args = runner._effective_jvm_args()

    assert "-Xms256m" in args
    assert "-Xmx512m" in args
    assert "-XX:MaxRAMPercentage=50" in args
    assert "-XX:+UseContainerSupport" in args
    assert "-XX:+PrintGCDetails" in args
    assert "-XX:+PrintGCDateStamps" in args
    assert "-XX:+HeapDumpOnOutOfMemoryError" in args


def test_java_tool_options_preferred_over_java_opts(monkeypatch):
    monkeypatch.setenv("JAVA_TOOL_OPTIONS", "-Xmx384m")
    monkeypatch.setenv("JAVA_OPTS", "-Xmx768m")

    args = runner._effective_jvm_args()

    assert "-Xmx384m" in args
    assert "-Xmx768m" not in args


def test_build_cmd_always_has_explicit_heap_flags(monkeypatch, tmp_path):
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_OPTS", raising=False)

    cmd = runner._build_cobieqc_cmd(
        java_bin="java",
        jar_path=tmp_path / "CobieQcReporter.jar",
        input_xlsx_path=tmp_path / "input.xlsx",
        output_html_path=tmp_path / "report.html",
        stage="D",
    )

    assert cmd[0] == "java"
    assert any(part.startswith("-Xms") for part in cmd)
    assert any(part.startswith("-Xmx") for part in cmd)
    assert "-jar" in cmd
