from cobieqc_service import runner


def test_effective_jvm_args_include_fixed_heap_defaults(monkeypatch):
    monkeypatch.delenv("COBIEQC_JAVA_XMS", raising=False)
    monkeypatch.delenv("COBIEQC_JAVA_XMX", raising=False)

    args, xms, xmx = runner._effective_jvm_args()

    assert xms == "128m"
    assert xmx == "512m"
    assert "-Xms128m" in args
    assert "-Xmx512m" in args
    assert "-XX:+UseContainerSupport" in args
    assert "-XX:+PrintGCDetails" in args
    assert "-XX:+PrintGCDateStamps" in args
    assert "-XX:+HeapDumpOnOutOfMemoryError" in args
    assert not any(arg.startswith("-XX:MaxRAMPercentage") for arg in args)


def test_effective_jvm_args_honors_subprocess_env_overrides(monkeypatch):
    monkeypatch.setenv("COBIEQC_JAVA_XMS", "192m")
    monkeypatch.setenv("COBIEQC_JAVA_XMX", "640m")

    args, _, _ = runner._effective_jvm_args()

    assert "-Xms192m" in args
    assert "-Xmx640m" in args


def test_effective_jvm_args_rejects_xms_greater_than_xmx(monkeypatch):
    monkeypatch.setenv("COBIEQC_JAVA_XMS", "768m")
    monkeypatch.setenv("COBIEQC_JAVA_XMX", "512m")

    try:
        runner._effective_jvm_args()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "must be <=" in str(exc)


def test_build_cmd_always_has_explicit_heap_flags(monkeypatch, tmp_path):
    monkeypatch.setenv("COBIEQC_JAVA_XMS", "128m")
    monkeypatch.setenv("COBIEQC_JAVA_XMX", "512m")

    cmd = runner._build_cobieqc_cmd(
        java_bin="java",
        jar_path=tmp_path / "CobieQcReporter.jar",
        input_xlsx_path=tmp_path / "input.xlsx",
        output_html_path=tmp_path / "report.html",
        stage="D",
    )

    assert cmd[0] == "java"
    assert "-Xms128m" in cmd
    assert "-Xmx512m" in cmd
    assert "-XX:+UseContainerSupport" in cmd
    assert "-jar" in cmd
