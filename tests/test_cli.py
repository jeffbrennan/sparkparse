import importlib.metadata
import json
from pathlib import Path

from typer.testing import CliRunner

from sparkparse.app import app

runner = CliRunner()
FULL_LOGS = Path(__file__).parent / "data" / "full_logs"
LOG_FILE = "nested_loop_join"


def test_version_matches_installed_metadata():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert importlib.metadata.version("sparkparse") in result.stdout


def test_logs_lists_sources():
    result = runner.invoke(app, ["logs", str(FULL_LOGS)])
    assert result.exit_code == 0
    assert LOG_FILE in result.stdout


def test_log_dir_option_alias_matches_positional():
    positional = runner.invoke(app, ["logs", str(FULL_LOGS)])
    aliased = runner.invoke(app, ["logs", "--log-dir", str(FULL_LOGS)])
    assert aliased.exit_code == 0
    assert positional.stdout == aliased.stdout


def test_missing_log_dir_is_a_clear_error():
    result = runner.invoke(app, ["logs", "/definitely/not/a/real/path"])
    assert result.exit_code == 2
    assert "does not exist" in result.output


def test_get_writes_reopenable_artifact(tmp_path):
    artifact = tmp_path / "artifact"
    result = runner.invoke(
        app,
        [
            "get",
            str(FULL_LOGS),
            "--log-file",
            LOG_FILE,
            "--out-dir",
            str(tmp_path / "out"),
            "--out-format",
            "json",
            "--artifact",
            str(artifact),
        ],
    )
    assert result.exit_code == 0, result.output

    from sparkparse.artifact import load_capture_artifact

    loaded = load_capture_artifact(artifact)
    assert not loaded.dag.is_empty()


def test_analyze_stdout_is_pure_json():
    result = runner.invoke(app, ["analyze", str(FULL_LOGS), "--log-file", LOG_FILE])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["log_name"] == LOG_FILE
    assert "queries" in payload


def test_double_log_dir_sources_are_rejected():
    result = runner.invoke(app, ["logs", str(FULL_LOGS), "--log-dir", str(FULL_LOGS)])
    assert result.exit_code == 2
