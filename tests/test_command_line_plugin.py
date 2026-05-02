from __future__ import annotations

import shlex
import sys
import time

import pytest

from plugins.command_line import CommandLinePlugin
from plugins.base import PluginExecutionError
from plugins.variables import prepare_command_plugin_config, validate_variable_definitions


def collect(plugin: CommandLinePlugin) -> list[str]:
    return [event.message for event in plugin.run()]


def test_command_line_plugin_succeeds_with_success_string() -> None:
    plugin = CommandLinePlugin(
        "ok",
        {
            "command": [
                sys.executable,
                "-c",
                "print('batch concluido')",
            ],
            "success_contains": "concluido",
        },
    )

    messages = collect(plugin)

    assert any("batch concluido" in message for message in messages)
    assert any("exit code 0" in message for message in messages)


def test_command_line_plugin_runs_list_command_through_pipeline() -> None:
    command = [
        sys.executable,
        "-c",
        "print('pipeline ok')",
    ]
    pipeline = (
        f"{shlex.quote(sys.executable)} -c "
        "'import sys; sys.stdout.write(sys.stdin.read().upper())'"
    )
    metadata: dict[str, object] = {}
    plugin = CommandLinePlugin(
        "pipe",
        {
            "command": command,
            "pipeline": pipeline,
        },
    )
    plugin.set_runtime_context("modulo", "run-id", metadata.update)

    messages = collect(plugin)

    full_display_command = f"{shlex.join(command)} | {pipeline}"
    assert messages[0] == f"Starting command: {full_display_command}"
    assert any("PIPELINE OK" in message for message in messages)
    assert metadata["command"] == full_display_command


def test_command_line_plugin_pipeline_uses_pipefail() -> None:
    pipeline = (
        f"{shlex.quote(sys.executable)} -c "
        "'import sys; sys.stdin.read(); sys.exit(0)'"
    )
    plugin = CommandLinePlugin(
        "pipefail",
        {
            "command": [
                sys.executable,
                "-c",
                "import sys; print('left failed'); sys.exit(7)",
            ],
            "pipeline": pipeline,
        },
    )

    with pytest.raises(PluginExecutionError, match="exit code 7"):
        collect(plugin)


def test_command_line_plugin_truncates_started_command_log() -> None:
    long_argument = "a" * 520 + "tail"
    plugin = CommandLinePlugin(
        "long_command",
        {
            "command": [
                sys.executable,
                "-c",
                "print('ok')",
                long_argument,
            ],
        },
    )

    messages = collect(plugin)
    started_message = messages[0]

    assert started_message.startswith("Starting command: ")
    assert started_message.endswith("[...]")
    command_preview = started_message.removeprefix("Starting command: ").removesuffix("[...]")
    assert len(command_preview) == 500
    assert "tail" not in started_message


def test_command_line_plugin_fails_on_non_zero_exit_code() -> None:
    plugin = CommandLinePlugin(
        "bad_exit",
        {
            "command": [
                sys.executable,
                "-c",
                "import sys; print('falhou'); sys.exit(2)",
            ],
        },
    )

    with pytest.raises(PluginExecutionError, match="exit code 2"):
        collect(plugin)


def test_command_line_plugin_fails_on_error_string() -> None:
    plugin = CommandLinePlugin(
        "bad_output",
        {
            "command": [
                sys.executable,
                "-c",
                "print('ERROR no processamento')",
            ],
            "error_contains": "ERROR",
        },
    )

    with pytest.raises(PluginExecutionError, match="error string"):
        collect(plugin)


def test_command_line_plugin_fails_when_success_string_is_missing() -> None:
    plugin = CommandLinePlugin(
        "missing_success",
        {
            "command": [
                sys.executable,
                "-c",
                "print('sem marcador esperado')",
            ],
            "success_contains": "concluido",
        },
    )

    with pytest.raises(PluginExecutionError, match="success string"):
        collect(plugin)


def test_command_line_plugin_rejects_invalid_command_list() -> None:
    with pytest.raises(ValueError, match="non-empty strings"):
        CommandLinePlugin("invalid", {"command": ["echo", "ok", 1]})


@pytest.mark.parametrize("pipeline", ["", "   ", ["cat"]])
def test_command_line_plugin_rejects_invalid_pipeline(pipeline: object) -> None:
    with pytest.raises(ValueError, match="pipeline must be a non-empty string"):
        CommandLinePlugin("invalid", {"command": "echo ok", "pipeline": pipeline})


def test_command_line_plugin_times_out_and_kills_process() -> None:
    plugin = CommandLinePlugin(
        "slow",
        {
            "command": [
                sys.executable,
                "-c",
                "import time; time.sleep(5)",
            ],
            "timeout": 1,
        },
    )

    started_at = time.monotonic()
    with pytest.raises(PluginExecutionError, match="timeout"):
        collect(plugin)

    assert time.monotonic() - started_at < 4


def test_command_line_plugin_retries_timeout_extra_attempts() -> None:
    plugin = CommandLinePlugin(
        "slow_retry",
        {
            "command": [
                sys.executable,
                "-c",
                "import time; print('attempt', flush=True); time.sleep(5)",
            ],
            "timeout": 1,
            "timeout_retries": 1,
        },
    )
    messages: list[str] = []

    with pytest.raises(PluginExecutionError, match="timeout"):
        for event in plugin.run():
            messages.append(event.message)

    assert sum(message.startswith("Starting command:") for message in messages) == 2
    assert any("retry 1/1" in message for message in messages)
    assert any("Retrying plugin" in message for message in messages)


def test_command_line_plugin_ignores_timeout_retries_without_timeout() -> None:
    plugin = CommandLinePlugin(
        "retry_without_timeout",
        {
            "command": [
                sys.executable,
                "-c",
                "print('ok')",
            ],
            "timeout_retries": 1,
        },
    )

    messages = collect(plugin)

    assert any("ok" in message for message in messages)
    assert not any("Retrying plugin" in message for message in messages)


@pytest.mark.parametrize("timeout", [0, -1, "1", 1.5, True])
def test_command_line_plugin_rejects_invalid_timeout(timeout: object) -> None:
    with pytest.raises(ValueError, match="timeout must be a positive integer"):
        CommandLinePlugin("invalid", {"command": "echo ok", "timeout": timeout})


@pytest.mark.parametrize("timeout_retries", [-1, "1", 1.5, True])
def test_command_line_plugin_rejects_invalid_timeout_retries(
    timeout_retries: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="timeout_retries must be a non-negative integer",
    ):
        CommandLinePlugin(
            "invalid",
            {"command": "echo ok", "timeout_retries": timeout_retries},
        )


def test_variable_definitions_reject_invalid_schema() -> None:
    with pytest.raises(ValueError, match="unsupported keys"):
        validate_variable_definitions(
            "teste_automatizado",
            {
                "password": {
                    "type": "sensitive",
                    "value": "secret-value",
                    "default": "fallback",
                }
            },
        )


def test_command_line_plugin_substitutes_string_variable_in_shell_command() -> None:
    config = prepare_command_plugin_config(
        {
            "command": (
                f"{sys.executable} -c \"import sys; print(sys.argv[1])\" "
                "{message}"
            )
        },
        {"message": {"type": "string", "value": "hello world"}},
    )
    plugin = CommandLinePlugin("with_string", config)

    messages = collect(plugin)

    assert any("hello world" in message for message in messages)


def test_command_line_plugin_substitutes_integer_variable_in_command_list() -> None:
    config = prepare_command_plugin_config(
        {
            "command": [
                sys.executable,
                "-c",
                "import sys; print(int(sys.argv[1]) + 1)",
                "{limit}",
            ]
        },
        {"limit": {"type": "integer", "value": "41"}},
    )
    plugin = CommandLinePlugin("with_integer", config)

    messages = collect(plugin)

    assert any("42" in message for message in messages)


def test_command_line_plugin_resolves_environment_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BATCH_MESSAGE", "from-env")
    config = prepare_command_plugin_config(
        {
            "command": [
                sys.executable,
                "-c",
                "import sys; print(sys.argv[1])",
                "{message}",
            ]
        },
        {"message": {"type": "string", "value": "$BATCH_MESSAGE"}},
    )
    plugin = CommandLinePlugin("with_env", config)

    messages = collect(plugin)

    assert any("from-env" in message for message in messages)


def test_command_line_plugin_fails_when_environment_variable_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_BATCH_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="missing environment variable"):
        prepare_command_plugin_config(
            {"command": "echo {password}"},
            {"password": {"type": "sensitive", "value": "$MISSING_BATCH_PASSWORD"}},
        )


def test_command_line_plugin_fails_on_unknown_placeholder() -> None:
    with pytest.raises(ValueError, match="no configured variable"):
        prepare_command_plugin_config(
            {"command": "echo {missing}"},
            {"message": {"type": "string", "value": "ok"}},
        )


def test_command_line_plugin_fails_on_invalid_integer_variable() -> None:
    with pytest.raises(ValueError, match="must be an integer"):
        prepare_command_plugin_config(
            {"command": "echo {limit}"},
            {"limit": {"type": "integer", "value": "ten"}},
        )


def test_command_line_plugin_masks_sensitive_values_in_logs_and_metadata() -> None:
    metadata: dict[str, object] = {}
    config = prepare_command_plugin_config(
        {
            "command": [
                sys.executable,
                "-c",
                "import sys; print(sys.argv[1])",
                "{password}",
            ]
        },
        {"password": {"type": "sensitive", "value": "secret-value"}},
    )
    plugin = CommandLinePlugin("with_secret", config)
    plugin.set_runtime_context("teste_automatizado", "run-id", metadata.update)

    messages = collect(plugin)

    assert any("****" in message for message in messages)
    assert all("secret-value" not in message for message in messages)
    assert "secret-value" not in str(metadata)
    assert "****" in str(metadata)


def test_command_line_plugin_masks_sensitive_values_in_pipeline() -> None:
    metadata: dict[str, object] = {}
    pipeline = (
        f"{shlex.quote(sys.executable)} -c "
        "'import sys; print(sys.argv[1]); sys.stdin.read()' {password}"
    )
    config = prepare_command_plugin_config(
        {
            "command": "printf ready",
            "pipeline": pipeline,
        },
        {"password": {"type": "sensitive", "value": "secret-value"}},
    )
    plugin = CommandLinePlugin("with_secret_pipeline", config)
    plugin.set_runtime_context("teste_automatizado", "run-id", metadata.update)

    messages = collect(plugin)

    serialized = "\n".join(messages) + repr(metadata)
    assert "secret-value" not in serialized
    assert "****" in serialized
