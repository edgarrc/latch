from __future__ import annotations

import sys

import pytest

from plugins.command_line import CommandLinePlugin
from plugins.base import PluginExecutionError


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

    with pytest.raises(PluginExecutionError, match="string de erro"):
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

    with pytest.raises(PluginExecutionError, match="string de sucesso"):
        collect(plugin)


def test_command_line_plugin_rejects_invalid_command_list() -> None:
    with pytest.raises(ValueError, match="non-empty strings"):
        CommandLinePlugin("invalid", {"command": ["echo", "ok", 1]})
