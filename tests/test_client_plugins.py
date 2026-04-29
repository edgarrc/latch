from __future__ import annotations

import stat
import sys
import threading
import time
from pathlib import Path

import pytest

import plugins.clickhouse_client as clickhouse_client_module
import plugins.redis_client as redis_client_module
from plugins.base import PluginExecutionError, PluginKilledError
from plugins.clickhouse_client import ClickHouseClientPlugin
from plugins.redis_client import RedisClientPlugin
from plugins.variables import prepare_plugin_config


def collect(plugin) -> list[str]:
    return [event.message for event in plugin.run()]


def write_fake_client(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake-client"
    script_path.write_text(
        (
            f"#!{sys.executable}\n"
            "import sys\n"
            "import time\n"
            "\n"
            "if '--sleep' in sys.argv:\n"
            "    time.sleep(30)\n"
            "if '--exit-2' in sys.argv:\n"
            "    print('falhou')\n"
            "    raise SystemExit(2)\n"
            "for arg in sys.argv[1:]:\n"
            "    print(arg)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR)
    return script_path


def test_clickhouse_client_builds_fixed_argv() -> None:
    plugin = ClickHouseClientPlugin(
        "consultar",
        {
            "type": "clickhouse_client",
            "user": "analytics",
            "password": "secret",
            "database": "default",
            "query": "SELECT COUNT(*) FROM eventos",
        },
    )

    assert plugin.command == [
        "/usr/bin/clickhouse-client",
        "--user",
        "analytics",
        "--password",
        "secret",
        "--database",
        "default",
        "--query",
        "SELECT COUNT(*) FROM eventos",
    ]
    assert "secret" not in plugin.display_command
    assert "****" in plugin.display_command


def test_clickhouse_client_masks_password_in_logs_and_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_client = write_fake_client(tmp_path)
    monkeypatch.setattr(
        clickhouse_client_module,
        "CLICKHOUSE_CLIENT_BINARY",
        str(fake_client),
    )
    metadata: dict[str, object] = {}
    plugin = ClickHouseClientPlugin(
        "consultar",
        {
            "type": "clickhouse_client",
            "password": "secret-value",
            "query": "SELECT 1",
        },
    )
    plugin.set_runtime_context("modulo", "run-id", metadata.update)

    messages = collect(plugin)

    serialized = "\n".join(messages) + repr(metadata)
    assert "secret-value" not in serialized
    assert "****" in serialized
    assert "command" in metadata


def test_redis_client_builds_argv_from_list_and_string() -> None:
    list_plugin = RedisClientPlugin(
        "scan_lista",
        {
            "type": "redis_client",
            "host": "redis.local",
            "args": ["--scan", "--pattern", "exp_*"],
        },
    )
    string_plugin = RedisClientPlugin(
        "scan_string",
        {
            "type": "redis_client",
            "host": "redis.local",
            "args": "--scan --pattern 'exp *'",
        },
    )

    assert list_plugin.command == [
        "/usr/bin/redis-cli",
        "-h",
        "redis.local",
        "--scan",
        "--pattern",
        "exp_*",
    ]
    assert string_plugin.command == [
        "/usr/bin/redis-cli",
        "-h",
        "redis.local",
        "--scan",
        "--pattern",
        "exp *",
    ]


def test_client_plugins_preserve_command_line_output_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_client = write_fake_client(tmp_path)
    monkeypatch.setattr(redis_client_module, "REDIS_CLI_BINARY", str(fake_client))

    with pytest.raises(PluginExecutionError, match="string de erro"):
        collect(
            RedisClientPlugin(
                "erro_texto",
                {
                    "type": "redis_client",
                    "args": ["ERROR"],
                    "error_contains": "ERROR",
                },
            )
        )

    with pytest.raises(PluginExecutionError, match="exit code 2"):
        collect(
            RedisClientPlugin(
                "erro_saida",
                {
                    "type": "redis_client",
                    "args": ["--exit-2"],
                },
            )
        )

    with pytest.raises(PluginExecutionError, match="string de sucesso"):
        collect(
            RedisClientPlugin(
                "sem_sucesso",
                {
                    "type": "redis_client",
                    "args": ["ok"],
                    "success_contains": "concluido",
                },
            )
        )


def test_client_plugins_replace_variables_in_structured_fields() -> None:
    variables = {
        "clickhouse_user": {"type": "string", "value": "analytics"},
        "clickhouse_password": {"type": "sensitive", "value": "secret-value"},
        "clickhouse_database": {"type": "string", "value": "default"},
        "limite": {"type": "integer", "value": "10"},
        "redis_host": {"type": "string", "value": "redis.local"},
        "pattern": {"type": "string", "value": "exp_*"},
    }

    clickhouse_config = prepare_plugin_config(
        {
            "id": "consultar",
            "type": "clickhouse_client",
            "user": "{clickhouse_user}",
            "password": "{clickhouse_password}",
            "database": "{clickhouse_database}",
            "query": "SELECT * FROM eventos LIMIT {limite}",
        },
        variables,
    )
    clickhouse_plugin = ClickHouseClientPlugin("consultar", clickhouse_config)
    redis_config = prepare_plugin_config(
        {
            "id": "scan",
            "type": "redis_client",
            "host": "{redis_host}",
            "args": "--scan --pattern {pattern}",
        },
        variables,
    )
    redis_plugin = RedisClientPlugin("scan", redis_config)

    assert clickhouse_plugin.command[-1] == "SELECT * FROM eventos LIMIT 10"
    assert "secret-value" in clickhouse_plugin.command
    assert "secret-value" not in clickhouse_plugin.display_command
    assert "****" in clickhouse_plugin.display_command
    assert redis_plugin.command == [
        "/usr/bin/redis-cli",
        "-h",
        "redis.local",
        "--scan",
        "--pattern",
        "exp_*",
    ]


def test_client_plugins_reject_invalid_required_fields() -> None:
    with pytest.raises(ValueError, match="non-empty query"):
        ClickHouseClientPlugin("sem_query", {"type": "clickhouse_client"})

    with pytest.raises(ValueError, match="non-empty string or list"):
        RedisClientPlugin("sem_args", {"type": "redis_client"})

    with pytest.raises(ValueError, match="host must be a string"):
        RedisClientPlugin(
            "host_invalido",
            {"type": "redis_client", "host": 1, "args": ["--scan"]},
        )

    with pytest.raises(ValueError, match="non-empty strings"):
        RedisClientPlugin(
            "args_invalidos",
            {"type": "redis_client", "args": ["--scan", ""]},
        )


def test_redis_client_kill_uses_inherited_process_group_behavior(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_client = write_fake_client(tmp_path)
    monkeypatch.setattr(redis_client_module, "REDIS_CLI_BINARY", str(fake_client))
    metadata: dict[str, object] = {}
    plugin = RedisClientPlugin("sleep", {"type": "redis_client", "args": ["--sleep"]})
    plugin.set_runtime_context("modulo", "run-id", metadata.update)
    result: dict[str, object] = {"messages": [], "error": None}

    def run_plugin() -> None:
        try:
            result["messages"] = collect(plugin)
        except Exception as exc:  # noqa: BLE001 - thread result is asserted below.
            result["error"] = exc

    thread = threading.Thread(target=run_plugin)
    thread.start()
    deadline = time.monotonic() + 5
    while "pgid" not in metadata and time.monotonic() < deadline:
        time.sleep(0.01)

    try:
        plugin.kill()
    finally:
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert isinstance(result["error"], PluginKilledError)
    assert metadata["kill_requested"] is True
    assert str(metadata["kill_command"]).startswith("kill -KILL -")


def test_clickhouse_client_kill_uses_inherited_process_group_behavior(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_client = write_fake_client(tmp_path)
    monkeypatch.setattr(
        clickhouse_client_module,
        "CLICKHOUSE_CLIENT_BINARY",
        str(fake_client),
    )
    metadata: dict[str, object] = {}
    plugin = ClickHouseClientPlugin(
        "sleep",
        {"type": "clickhouse_client", "query": "SELECT 1", "user": "--sleep"},
    )
    plugin.set_runtime_context("modulo", "run-id", metadata.update)
    result: dict[str, object] = {"messages": [], "error": None}

    def run_plugin() -> None:
        try:
            result["messages"] = collect(plugin)
        except Exception as exc:  # noqa: BLE001 - thread result is asserted below.
            result["error"] = exc

    thread = threading.Thread(target=run_plugin)
    thread.start()
    deadline = time.monotonic() + 5
    while "pgid" not in metadata and time.monotonic() < deadline:
        time.sleep(0.01)

    try:
        plugin.kill()
    finally:
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert isinstance(result["error"], PluginKilledError)
    assert metadata["kill_requested"] is True
    assert str(metadata["kill_command"]).startswith("kill -KILL -")
