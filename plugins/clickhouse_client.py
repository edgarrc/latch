from __future__ import annotations

import shlex
from typing import Any

from .command_line import CommandLinePlugin
from .variables import mask_sensitive_text


CLICKHOUSE_CLIENT_BINARY = "/usr/bin/clickhouse-client"


class ClickHouseClientPlugin(CommandLinePlugin):
    plugin_type = "clickhouse_client"

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        query = config.get("query")
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"Plugin {plugin_id!r} must define a non-empty query.")

        command = [CLICKHOUSE_CLIENT_BINARY]
        self._append_optional_arg(command, config, "user", "--user", plugin_id)
        self._append_optional_arg(command, config, "password", "--password", plugin_id)
        self._append_optional_arg(command, config, "database", "--database", plugin_id)
        command.extend(["--query", query])

        sensitive_values = self._sensitive_values(config)
        prepared_config = dict(config)
        prepared_config["command"] = command
        prepared_config["_display_command"] = mask_sensitive_text(
            shlex.join(command),
            sensitive_values,
        )
        prepared_config["_sensitive_values"] = sensitive_values
        super().__init__(plugin_id, prepared_config)

    @staticmethod
    def _append_optional_arg(
        command: list[str],
        config: dict[str, Any],
        field_name: str,
        flag: str,
        plugin_id: str,
    ) -> None:
        value = config.get(field_name)
        if value is None or value == "":
            return
        if not isinstance(value, str):
            raise ValueError(f"Plugin {plugin_id!r} {field_name} must be a string.")
        command.extend([flag, value])

    @staticmethod
    def _sensitive_values(config: dict[str, Any]) -> tuple[str, ...]:
        values = [
            value
            for value in config.get("_sensitive_values", ())
            if isinstance(value, str) and value
        ]
        password = config.get("password")
        if isinstance(password, str) and password:
            values.append(password)
        return tuple(sorted(set(values), key=len, reverse=True))
