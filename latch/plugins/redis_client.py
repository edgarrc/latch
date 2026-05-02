from __future__ import annotations

import shlex
from typing import Any

from .command_line import CommandLinePlugin
from .variables import mask_sensitive_text


REDIS_CLI_BINARY = "/usr/bin/redis-cli"


class RedisClientPlugin(CommandLinePlugin):
    plugin_type = "redis_client"

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        args = self._parse_args(plugin_id, config.get("args"))
        command = [REDIS_CLI_BINARY]
        host = self._parse_host(plugin_id, config.get("host"))
        if host:
            command.extend(["-h", host])
        command.extend(args)
        sensitive_values = tuple(
            sorted(
                {
                    value
                    for value in config.get("_sensitive_values", ())
                    if isinstance(value, str) and value
                },
                key=len,
                reverse=True,
            )
        )

        prepared_config = dict(config)
        prepared_config["command"] = command
        prepared_config["_display_command"] = mask_sensitive_text(
            shlex.join(command),
            sensitive_values,
        )
        prepared_config["_sensitive_values"] = sensitive_values
        super().__init__(plugin_id, prepared_config)

    @staticmethod
    def _parse_args(plugin_id: str, args: Any) -> list[str]:
        if isinstance(args, str):
            if not args.strip():
                raise ValueError(f"Plugin {plugin_id!r} must define non-empty args.")
            parsed_args = shlex.split(args)
        elif isinstance(args, list):
            parsed_args = args
        else:
            raise ValueError(
                f"Plugin {plugin_id!r} args must be a non-empty string or list."
            )

        if not parsed_args:
            raise ValueError(f"Plugin {plugin_id!r} must define non-empty args.")
        if not all(isinstance(arg, str) and arg for arg in parsed_args):
            raise ValueError(
                f"Plugin {plugin_id!r} args list must contain only non-empty strings."
        )
        return list(parsed_args)

    @staticmethod
    def _parse_host(plugin_id: str, host: Any) -> str:
        if host is None or host == "":
            return ""
        if not isinstance(host, str):
            raise ValueError(f"Plugin {plugin_id!r} host must be a string.")
        if not host.strip():
            raise ValueError(f"Plugin {plugin_id!r} host must be a non-empty string.")
        return host.strip()
