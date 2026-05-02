from __future__ import annotations

import importlib
from typing import Any

from .plugins.base import BasePlugin
from .plugins.variables import prepare_plugin_config

PLUGIN_TYPES = {
    "clickhouse_client": "latch.plugins.clickhouse_client.ClickHouseClientPlugin",
    "command_line": "latch.plugins.command_line.CommandLinePlugin",
    "redis_client": "latch.plugins.redis_client.RedisClientPlugin",
}


def create_plugin(
    plugin_config: dict[str, Any],
    module_variables: dict[str, dict[str, Any]] | None = None,
) -> BasePlugin:
    plugin_type = plugin_config["type"]
    import_path = PLUGIN_TYPES.get(plugin_type)
    if import_path is None:
        raise ValueError(f"Tipo de plugin desconhecido: {plugin_type!r}.")

    prepared_plugin_config = prepare_plugin_config(
        plugin_config,
        module_variables or {},
    )

    module_path, class_name = import_path.rsplit(".", 1)
    plugin_module = importlib.import_module(module_path)
    plugin_class = getattr(plugin_module, class_name)
    return plugin_class(prepared_plugin_config["id"], prepared_plugin_config)
