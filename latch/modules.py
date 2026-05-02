from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from flask import abort
from yaml.nodes import MappingNode, Node, ScalarNode

from . import config
from .plugin_registry import PLUGIN_TYPES, create_plugin
from .utils import validate_schedule_enabled, validate_schedule_expression
from .plugins.variables import validate_variable_definitions


def discover_module_names() -> list[str]:
    module_names = [
        path.stem
        for path in config.USER_MODULES_DIR.glob("*.yaml")
        if path.is_file() and config.MODULE_ID_RE.fullmatch(path.stem)
    ]
    return sorted(module_names)


def discover_system_module_names() -> list[str]:
    module_names = [
        path.stem
        for path in config.SYSTEM_MODULES_DIR.glob("*.yaml")
        if path.is_file() and config.MODULE_ID_RE.fullmatch(path.stem)
    ]
    return sorted(module_names)


def validate_module_id(module_name: str) -> None:
    if not config.MODULE_ID_RE.fullmatch(module_name):
        raise ValueError(
            "The module ID must contain only letters, numbers, '_' or '-'."
        )


def module_config_path(module_name: str) -> Path:
    validate_module_id(module_name)
    return config.USER_MODULES_DIR / f"{module_name}.yaml"


def system_module_config_path(module_name: str) -> Path:
    validate_module_id(module_name)
    return config.SYSTEM_MODULES_DIR / f"{module_name}.yaml"


def any_module_config_path(module_name: str) -> Path:
    user_path = module_config_path(module_name)
    if user_path.exists():
        return user_path

    system_path = system_module_config_path(module_name)
    if system_path.exists():
        return system_path

    return user_path


def is_system_module(module_name: str) -> bool:
    try:
        return system_module_config_path(module_name).exists()
    except ValueError:
        return False


def ensure_not_system_module(module_name: str) -> None:
    if is_system_module(module_name):
        raise ValueError("ID reserved for internal system use.")


def ensure_module_exists(module_name: str) -> None:
    try:
        config_path = any_module_config_path(module_name)
    except ValueError:
        abort(404)
    if not config_path.exists():
        abort(404)


def ensure_public_module_exists(module_name: str) -> None:
    try:
        config_path = module_config_path(module_name)
    except ValueError:
        abort(404)
    if not config_path.exists():
        abort(404)


def read_module_yaml(module_name: str) -> str:
    ensure_module_exists(module_name)
    return module_config_path(module_name).read_text(encoding="utf-8")


def default_module_yaml() -> str:
    return (
        "name: New module\n"
        "description: Describe what this module does.\n"
        "plugins:\n"
        "  - id: first_step\n"
        "    type: command_line\n"
        "    description: Describe this step.\n"
        "    command: \"echo first step\"\n"
        "    error_contains: \"ERROR\"\n"
        "    success_contains:\n"
    )


def parse_module_yaml(content: str) -> dict[str, Any]:
    try:
        config = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML: {exc}") from exc

    if not isinstance(config, dict):
        raise ValueError("The module YAML must be an object.")
    return config


def yaml_mapping_value(mapping: MappingNode, key: str) -> Node | None:
    for key_node, value_node in mapping.value:
        if isinstance(key_node, ScalarNode) and key_node.value == key:
            return value_node
    return None


def mask_sensitive_module_yaml(content: str) -> str:
    try:
        root = yaml.compose(content)
    except yaml.YAMLError:
        return content

    if not isinstance(root, MappingNode):
        return content

    variables_node = yaml_mapping_value(root, "variables")
    if not isinstance(variables_node, MappingNode):
        return content

    replacements: list[tuple[int, int, str]] = []
    for _variable_name_node, variable_node in variables_node.value:
        if not isinstance(variable_node, MappingNode):
            continue

        type_node = yaml_mapping_value(variable_node, "type")
        value_node = yaml_mapping_value(variable_node, "value")
        if (
            isinstance(type_node, ScalarNode)
            and type_node.value == "sensitive"
            and isinstance(value_node, ScalarNode)
        ):
            replacements.append(
                (value_node.start_mark.index, value_node.end_mark.index, '"****"')
            )

    masked_content = content
    for start, end, replacement in sorted(replacements, reverse=True):
        masked_content = masked_content[:start] + replacement + masked_content[end:]
    return masked_content


def validate_optional_text(config: dict[str, Any], key: str, context: str) -> str:
    value = config.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{context} {key!r} deve ser texto.")
    return value


def validate_plugin_timeout_config(plugin: dict[str, Any], plugin_id: str) -> None:
    timeout = plugin.get("timeout")
    if timeout is not None:
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
            raise ValueError(f"Plugin {plugin_id!r} timeout must be a positive integer.")

    timeout_retries = plugin.get("timeout_retries")
    if timeout_retries is not None:
        if (
            isinstance(timeout_retries, bool)
            or not isinstance(timeout_retries, int)
            or timeout_retries < 0
        ):
            raise ValueError(
                f"Plugin {plugin_id!r} timeout_retries must be a non-negative integer."
            )


def validate_module_config(
    module_name: str,
    config: dict[str, Any],
    *,
    instantiate_plugins: bool = True,
) -> dict[str, Any]:
    name = config.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Module {module_name!r} must define a non-empty name.")

    description = validate_optional_text(config, "description", f"Module {module_name!r}")
    schedule = validate_schedule_expression(config.get("schedule"), f"Module {module_name!r}")
    schedule_enabled = validate_schedule_enabled(
        config.get("schedule_enabled"),
        schedule,
        f"Module {module_name!r}",
    )
    plugins = config.get("plugins")
    if not isinstance(plugins, list) or not plugins:
        raise ValueError(f"Module {module_name!r} must define a non-empty plugins list.")

    variables = validate_variable_definitions(module_name, config.get("variables"))
    validated_plugins: list[dict[str, Any]] = []
    plugin_ids: set[str] = set()
    for index, plugin in enumerate(plugins, start=1):
        if not isinstance(plugin, dict):
            raise ValueError(f"Plugin #{index} in module {module_name!r} must be an object.")

        plugin_id = plugin.get("id")
        if not isinstance(plugin_id, str) or not plugin_id:
            raise ValueError(f"Plugin #{index} in module {module_name!r} must define id.")
        if plugin_id in plugin_ids:
            raise ValueError(f"Plugin {plugin_id!r} is duplicated in module {module_name!r}.")
        plugin_ids.add(plugin_id)

        plugin_type = plugin.get("type")
        if not isinstance(plugin_type, str) or not plugin_type:
            raise ValueError(f"Plugin {plugin_id!r} in module {module_name!r} must define type.")
        if plugin_type not in PLUGIN_TYPES:
            raise ValueError(f"Tipo de plugin desconhecido: {plugin_type!r}.")
        validate_optional_text(plugin, "description", f"Plugin {plugin_id!r}")
        validate_plugin_timeout_config(plugin, plugin_id)

        if instantiate_plugins:
            create_plugin(plugin, variables)
        validated_plugins.append(dict(plugin))

    return {
        "id": module_name,
        "name": name,
        "description": description,
        "schedule": schedule,
        "schedule_enabled": schedule_enabled,
        "variables": variables,
        "plugins": validated_plugins,
    }


def dump_module_yaml(module: dict[str, Any]) -> str:
    config: dict[str, Any] = {
        "name": module["name"],
    }
    if module.get("description"):
        config["description"] = module["description"]
    if module.get("schedule"):
        config["schedule_enabled"] = bool(module.get("schedule_enabled"))
        config["schedule"] = module["schedule"]
    if module.get("variables"):
        config["variables"] = module["variables"]
    config["plugins"] = module["plugins"]
    return yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )


def write_module_config(module_name: str, module: dict[str, Any]) -> None:
    module_config_path(module_name).write_text(dump_module_yaml(module), encoding="utf-8")


def write_module_yaml_content(module_name: str, content: str) -> None:
    module_config_path(module_name).write_text(content, encoding="utf-8")


def delete_module_files(module_name: str) -> None:
    module_config_path(module_name).unlink(missing_ok=True)
    (config.TEMP_DIR / f"temp_{module_name}.jsonl").unlink(missing_ok=True)
    (config.TEMP_DIR / f"active_{module_name}.json").unlink(missing_ok=True)
    (config.LOCKS_DIR / f"{module_name}.lock").unlink(missing_ok=True)


def load_module_config(module_name: str) -> dict[str, Any]:
    ensure_module_exists(module_name)
    config_path = any_module_config_path(module_name)
    if not config_path.exists():
        abort(404)

    with config_path.open("r", encoding="utf-8") as config_file:
        config = parse_module_yaml(config_file.read())

    return validate_module_config(module_name, config, instantiate_plugins=False)


def load_system_module_config(module_name: str) -> dict[str, Any]:
    try:
        config_path = system_module_config_path(module_name)
    except ValueError:
        abort(404)
    if not config_path.exists():
        abort(404)

    with config_path.open("r", encoding="utf-8") as config_file:
        config = parse_module_yaml(config_file.read())

    return validate_module_config(module_name, config, instantiate_plugins=False)
