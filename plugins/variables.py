from __future__ import annotations

import os
import re
import shlex
import string
from dataclasses import dataclass
from typing import Any


VARIABLE_TYPES = {"string", "integer", "sensitive"}
VARIABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ENVIRONMENT_REFERENCE_RE = re.compile(r"^\$([A-Za-z_][A-Za-z0-9_]*)$")


@dataclass(frozen=True)
class ResolvedVariables:
    values: dict[str, str | int]
    sensitive_names: set[str]
    sensitive_values: tuple[str, ...]


def validate_variable_definitions(
    module_name: str,
    variables: Any,
) -> dict[str, dict[str, Any]]:
    if variables is None:
        return {}
    if not isinstance(variables, dict):
        raise ValueError(f"Module {module_name!r} variables must be an object.")

    validated: dict[str, dict[str, Any]] = {}
    for variable_name, definition in variables.items():
        if not isinstance(variable_name, str) or not VARIABLE_NAME_RE.fullmatch(variable_name):
            raise ValueError(
                f"Module {module_name!r} variable name {variable_name!r} is invalid."
            )
        if not isinstance(definition, dict):
            raise ValueError(
                f"Module {module_name!r} variable {variable_name!r} must be an object."
            )

        extra_keys = set(definition) - {"type", "value"}
        if extra_keys:
            raise ValueError(
                f"Module {module_name!r} variable {variable_name!r} has unsupported keys: "
                f"{', '.join(sorted(extra_keys))}."
            )
        if "type" not in definition:
            raise ValueError(
                f"Module {module_name!r} variable {variable_name!r} must define type."
            )
        if "value" not in definition:
            raise ValueError(
                f"Module {module_name!r} variable {variable_name!r} must define value."
            )
        if definition["type"] not in VARIABLE_TYPES:
            raise ValueError(
                f"Module {module_name!r} variable {variable_name!r} type must be one of: "
                f"{', '.join(sorted(VARIABLE_TYPES))}."
            )

        validated[variable_name] = {
            "type": definition["type"],
            "value": definition["value"],
        }

    return validated


def prepare_command_plugin_config(
    plugin_config: dict[str, Any],
    variable_definitions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not variable_definitions:
        return plugin_config

    resolved_variables = resolve_variables(variable_definitions)
    command = plugin_config.get("command")
    resolved_command = render_command(command, resolved_variables, mask_sensitive=False)
    display_command = render_command(command, resolved_variables, mask_sensitive=True)

    prepared_config = dict(plugin_config)
    prepared_config["command"] = resolved_command
    prepared_config["_display_command"] = display_command_as_text(display_command)
    prepared_config["_sensitive_values"] = resolved_variables.sensitive_values
    return prepared_config


def resolve_variables(
    variable_definitions: dict[str, dict[str, Any]],
) -> ResolvedVariables:
    values: dict[str, str | int] = {}
    sensitive_names: set[str] = set()
    sensitive_values: list[str] = []

    for variable_name, definition in variable_definitions.items():
        variable_type = definition["type"]
        raw_value = resolve_environment_value(variable_name, definition["value"])
        value = coerce_value(variable_name, variable_type, raw_value)
        values[variable_name] = value

        if variable_type == "sensitive":
            sensitive_names.add(variable_name)
            if value:
                sensitive_values.append(str(value))

    sensitive_values.sort(key=len, reverse=True)
    return ResolvedVariables(
        values=values,
        sensitive_names=sensitive_names,
        sensitive_values=tuple(sensitive_values),
    )


def resolve_environment_value(variable_name: str, raw_value: Any) -> Any:
    if not isinstance(raw_value, str):
        return raw_value

    environment_reference = ENVIRONMENT_REFERENCE_RE.fullmatch(raw_value)
    if environment_reference is None:
        return raw_value

    environment_name = environment_reference.group(1)
    if environment_name not in os.environ:
        raise ValueError(
            f"Variable {variable_name!r} references missing environment variable "
            f"{environment_name!r}."
        )
    return os.environ[environment_name]


def coerce_value(variable_name: str, variable_type: str, raw_value: Any) -> str | int:
    if variable_type in {"string", "sensitive"}:
        if not isinstance(raw_value, str):
            raise ValueError(f"Variable {variable_name!r} must be a string.")
        return raw_value

    if variable_type == "integer":
        if isinstance(raw_value, bool):
            raise ValueError(f"Variable {variable_name!r} must be an integer.")
        if isinstance(raw_value, int):
            return raw_value
        if isinstance(raw_value, str) and re.fullmatch(r"[+-]?\d+", raw_value):
            return int(raw_value)
        raise ValueError(f"Variable {variable_name!r} must be an integer.")

    raise ValueError(f"Variable {variable_name!r} has unsupported type {variable_type!r}.")


def render_command(
    command: Any,
    resolved_variables: ResolvedVariables,
    *,
    mask_sensitive: bool,
) -> str | list[str]:
    if isinstance(command, str):
        return render_template(
            command,
            resolved_variables,
            quote_for_shell=True,
            mask_sensitive=mask_sensitive,
        )
    if isinstance(command, list):
        return [
            render_template(
                part,
                resolved_variables,
                quote_for_shell=False,
                mask_sensitive=mask_sensitive,
            )
            if isinstance(part, str)
            else part
            for part in command
        ]
    return command


def render_template(
    template: str,
    resolved_variables: ResolvedVariables,
    *,
    quote_for_shell: bool,
    mask_sensitive: bool,
) -> str:
    output: list[str] = []
    formatter = string.Formatter()

    try:
        parsed_template = formatter.parse(template)
        for literal_text, field_name, format_spec, conversion in parsed_template:
            output.append(literal_text)
            if field_name is None:
                continue
            if format_spec or conversion:
                raise ValueError(
                    f"Placeholder {{{field_name}}} must not define format or conversion."
                )
            if not VARIABLE_NAME_RE.fullmatch(field_name):
                raise ValueError(f"Placeholder {{{field_name}}} is invalid.")
            if field_name not in resolved_variables.values:
                raise ValueError(f"Placeholder {{{field_name}}} has no configured variable.")

            if mask_sensitive and field_name in resolved_variables.sensitive_names:
                value = "****"
            else:
                value = str(resolved_variables.values[field_name])
                if quote_for_shell:
                    value = shlex.quote(value)
            output.append(value)
    except ValueError:
        raise

    return "".join(output)


def display_command_as_text(command: str | list[str]) -> str:
    if isinstance(command, list):
        return shlex.join(command)
    return command


def mask_sensitive_text(text: str, sensitive_values: tuple[str, ...]) -> str:
    masked_text = text
    for sensitive_value in sensitive_values:
        masked_text = masked_text.replace(sensitive_value, "****")
    return masked_text
