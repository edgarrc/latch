from __future__ import annotations

import importlib
import json
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import yaml
from filelock import FileLock, Timeout
from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context

from plugins.base import (
    BasePlugin,
    PluginEvent,
    PluginExecutionError,
    PluginKillError,
    PluginKilledError,
)
from plugins.variables import prepare_command_plugin_config, validate_variable_definitions


BASE_DIR = Path(__file__).resolve().parent
MODULES_DIR = BASE_DIR / "modules"
LOCKS_DIR = BASE_DIR / "locks"
TEMP_DIR = BASE_DIR / "temp"
MODULE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
PLUGIN_TYPES = {
    "command_line": "plugins.command_line.CommandLinePlugin",
}
ACTIVE_MODULES: set[str] = set()
ACTIVE_RUNS: dict[str, "ActiveRun"] = {}
ACTIVE_MODULES_LOCK = threading.Lock()

LOCKS_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
app = Flask(__name__)


@dataclass
class ActiveRun:
    module_id: str
    run_id: str
    sequence: int = 0
    current_plugin: BasePlugin | None = None
    current_plugin_id: str | None = None
    current_plugin_type: str | None = None
    kill_requested: bool = False
    plugin_statuses: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@app.get("/")
def index() -> str:
    return render_template(
        "index.html",
        modules=[module_with_status(module_name) for module_name in discover_module_names()],
    )


@app.get("/modules/new")
def new_module_page() -> str:
    return render_template(
        "module_edit.html",
        mode="create",
        module_id="",
        module_name="",
        module_running=False,
        yaml_content=default_module_yaml(),
    )


@app.get("/modules/<module_name>/edit")
def edit_module_page(module_name: str) -> str:
    ensure_module_exists(module_name)
    module = module_with_status(module_name)
    return render_template(
        "module_edit.html",
        mode="edit",
        module_id=module_name,
        module_name=module["name"],
        module_running=module["running"],
        yaml_content=read_module_yaml(module_name),
    )


@app.get("/<module_name>")
def module_page(module_name: str) -> str:
    module = module_with_status(module_name)
    return render_template("module.html", module=module)


@app.get("/api/modules/status")
def modules_status() -> Response:
    return jsonify(
        {
            "modules": {
                module_name: {"running": is_module_running(module_name)}
                for module_name in discover_module_names()
            }
        }
    )


@app.get("/api/modules/<module_name>/status")
def module_status(module_name: str) -> Response:
    ensure_module_exists(module_name)
    return jsonify({"id": module_name, "running": is_module_running(module_name)})


@app.post("/api/modules/validate")
def validate_module() -> Response:
    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id") or "new_module"
    content = payload.get("content")
    if not isinstance(content, str):
        return jsonify({"valid": False, "message": "O YAML do módulo é obrigatório."}), 400

    try:
        validate_module_id(str(module_id))
        config = parse_module_yaml(content)
        module = validate_module_config(str(module_id), config)
    except ValueError as exc:
        return jsonify({"valid": False, "message": str(exc)}), 400

    return jsonify(
        {
            "valid": True,
            "message": "Configuração válida.",
            "module": {
                "id": module["id"],
                "name": module["name"],
                "plugins": len(module["plugins"]),
            },
        }
    )


@app.post("/api/modules")
def create_module_config() -> Response:
    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id")
    content = payload.get("content")
    if not isinstance(module_id, str) or not module_id:
        return jsonify({"saved": False, "message": "Informe o ID do módulo."}), 400
    if not isinstance(content, str):
        return jsonify({"saved": False, "message": "O YAML do módulo é obrigatório."}), 400

    try:
        validate_module_id(module_id)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    config_path = module_config_path(module_id)
    if config_path.exists():
        return jsonify({"saved": False, "message": "Já existe um módulo com esse ID."}), 409

    try:
        config = parse_module_yaml(content)
        module = validate_module_config(module_id, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    write_module_config(module_id, module)
    return jsonify(
        {
            "saved": True,
            "id": module_id,
            "message": "Módulo criado.",
            "redirect": f"/modules/{module_id}/edit",
        }
    ), 201


@app.put("/api/modules/<module_name>")
def update_module_config(module_name: str) -> Response:
    ensure_module_exists(module_name)
    if is_module_running(module_name):
        return jsonify(
            {
                "saved": False,
                "running": True,
                "message": "Não é possível salvar enquanto o módulo está em execução.",
            }
        ), 409

    payload = request.get_json(silent=True) or {}
    content = payload.get("content")
    if not isinstance(content, str):
        return jsonify({"saved": False, "message": "O YAML do módulo é obrigatório."}), 400

    try:
        config = parse_module_yaml(content)
        module = validate_module_config(module_name, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    write_module_config(module_name, module)
    return jsonify({"saved": True, "id": module_name, "message": "Módulo salvo."})


@app.delete("/api/modules/<module_name>")
def delete_module_config(module_name: str) -> Response:
    ensure_module_exists(module_name)
    if is_module_running(module_name):
        return jsonify(
            {
                "deleted": False,
                "running": True,
                "message": "Não é possível excluir enquanto o módulo está em execução.",
            }
        ), 409

    delete_module_files(module_name)
    return jsonify({"deleted": True, "id": module_name, "message": "Módulo excluído."})


@app.get("/api/modules/<module_name>/logs")
def module_logs(module_name: str) -> Response:
    ensure_module_exists(module_name)

    since = request.args.get("since", default=0, type=int) or 0
    current_run_id = request.args.get("run_id", default="", type=str)
    since = max(since, 0)
    all_events = read_module_log(module_name)
    latest_sequence = all_events[-1]["sequence"] if all_events else 0
    latest_run_id = all_events[-1].get("run_id", "") if all_events else ""
    reset = bool(
        latest_run_id
        and (
            (current_run_id and current_run_id != latest_run_id)
            or (since > latest_sequence and latest_sequence > 0)
        )
    )
    events = all_events if reset else [
        event for event in all_events if event["sequence"] > since
    ]

    return jsonify(
        {
            "id": module_name,
            "running": is_module_running(module_name),
            "run_id": latest_run_id,
            "latest_sequence": latest_sequence,
            "reset": reset,
            "events": events,
        }
    )


@app.post("/api/modules/<module_name>/logs/clear")
def clear_module_logs(module_name: str) -> Response:
    ensure_module_exists(module_name)

    if is_module_running(module_name):
        return jsonify(
            {
                "id": module_name,
                "cleared": False,
                "running": True,
                "message": "Não é possível limpar o log enquanto o módulo está em execução.",
            }
        ), 409

    clear_module_log(module_name)
    return jsonify({"id": module_name, "cleared": True, "running": False})


@app.post("/api/modules/<module_name>/kill")
def kill_module(module_name: str) -> Response:
    ensure_module_exists(module_name)

    active_run = get_active_run(module_name)
    if active_run is None:
        return jsonify(
            {
                "id": module_name,
                "killed": False,
                "running": False,
                "message": "O módulo não está em execução.",
            }
        ), 409

    plugin = active_run.current_plugin
    if plugin is not None:
        try:
            plugin.kill()
        except PluginKillError as exc:
            append_active_log(
                module_name,
                "log",
                {
                    "level": "error",
                    "plugin": active_run.current_plugin_id,
                    "message": str(exc),
                },
            )
            return jsonify(
                {
                    "id": module_name,
                    "killed": False,
                    "running": is_module_running(module_name),
                    "message": str(exc),
                }
            ), 409

    mark_kill_requested(module_name)
    append_active_log(
        module_name,
        "log",
        {
            "level": "error",
            "plugin": active_run.current_plugin_id,
            "message": "Kill solicitado pelo usuário.",
        },
    )

    return jsonify(
        {
            "id": module_name,
            "killed": True,
            "running": True,
            "message": "Kill solicitado.",
        }
    )


@app.get("/api/modules/<module_name>/run")
def run_module(module_name: str) -> Response:
    module = load_module_config(module_name)
    return Response(
        stream_with_context(stream_batch(module)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def discover_module_names() -> list[str]:
    module_names = [
        path.stem
        for path in MODULES_DIR.glob("*.yaml")
        if path.is_file() and MODULE_ID_RE.fullmatch(path.stem)
    ]
    return sorted(module_names)


def validate_module_id(module_name: str) -> None:
    if not MODULE_ID_RE.fullmatch(module_name):
        raise ValueError(
            "O ID do módulo deve conter apenas letras, números, '_' ou '-'."
        )


def module_config_path(module_name: str) -> Path:
    validate_module_id(module_name)
    return MODULES_DIR / f"{module_name}.yaml"


def ensure_module_exists(module_name: str) -> None:
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
        "name: Novo módulo\n"
        "description: Descreva o que este módulo faz.\n"
        "plugins:\n"
        "  - id: primeira_etapa\n"
        "    type: command_line\n"
        "    description: Descreva esta etapa.\n"
        "    command: \"echo primeira etapa\"\n"
        "    error_contains: \"ERROR\"\n"
        "    success_contains:\n"
    )


def parse_module_yaml(content: str) -> dict[str, Any]:
    try:
        config = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML inválido: {exc}") from exc

    if not isinstance(config, dict):
        raise ValueError("O YAML do módulo deve ser um objeto.")
    return config


def validate_optional_text(config: dict[str, Any], key: str, context: str) -> str:
    value = config.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{context} {key!r} deve ser texto.")
    return value


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

        if instantiate_plugins:
            create_plugin(plugin, variables)
        validated_plugins.append(dict(plugin))

    return {
        "id": module_name,
        "name": name,
        "description": description,
        "variables": variables,
        "plugins": validated_plugins,
    }


def dump_module_yaml(module: dict[str, Any]) -> str:
    config: dict[str, Any] = {
        "name": module["name"],
    }
    if module.get("description"):
        config["description"] = module["description"]
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


def delete_module_files(module_name: str) -> None:
    module_config_path(module_name).unlink(missing_ok=True)
    (TEMP_DIR / f"temp_{module_name}.jsonl").unlink(missing_ok=True)
    (TEMP_DIR / f"active_{module_name}.json").unlink(missing_ok=True)
    (LOCKS_DIR / f"{module_name}.lock").unlink(missing_ok=True)


def load_module_config(module_name: str) -> dict[str, Any]:
    ensure_module_exists(module_name)
    config_path = module_config_path(module_name)
    if not config_path.exists():
        abort(404)

    with config_path.open("r", encoding="utf-8") as config_file:
        config = parse_module_yaml(config_file.read())

    return validate_module_config(module_name, config, instantiate_plugins=False)


def module_with_status(module_name: str) -> dict[str, Any]:
    module = load_module_config(module_name)
    module["running"] = is_module_running(module_name)
    return module


def is_module_running(module_name: str) -> bool:
    ensure_module_exists(module_name)

    with ACTIVE_MODULES_LOCK:
        if module_name in ACTIVE_MODULES:
            return True

    lock = FileLock(LOCKS_DIR / f"{module_name}.lock")
    try:
        lock.acquire(blocking=False)
    except Timeout:
        return True
    else:
        lock.release()
        return False


def get_active_run(module_name: str) -> ActiveRun | None:
    with ACTIVE_MODULES_LOCK:
        return ACTIVE_RUNS.get(module_name)


def create_active_run(
    module_name: str,
    run_id: str,
    plugins: list[dict[str, Any]] | None = None,
) -> ActiveRun:
    plugin_statuses = {
        plugin["id"]: "enqueued"
        for plugin in plugins or []
        if isinstance(plugin, dict) and isinstance(plugin.get("id"), str)
    }
    active_run = ActiveRun(
        module_id=module_name,
        run_id=run_id,
        plugin_statuses=plugin_statuses,
    )
    with ACTIVE_MODULES_LOCK:
        ACTIVE_MODULES.add(module_name)
        ACTIVE_RUNS[module_name] = active_run
    write_active_run_file(active_run)
    return active_run


def remove_active_run(module_name: str) -> None:
    with ACTIVE_MODULES_LOCK:
        ACTIVE_MODULES.discard(module_name)
        ACTIVE_RUNS.pop(module_name, None)
    active_run_path(module_name).unlink(missing_ok=True)


def set_active_plugin(
    module_name: str,
    plugin: BasePlugin | None,
    plugin_config: dict[str, Any] | None = None,
) -> None:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        if active_run is None:
            return
        active_run.current_plugin = plugin
        active_run.current_plugin_id = plugin.plugin_id if plugin is not None else None
        active_run.current_plugin_type = (
            plugin_config.get("type") if plugin_config is not None else None
        )
        if plugin is None:
            active_run.metadata = {}
        snapshot = serialize_active_run(active_run)
    write_active_run_snapshot(module_name, snapshot)


def update_active_run_metadata(module_name: str, metadata: dict[str, Any]) -> None:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        if active_run is None:
            return
        active_run.metadata.update(metadata)
        snapshot = serialize_active_run(active_run)
    write_active_run_snapshot(module_name, snapshot)


def mark_kill_requested(module_name: str) -> None:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        if active_run is None:
            return
        active_run.kill_requested = True
        snapshot = serialize_active_run(active_run)
    write_active_run_snapshot(module_name, snapshot)


def set_plugin_status(module_name: str, plugin_id: str, status: str) -> dict[str, str]:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        if active_run is None:
            return {}
        active_run.plugin_statuses[plugin_id] = status
        snapshot = serialize_active_run(active_run)
        plugin_statuses = dict(active_run.plugin_statuses)
    write_active_run_snapshot(module_name, snapshot)
    return plugin_statuses


def get_plugin_statuses(module_name: str) -> dict[str, str]:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        if active_run is None:
            return {}
        return dict(active_run.plugin_statuses)


def get_active_kill_requested(module_name: str) -> bool:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS.get(module_name)
        return active_run.kill_requested if active_run is not None else False


def append_active_log(module_name: str, event: str, payload: dict[str, Any]) -> dict[str, Any]:
    with ACTIVE_MODULES_LOCK:
        active_run = ACTIVE_RUNS[module_name]
        active_run.sequence += 1
        record = build_log_record(active_run.run_id, active_run.sequence, event, payload)
        snapshot = serialize_active_run(active_run)
    append_module_log(module_name, record)
    write_active_run_snapshot(module_name, snapshot)
    return record


def active_run_path(module_name: str) -> Path:
    ensure_module_exists(module_name)
    return TEMP_DIR / f"active_{module_name}.json"


def serialize_active_run(active_run: ActiveRun) -> dict[str, Any]:
    return {
        "module_id": active_run.module_id,
        "run_id": active_run.run_id,
        "sequence": active_run.sequence,
        "current_plugin_id": active_run.current_plugin_id,
        "current_plugin_type": active_run.current_plugin_type,
        "kill_requested": active_run.kill_requested,
        "plugin_statuses": active_run.plugin_statuses,
        "metadata": active_run.metadata,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_active_run_file(active_run: ActiveRun) -> None:
    write_active_run_snapshot(active_run.module_id, serialize_active_run(active_run))


def write_active_run_snapshot(module_name: str, snapshot: dict[str, Any]) -> None:
    active_run_path(module_name).write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def module_log_path(module_name: str) -> Path:
    ensure_module_exists(module_name)
    return TEMP_DIR / f"temp_{module_name}.jsonl"


def clear_module_log(module_name: str) -> None:
    module_log_path(module_name).write_text("", encoding="utf-8")


def build_log_record(
    run_id: str,
    sequence: int,
    event: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "sequence": sequence,
        "event": event,
        "created_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }


def append_module_log(module_name: str, record: dict[str, Any]) -> None:
    with module_log_path(module_name).open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(record, ensure_ascii=False) + "\n")
        log_file.flush()


def read_module_log(module_name: str) -> list[dict[str, Any]]:
    log_path = module_log_path(module_name)
    if not log_path.exists():
        return []

    events: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as log_file:
        for line in log_file:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if isinstance(record.get("sequence"), int):
                events.append(record)

    return events


def stream_batch(module: dict[str, Any]) -> Iterator[str]:
    module_id = module["id"]
    lock_path = LOCKS_DIR / f"{module_id}.lock"
    lock = FileLock(lock_path)

    try:
        lock.acquire(blocking=False)
    except Timeout:
        yield sse(
            "done",
            {
                "status": "locked",
                "level": "error",
                "message": f"O módulo {module['name']} já está em execução.",
            },
        )
        return

    run_id = uuid4().hex
    clear_module_log(module_id)
    active_run = create_active_run(module_id, run_id, module["plugins"])

    def emit(event: str, payload: dict[str, Any]) -> str:
        record = append_active_log(module_id, event, payload)
        return sse(event, record)

    try:
        yield emit(
            "status",
            {
                "level": "info",
                "message": f"Iniciando batch {module['name']}.",
                "plugin_statuses": dict(active_run.plugin_statuses),
            },
        )

        for plugin_config in module["plugins"]:
            if get_active_kill_requested(module_id):
                yield emit(
                    "done",
                    {
                        "status": "killed",
                        "level": "error",
                        "plugin_statuses": get_plugin_statuses(module_id),
                        "message": f"Batch {module['name']} interrompido pelo usuário.",
                    },
                )
                return

            plugin_id = plugin_config["id"]
            try:
                plugin = create_plugin(plugin_config, module.get("variables", {}))
            except ValueError as exc:
                plugin_statuses = set_plugin_status(module_id, plugin_id, "failed")
                yield emit(
                    "done",
                    {
                        "status": "failed",
                        "level": "error",
                        "plugin": plugin_id,
                        "plugin_statuses": plugin_statuses,
                        "message": str(exc),
                    },
                )
                return

            plugin.set_runtime_context(
                module_id,
                run_id,
                lambda metadata, current_module_id=module_id: update_active_run_metadata(
                    current_module_id,
                    metadata,
                ),
            )
            set_active_plugin(module_id, plugin, plugin_config)
            plugin_statuses = set_plugin_status(module_id, plugin_id, "running")
            yield emit(
                "plugin_start",
                {
                    "level": "info",
                    "plugin": plugin_id,
                    "plugin_status": "running",
                    "plugin_statuses": plugin_statuses,
                    "message": f"Executando plugin {plugin_id}.",
                },
            )

            try:
                for event in plugin.run():
                    yield emit(
                        "log",
                        {
                            "level": event.level,
                            "plugin": plugin.plugin_id,
                            "stream": event.stream,
                            "message": event.message,
                        },
                    )
            except PluginKilledError as exc:
                plugin_statuses = set_plugin_status(module_id, plugin_id, "killed")
                yield emit(
                    "done",
                    {
                        "status": "killed",
                        "level": "error",
                        "plugin": plugin_id,
                        "plugin_statuses": plugin_statuses,
                        "message": str(exc),
                    },
                )
                return
            except (PluginExecutionError, PluginKillError, ValueError) as exc:
                plugin_statuses = set_plugin_status(module_id, plugin_id, "failed")
                yield emit(
                    "done",
                    {
                        "status": "failed",
                        "level": "error",
                        "plugin": plugin_id,
                        "plugin_statuses": plugin_statuses,
                        "message": str(exc),
                    },
                )
                return

            plugin_statuses = set_plugin_status(module_id, plugin_id, "success")
            set_active_plugin(module_id, None)
            yield emit(
                "plugin_done",
                {
                    "level": "success",
                    "plugin": plugin_id,
                    "plugin_status": "success",
                    "plugin_statuses": plugin_statuses,
                    "message": f"Plugin {plugin_id} concluído.",
                },
            )

        if get_active_kill_requested(module_id):
            yield emit(
                "done",
                {
                    "status": "killed",
                    "level": "error",
                    "plugin_statuses": get_plugin_statuses(module_id),
                    "message": f"Batch {module['name']} interrompido pelo usuário.",
                },
            )
            return

        yield emit(
            "done",
            {
                "status": "success",
                "level": "success",
                "plugin_statuses": {
                    plugin["id"]: "success"
                    for plugin in module["plugins"]
                    if isinstance(plugin, dict) and isinstance(plugin.get("id"), str)
                },
                "message": f"Batch {module['name']} concluído com sucesso.",
            },
        )
    except Exception as exc:
        yield emit(
            "done",
            {
                "status": "failed",
                "level": "error",
                "plugin_statuses": get_plugin_statuses(module_id),
                "message": f"Erro inesperado: {exc}",
            },
        )
    finally:
        remove_active_run(module_id)
        lock.release()


def create_plugin(
    plugin_config: dict[str, Any],
    module_variables: dict[str, dict[str, Any]] | None = None,
) -> BasePlugin:
    plugin_type = plugin_config["type"]
    import_path = PLUGIN_TYPES.get(plugin_type)
    if import_path is None:
        raise ValueError(f"Tipo de plugin desconhecido: {plugin_type!r}.")

    prepared_plugin_config = plugin_config
    if plugin_type == "command_line":
        prepared_plugin_config = prepare_command_plugin_config(
            plugin_config,
            module_variables or {},
        )

    module_path, class_name = import_path.rsplit(".", 1)
    plugin_module = importlib.import_module(module_path)
    plugin_class = getattr(plugin_module, class_name)
    return plugin_class(prepared_plugin_config["id"], prepared_plugin_config)


def sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


if __name__ == "__main__":
    app.run(debug=True, threaded=True)
