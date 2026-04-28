from __future__ import annotations

import importlib
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import yaml
from filelock import FileLock, Timeout
from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context

from plugins.base import BasePlugin, PluginEvent, PluginExecutionError


BASE_DIR = Path(__file__).resolve().parent
MODULES_DIR = BASE_DIR / "modules"
LOCKS_DIR = BASE_DIR / "locks"
TEMP_DIR = BASE_DIR / "temp"
ALLOWED_MODULES = {"tri", "analitico"}
PLUGIN_TYPES = {
    "command_line": "plugins.command_line.CommandLinePlugin",
}
ACTIVE_MODULES: set[str] = set()
ACTIVE_MODULES_LOCK = threading.Lock()

LOCKS_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
app = Flask(__name__)


@app.get("/")
def index() -> str:
    return render_template(
        "index.html",
        modules=[module_with_status(module_name) for module_name in sorted(ALLOWED_MODULES)],
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
                for module_name in sorted(ALLOWED_MODULES)
            }
        }
    )


@app.get("/api/modules/<module_name>/status")
def module_status(module_name: str) -> Response:
    if module_name not in ALLOWED_MODULES:
        abort(404)
    return jsonify({"id": module_name, "running": is_module_running(module_name)})


@app.get("/api/modules/<module_name>/logs")
def module_logs(module_name: str) -> Response:
    if module_name not in ALLOWED_MODULES:
        abort(404)

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
    if module_name not in ALLOWED_MODULES:
        abort(404)

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


def load_module_config(module_name: str) -> dict[str, Any]:
    if module_name not in ALLOWED_MODULES:
        abort(404)

    config_path = MODULES_DIR / f"{module_name}.yaml"
    if not config_path.exists():
        abort(404)

    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    plugins = config.get("plugins")
    if not isinstance(plugins, list):
        raise ValueError(f"Module {module_name!r} must define a plugins list.")

    for index, plugin in enumerate(plugins, start=1):
        if not isinstance(plugin, dict):
            raise ValueError(f"Plugin #{index} in module {module_name!r} must be an object.")
        if not plugin.get("id"):
            raise ValueError(f"Plugin #{index} in module {module_name!r} must define id.")
        if not plugin.get("type"):
            raise ValueError(f"Plugin {plugin['id']!r} in module {module_name!r} must define type.")

    return {
        "id": module_name,
        "name": config.get("name", module_name),
        "plugins": plugins,
    }


def module_with_status(module_name: str) -> dict[str, Any]:
    module = load_module_config(module_name)
    module["running"] = is_module_running(module_name)
    return module


def is_module_running(module_name: str) -> bool:
    if module_name not in ALLOWED_MODULES:
        abort(404)

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


def set_module_active(module_name: str, active: bool) -> None:
    with ACTIVE_MODULES_LOCK:
        if active:
            ACTIVE_MODULES.add(module_name)
        else:
            ACTIVE_MODULES.discard(module_name)


def module_log_path(module_name: str) -> Path:
    if module_name not in ALLOWED_MODULES:
        abort(404)
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
    set_module_active(module_id, True)
    sequence = 0

    def emit(event: str, payload: dict[str, Any]) -> str:
        nonlocal sequence
        sequence += 1
        record = build_log_record(run_id, sequence, event, payload)
        append_module_log(module_id, record)
        return sse(event, record)

    try:
        yield emit("status", {"level": "info", "message": f"Iniciando batch {module['name']}."})

        for plugin_config in module["plugins"]:
            plugin_id = plugin_config["id"]
            yield emit(
                "plugin_start",
                {
                    "level": "info",
                    "plugin": plugin_id,
                    "message": f"Executando plugin {plugin_id}.",
                },
            )

            try:
                plugin = create_plugin(plugin_config)
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
            except (PluginExecutionError, ValueError) as exc:
                yield emit(
                    "done",
                    {
                        "status": "failed",
                        "level": "error",
                        "plugin": plugin_id,
                        "message": str(exc),
                    },
                )
                return

            yield emit(
                "plugin_done",
                {
                    "level": "success",
                    "plugin": plugin_id,
                    "message": f"Plugin {plugin_id} concluído.",
                },
            )

        yield emit(
            "done",
            {
                "status": "success",
                "level": "success",
                "message": f"Batch {module['name']} concluído com sucesso.",
            },
        )
    except Exception as exc:
        yield emit(
            "done",
            {
                "status": "failed",
                "level": "error",
                "message": f"Erro inesperado: {exc}",
            },
        )
    finally:
        set_module_active(module_id, False)
        lock.release()


def create_plugin(plugin_config: dict[str, Any]) -> BasePlugin:
    plugin_type = plugin_config["type"]
    import_path = PLUGIN_TYPES.get(plugin_type)
    if import_path is None:
        raise ValueError(f"Tipo de plugin desconhecido: {plugin_type!r}.")

    module_path, class_name = import_path.rsplit(".", 1)
    plugin_module = importlib.import_module(module_path)
    plugin_class = getattr(plugin_module, class_name)
    return plugin_class(plugin_config["id"], plugin_config)


def sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


if __name__ == "__main__":
    app.run(debug=True, threaded=True)
