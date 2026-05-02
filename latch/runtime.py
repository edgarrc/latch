from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from filelock import FileLock, Timeout

from . import config, events, modules, plugin_registry, utils
from .plugins.base import (
    BasePlugin,
    PluginExecutionError,
    PluginKillError,
    PluginKilledError,
)

ACTIVE_MODULES: set[str] = set()
ACTIVE_RUNS: dict[str, "ActiveRun"] = {}
ACTIVE_MODULES_LOCK = threading.Lock()


def clear_generated_temp_files() -> None:
    for pattern in config.GENERATED_TEMP_PATTERNS:
        for path in config.TEMP_DIR.glob(pattern):
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)


@dataclass
class ActiveRun:
    module_id: str
    run_id: str
    trigger: str = config.RUN_TRIGGER_MANUAL
    scheduled_for: str | None = None
    sequence: int = 0
    current_plugin: BasePlugin | None = None
    current_plugin_id: str | None = None
    current_plugin_type: str | None = None
    kill_requested: bool = False
    plugin_statuses: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

def module_with_status(module_name: str) -> dict[str, Any]:
    module = modules.load_module_config(module_name)
    module.update(module_runtime_status(module))
    return module


def module_runtime_status(module: dict[str, Any]) -> dict[str, Any]:
    module_name = module["id"]
    running = is_module_running(module_name)
    active_run = get_active_run(module_name)
    next_run = ""
    schedule = module.get("schedule") or ""
    schedule_enabled = bool(module.get("schedule_enabled"))
    if schedule and schedule_enabled:
        next_run = scheduled_next_run(module_name, schedule)

    return {
        "running": running,
        "scheduled": bool(schedule and schedule_enabled),
        "schedule": schedule,
        "schedule_enabled": schedule_enabled,
        "schedule_configured": bool(schedule),
        "next_run": next_run,
        "next_run_display": utils.format_datetime_for_display(next_run),
        "trigger": active_run.trigger if active_run is not None else "",
        "scheduled_for": active_run.scheduled_for if active_run is not None else "",
        "scheduled_for_display": utils.format_datetime_for_display(
            active_run.scheduled_for if active_run is not None else None
        ),
    }


def scheduled_next_run(module_name: str, schedule: str) -> str:
    now = utils.local_now()
    from .scheduler import SCHEDULER
    scheduler = SCHEDULER
    if scheduler is not None:
        next_run = scheduler.next_run_for(module_name)
        if next_run and utils.scheduled_time_is_future(next_run, now):
            return next_run
    try:
        return utils.next_schedule_time(schedule, now).isoformat()
    except ValueError:
        return ""


def is_module_running(module_name: str) -> bool:
    modules.ensure_module_exists(module_name)

    with ACTIVE_MODULES_LOCK:
        if module_name in ACTIVE_MODULES:
            return True

    lock = FileLock(config.LOCKS_DIR / f"{module_name}.lock")
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
    *,
    trigger: str = config.RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
) -> ActiveRun:
    if trigger not in config.RUN_TRIGGERS:
        raise ValueError(f"Invalid execution trigger: {trigger!r}.")
    plugin_statuses = {
        plugin["id"]: "enqueued"
        for plugin in plugins or []
        if isinstance(plugin, dict) and isinstance(plugin.get("id"), str)
    }
    active_run = ActiveRun(
        module_id=module_name,
        run_id=run_id,
        trigger=trigger,
        scheduled_for=scheduled_for,
        plugin_statuses=plugin_statuses,
    )
    with ACTIVE_MODULES_LOCK:
        ACTIVE_MODULES.add(module_name)
        ACTIVE_RUNS[module_name] = active_run
    write_active_run_file(active_run)
    events.signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["status"],
        reason="batch_started",
    )
    return active_run


def remove_active_run(module_name: str) -> None:
    with ACTIVE_MODULES_LOCK:
        ACTIVE_MODULES.discard(module_name)
        ACTIVE_RUNS.pop(module_name, None)
    active_run_path(module_name).unlink(missing_ok=True)
    events.signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["status"],
        reason="batch_finished",
    )


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
    events.signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["logs", "status"],
        reason="kill_requested",
    )


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
        payload_with_context = {
            "trigger": active_run.trigger,
            "scheduled_for": active_run.scheduled_for,
            **payload,
        }
        record = build_log_record(
            active_run.run_id,
            active_run.sequence,
            event,
            payload_with_context,
        )
        snapshot = serialize_active_run(active_run)
    append_module_log(module_name, record)
    write_active_run_snapshot(module_name, snapshot)
    events.signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["logs"],
        reason=f"batch_{event}",
    )
    return record


def active_run_path(module_name: str) -> Path:
    modules.ensure_module_exists(module_name)
    return config.TEMP_DIR / f"active_{module_name}.json"


def serialize_active_run(active_run: ActiveRun) -> dict[str, Any]:
    return {
        "module_id": active_run.module_id,
        "run_id": active_run.run_id,
        "trigger": active_run.trigger,
        "scheduled_for": active_run.scheduled_for,
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
    modules.ensure_module_exists(module_name)
    return config.TEMP_DIR / f"temp_{module_name}.jsonl"


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


def stream_batch(
    module: dict[str, Any],
    *,
    trigger: str = config.RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
) -> Iterator[str]:
    module_id = module["id"]
    lock_path = config.LOCKS_DIR / f"{module_id}.lock"
    lock = FileLock(lock_path)

    try:
        lock.acquire(blocking=False)
    except Timeout:
        yield utils.sse(
            "done",
            {
                "status": "locked",
                "level": "error",
                "trigger": trigger,
                "scheduled_for": scheduled_for,
                "message": f"Module {module['name']} is already running.",
            },
        )
        return

    run_id = uuid4().hex
    clear_module_log(module_id)
    active_run = create_active_run(
        module_id,
        run_id,
        module["plugins"],
        trigger=trigger,
        scheduled_for=scheduled_for,
    )

    def emit(event: str, payload: dict[str, Any]) -> str:
        record = append_active_log(module_id, event, payload)
        return utils.sse(event, record)

    try:
        yield emit(
            "status",
            {
                "level": "info",
                "message": (
                    f"Starting scheduled batch {module['name']}."
                    if trigger == config.RUN_TRIGGER_SCHEDULE
                    else f"Starting batch {module['name']}."
                ),
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
                        "message": f"Batch {module['name']} interrupted by the user.",
                    },
                )
                return

            plugin_id = plugin_config["id"]
            try:
                plugin = plugin_registry.create_plugin(plugin_config, module.get("variables", {}))
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
                    "message": f"Running plugin {plugin_id}.",
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
                    "message": f"Plugin {plugin_id} completed.",
                },
            )

        if get_active_kill_requested(module_id):
            yield emit(
                "done",
                {
                    "status": "killed",
                    "level": "error",
                    "plugin_statuses": get_plugin_statuses(module_id),
                    "message": f"Batch {module['name']} interrupted by the user.",
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
                "message": f"Batch {module['name']} completed successfully.",
            },
        )
    except Exception as exc:
        yield emit(
            "done",
            {
                "status": "failed",
                "level": "error",
                "plugin_statuses": get_plugin_statuses(module_id),
                "message": f"Unexpected error: {exc}",
            },
        )
    finally:
        remove_active_run(module_id)
        lock.release()


_BATCH_STREAM_DONE = object()


def start_detached_batch(
    module: dict[str, Any],
    *,
    trigger: str = config.RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
    event_queue: queue.Queue[str | object] | None = None,
    client_closed: threading.Event | None = None,
) -> threading.Thread:
    def enqueue(event: str | object) -> None:
        if event_queue is None:
            return
        if client_closed is None or not client_closed.is_set():
            event_queue.put(event)

    def worker() -> None:
        try:
            for event in stream_batch(
                module,
                trigger=trigger,
                scheduled_for=scheduled_for,
            ):
                enqueue(event)
        finally:
            enqueue(_BATCH_STREAM_DONE)

    thread = threading.Thread(
        target=worker,
        name=f"batch-runner-{trigger}-{module['id']}",
        daemon=True,
    )
    thread.start()
    return thread


def stream_detached_batch(
    module: dict[str, Any],
    *,
    trigger: str = config.RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
    heartbeat_seconds: float = config.SSE_HEARTBEAT_SECONDS,
) -> Iterator[str]:
    event_queue: queue.Queue[str | object] = queue.Queue()
    client_closed = threading.Event()
    start_detached_batch(
        module,
        trigger=trigger,
        scheduled_for=scheduled_for,
        event_queue=event_queue,
        client_closed=client_closed,
    )

    try:
        while True:
            try:
                event = event_queue.get(timeout=heartbeat_seconds)
            except queue.Empty:
                yield ": heartbeat\n\n"
                continue
            if event is _BATCH_STREAM_DONE:
                return
            yield str(event)
    finally:
        client_closed.set()
