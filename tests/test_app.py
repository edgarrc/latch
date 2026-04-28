from __future__ import annotations

from typing import Iterator

from app import (
    app,
    create_active_run,
    get_active_kill_requested,
    load_module_config,
    read_module_log,
    remove_active_run,
    set_active_plugin,
    stream_batch,
)
from plugins.base import BasePlugin, PluginEvent


class FakeKillablePlugin(BasePlugin):
    def __init__(self) -> None:
        super().__init__("fake_plugin", {"type": "fake"})
        self.killed = False

    def run(self) -> Iterator[PluginEvent]:
        yield PluginEvent("info", "fake")

    def kill(self) -> None:
        self.killed = True


def test_load_module_config_reads_configured_plugins() -> None:
    module = load_module_config("tri")

    assert module["id"] == "tri"
    assert module["plugins"][0]["id"] == "preparar_tri"


def test_stream_batch_reports_locked_module() -> None:
    module = load_module_config("tri")
    events = stream_batch(module)
    next(events)

    try:
        nested_events = list(stream_batch(module))
    finally:
        events.close()

    assert any('"status": "locked"' in event for event in nested_events)


def test_module_locks_are_independent() -> None:
    tri_events = stream_batch(load_module_config("tri"))
    next(tri_events)

    try:
        analitico_events = list(stream_batch(load_module_config("analitico")))
    finally:
        tri_events.close()

    assert any('"status": "success"' in event for event in analitico_events)
    assert not any('"status": "locked"' in event for event in analitico_events)


def test_stream_batch_persists_last_execution_log() -> None:
    list(stream_batch(load_module_config("tri")))

    records = read_module_log("tri")

    assert records[0]["event"] == "status"
    assert records[0]["run_id"]
    assert records[-1]["event"] == "done"
    assert records[-1]["status"] == "success"


def test_module_logs_endpoint_returns_persisted_events() -> None:
    list(stream_batch(load_module_config("tri")))

    client = app.test_client()
    response = client.get("/api/modules/tri/logs")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["id"] == "tri"
    assert payload["run_id"]
    assert payload["latest_sequence"] >= 1
    assert payload["events"][0]["event"] == "status"


def test_module_logs_endpoint_resets_when_new_run_replaces_previous_log() -> None:
    list(stream_batch(load_module_config("tri")))
    previous_records = read_module_log("tri")
    previous_run_id = previous_records[-1]["run_id"]
    previous_sequence = previous_records[-1]["sequence"]

    list(stream_batch(load_module_config("tri")))

    client = app.test_client()
    response = client.get(
        f"/api/modules/tri/logs?run_id={previous_run_id}&since={previous_sequence}"
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["reset"] is True
    assert payload["run_id"] != previous_run_id
    assert payload["events"][0]["sequence"] == 1


def test_clear_module_logs_when_module_is_not_running() -> None:
    list(stream_batch(load_module_config("tri")))

    client = app.test_client()
    response = client.post("/api/modules/tri/logs/clear")

    assert response.status_code == 200
    assert response.get_json()["cleared"] is True
    assert read_module_log("tri") == []


def test_clear_module_logs_is_blocked_while_module_is_running() -> None:
    events = stream_batch(load_module_config("tri"))
    next(events)

    try:
        client = app.test_client()
        response = client.post("/api/modules/tri/logs/clear")
    finally:
        events.close()

    assert response.status_code == 409
    assert response.get_json()["cleared"] is False


def test_kill_module_returns_conflict_when_module_is_not_running() -> None:
    client = app.test_client()
    response = client.post("/api/modules/tri/kill")

    assert response.status_code == 409
    assert response.get_json()["killed"] is False


def test_kill_module_calls_active_plugin_kill() -> None:
    plugin = FakeKillablePlugin()
    create_active_run("tri", "test-run")
    set_active_plugin("tri", plugin, {"type": "fake"})

    try:
        client = app.test_client()
        response = client.post("/api/modules/tri/kill")
        kill_requested = get_active_kill_requested("tri")
    finally:
        remove_active_run("tri")

    assert response.status_code == 200
    assert response.get_json()["killed"] is True
    assert plugin.killed is True
    assert kill_requested is True
