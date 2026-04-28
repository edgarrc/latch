from __future__ import annotations

import app as app_module
import json
import sys
from typing import Iterator

import pytest

from app import (
    active_run_path,
    app,
    create_active_run,
    get_active_kill_requested,
    load_module_config,
    read_module_log,
    remove_active_run,
    set_active_plugin,
    stream_batch,
)
from plugins.base import BasePlugin, PluginEvent, PluginKilledError


class FakeKillablePlugin(BasePlugin):
    def __init__(self) -> None:
        super().__init__("fake_plugin", {"type": "fake"})
        self.killed = False

    def run(self) -> Iterator[PluginEvent]:
        yield PluginEvent("info", "fake")

    def kill(self) -> None:
        self.killed = True


class FakeKilledPlugin(BasePlugin):
    def __init__(self, plugin_id: str) -> None:
        super().__init__(plugin_id, {"type": "fake"})

    def run(self) -> Iterator[PluginEvent]:
        raise PluginKilledError("fake killed")
        yield PluginEvent("info", "unreachable")

    def kill(self) -> None:
        return


def parse_sse_data(event: str) -> dict[str, object]:
    data_line = next(line for line in event.splitlines() if line.startswith("data: "))
    return json.loads(data_line.removeprefix("data: "))


def test_load_module_config_reads_configured_plugins() -> None:
    module = load_module_config("tri")

    assert module["id"] == "tri"
    assert module["plugins"][0]["id"] == "preparar_tri"


def test_module_page_renders_plugin_status_column() -> None:
    client = app.test_client()
    response = client.get("/tri")

    assert response.status_code == 200
    assert b"Status" in response.data
    assert b"data-plugin-id=\"preparar_tri\"" in response.data
    assert "Não iniciado".encode() in response.data


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


def test_stream_batch_emits_plugin_statuses_on_success() -> None:
    events = [parse_sse_data(event) for event in stream_batch(load_module_config("tri"))]

    status_event = events[0]
    first_plugin_start = next(
        event for event in events if event["event"] == "plugin_start"
    )
    first_plugin_done = next(
        event
        for event in events
        if event["event"] == "plugin_done" and event["plugin"] == "preparar_tri"
    )
    done_event = events[-1]

    assert status_event["plugin_statuses"] == {
        "preparar_tri": "enqueued",
        "processar_tri": "enqueued",
    }
    assert first_plugin_start["plugin_statuses"]["preparar_tri"] == "running"
    assert first_plugin_done["plugin_statuses"]["preparar_tri"] == "success"
    assert done_event["plugin_statuses"] == {
        "preparar_tri": "success",
        "processar_tri": "success",
    }


def test_stream_batch_keeps_future_plugins_enqueued_after_failure() -> None:
    module = {
        "id": "tri",
        "name": "TRI",
        "variables": {},
        "plugins": [
            {
                "id": "falhar",
                "type": "command_line",
                "command": [
                    sys.executable,
                    "-c",
                    "import sys; sys.exit(2)",
                ],
            },
            {
                "id": "nao_executar",
                "type": "command_line",
                "command": "echo nao deve executar",
            },
        ],
    }

    events = [parse_sse_data(event) for event in stream_batch(module)]
    done_event = events[-1]

    assert done_event["status"] == "failed"
    assert done_event["plugin"] == "falhar"
    assert done_event["plugin_statuses"] == {
        "falhar": "failed",
        "nao_executar": "enqueued",
    }


def test_stream_batch_keeps_future_plugins_enqueued_after_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_create_plugin(
        plugin_config: dict[str, object],
        module_variables: dict[str, dict[str, object]] | None = None,
    ) -> BasePlugin:
        return FakeKilledPlugin(str(plugin_config["id"]))

    monkeypatch.setattr(app_module, "create_plugin", fake_create_plugin)
    module = {
        "id": "tri",
        "name": "TRI",
        "variables": {},
        "plugins": [
            {
                "id": "interromper",
                "type": "fake",
            },
            {
                "id": "nao_executar",
                "type": "fake",
            },
        ],
    }

    events = [parse_sse_data(event) for event in stream_batch(module)]
    done_event = events[-1]

    assert done_event["status"] == "killed"
    assert done_event["plugin"] == "interromper"
    assert done_event["plugin_statuses"] == {
        "interromper": "killed",
        "nao_executar": "enqueued",
    }


def test_active_run_file_persists_plugin_statuses_during_execution() -> None:
    events = stream_batch(load_module_config("tri"))
    first_event = parse_sse_data(next(events))

    try:
        active_snapshot = json.loads(active_run_path("tri").read_text(encoding="utf-8"))
        second_event = parse_sse_data(next(events))
        running_snapshot = json.loads(active_run_path("tri").read_text(encoding="utf-8"))
    finally:
        events.close()

    assert first_event["plugin_statuses"] == {
        "preparar_tri": "enqueued",
        "processar_tri": "enqueued",
    }
    assert active_snapshot["plugin_statuses"] == first_event["plugin_statuses"]
    assert second_event["event"] == "plugin_start"
    assert running_snapshot["plugin_statuses"]["preparar_tri"] == "running"


def test_stream_batch_masks_sensitive_values_in_persisted_log() -> None:
    module = {
        "id": "tri",
        "name": "TRI",
        "variables": {
            "password": {
                "type": "sensitive",
                "value": "secret-value",
            }
        },
        "plugins": [
            {
                "id": "leak_secret",
                "type": "command_line",
                "command": [
                    sys.executable,
                    "-c",
                    "import sys; print(sys.argv[1])",
                    "{password}",
                ],
            }
        ],
    }

    list(stream_batch(module))
    records = read_module_log("tri")
    serialized_records = json.dumps(records, ensure_ascii=False)

    assert "secret-value" not in serialized_records
    assert "****" in serialized_records
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
