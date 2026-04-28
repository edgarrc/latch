from __future__ import annotations

import app as app_module
import json
import sys
from typing import Iterator

import pytest
from werkzeug.security import check_password_hash

from app import (
    active_run_path,
    app,
    create_active_run,
    discover_module_names,
    get_active_kill_requested,
    load_module_config,
    read_module_log,
    remove_active_run,
    set_active_plugin,
    stream_batch,
)
from plugins.base import BasePlugin, PluginEvent, PluginKilledError


@pytest.fixture(autouse=True)
def disable_auth_for_existing_tests() -> Iterator[None]:
    previous_auth_disabled = app.config.get("AUTH_DISABLED")
    previous_secret_key = app.secret_key
    app.config["AUTH_DISABLED"] = True
    yield
    if previous_auth_disabled is None:
        app.config.pop("AUTH_DISABLED", None)
    else:
        app.config["AUTH_DISABLED"] = previous_auth_disabled
    app.secret_key = previous_secret_key


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


def enable_auth_with_settings_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "SETTINGS_PATH", tmp_path / "settings.yaml")
    app.config["AUTH_DISABLED"] = False
    app.secret_key = "test-secret"


def write_auth_settings(password: str = "secret") -> dict[str, str]:
    settings = app_module.build_settings(password)
    app_module.write_settings(settings)
    app.secret_key = settings["secret_key"]
    return settings


def test_missing_settings_redirects_to_setup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)

    client = app.test_client()

    response = client.get("/")
    setup_response = client.get("/setup")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/setup")
    assert setup_response.status_code == 200
    assert b"Setup inicial" in setup_response.data


def test_setup_creates_settings_hash_and_authenticates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)

    client = app.test_client()
    response = client.post(
        "/setup",
        data={"password": "secret", "password_confirm": "secret"},
    )

    settings = app_module.load_settings()

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert settings is not None
    assert settings["username"] == "admin"
    assert settings["password_hash"] != "secret"
    assert check_password_hash(settings["password_hash"], "secret")
    assert settings["secret_key"]
    with client.session_transaction() as client_session:
        assert client_session["user"] == "admin"
        assert client_session.permanent is True


def test_login_required_for_html_when_settings_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()

    client = app.test_client()
    response = client.get("/modules/new")

    assert response.status_code == 302
    assert "/login?next=/modules/new" in response.headers["Location"]


def test_login_required_for_api_when_settings_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()

    client = app.test_client()
    response = client.get("/api/modules/status")

    assert response.status_code == 401
    assert response.get_json()["authenticated"] is False


def test_login_rejects_invalid_password(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings("secret")

    client = app.test_client()
    response = client.post(
        "/login",
        data={"username": "admin", "password": "wrong"},
    )

    assert response.status_code == 200
    assert "Usuário ou senha inválidos.".encode() in response.data
    with client.session_transaction() as client_session:
        assert "user" not in client_session


def test_login_accepts_valid_password_and_logout_clears_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings("secret")

    client = app.test_client()
    login_response = client.post(
        "/login?next=/modules/new",
        data={"username": "admin", "password": "secret", "next": "/modules/new"},
    )

    assert login_response.status_code == 302
    assert login_response.headers["Location"].endswith("/modules/new")
    with client.session_transaction() as client_session:
        assert client_session["user"] == "admin"
        assert client_session.permanent is True

    protected_response = client.get("/")
    logout_response = client.post("/logout")

    assert protected_response.status_code == 200
    assert logout_response.status_code == 302
    assert logout_response.headers["Location"].endswith("/login")
    with client.session_transaction() as client_session:
        assert "user" not in client_session


def test_load_module_config_reads_configured_plugins() -> None:
    module = load_module_config("tri")

    assert module["id"] == "tri"
    assert module["description"]
    assert module["plugins"][0]["id"] == "preparar_tri"
    assert module["plugins"][0]["description"]


def test_discover_module_names_reads_yaml_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(app_module, "MODULES_DIR", tmp_path)
    (tmp_path / "zeta.yaml").write_text("name: Zeta\nplugins: []\n", encoding="utf-8")
    (tmp_path / "alpha.yaml").write_text("name: Alpha\nplugins: []\n", encoding="utf-8")
    (tmp_path / "bad.name.yaml").write_text("name: Bad\nplugins: []\n", encoding="utf-8")

    assert discover_module_names() == ["alpha", "zeta"]


def test_module_page_renders_plugin_status_column() -> None:
    client = app.test_client()
    response = client.get("/tri")

    assert response.status_code == 200
    assert b"Status" in response.data
    assert b"Descri" in response.data
    assert b"data-plugin-id=\"preparar_tri\"" in response.data
    assert "Não iniciado".encode() in response.data


def test_index_renders_add_and_edit_actions() -> None:
    client = app.test_client()
    response = client.get("/")

    assert response.status_code == 200
    assert b"Adicionar" in response.data
    assert b"/modules/tri/edit" in response.data


def test_validate_module_endpoint_rejects_invalid_plugin_type() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: desconhecido\n"
            ),
        },
    )

    assert response.status_code == 400
    assert response.get_json()["valid"] is False


def test_create_module_endpoint_persists_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(app_module, "MODULES_DIR", tmp_path)

    client = app.test_client()
    response = client.post(
        "/api/modules",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "description: Modulo novo\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    description: Etapa inicial\n"
                "    command: echo ok\n"
            ),
        },
    )

    assert response.status_code == 201
    assert response.get_json()["saved"] is True
    assert (tmp_path / "novo.yaml").exists()
    assert "description: Modulo novo" in (tmp_path / "novo.yaml").read_text(
        encoding="utf-8"
    )


def test_update_module_endpoint_blocks_running_module() -> None:
    create_active_run("tri", "test-run")

    try:
        client = app.test_client()
        response = client.put(
            "/api/modules/tri",
            json={
                "content": (
                    "name: TRI\n"
                    "plugins:\n"
                    "  - id: etapa\n"
                    "    type: command_line\n"
                    "    command: echo ok\n"
                ),
            },
        )
    finally:
        remove_active_run("tri")

    assert response.status_code == 409
    assert response.get_json()["saved"] is False


def test_delete_module_endpoint_removes_module_and_temporary_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    modules_dir = tmp_path / "modules"
    temp_dir = tmp_path / "temp"
    locks_dir = tmp_path / "locks"
    modules_dir.mkdir()
    temp_dir.mkdir()
    locks_dir.mkdir()
    monkeypatch.setattr(app_module, "MODULES_DIR", modules_dir)
    monkeypatch.setattr(app_module, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(app_module, "LOCKS_DIR", locks_dir)

    module_path = modules_dir / "remover.yaml"
    log_path = temp_dir / "temp_remover.jsonl"
    active_path = temp_dir / "active_remover.json"
    lock_path = locks_dir / "remover.lock"
    module_path.write_text(
        "name: Remover\nplugins:\n  - id: etapa\n    type: command_line\n    command: echo ok\n",
        encoding="utf-8",
    )
    log_path.write_text("log\n", encoding="utf-8")
    active_path.write_text("{}", encoding="utf-8")
    lock_path.write_text("", encoding="utf-8")

    client = app.test_client()
    response = client.delete("/api/modules/remover")

    assert response.status_code == 200
    assert response.get_json()["deleted"] is True
    assert not module_path.exists()
    assert not log_path.exists()
    assert not active_path.exists()
    assert not lock_path.exists()


def test_delete_module_endpoint_blocks_running_module() -> None:
    create_active_run("tri", "test-run")

    try:
        client = app.test_client()
        response = client.delete("/api/modules/tri")
    finally:
        remove_active_run("tri")

    assert response.status_code == 409
    assert response.get_json()["deleted"] is False


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
