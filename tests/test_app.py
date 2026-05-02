from __future__ import annotations

import app as app_module
import configparser
import json
import queue
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest
from werkzeug.security import check_password_hash

from app import (
    active_run_path,
    app,
    clear_generated_temp_files,
    create_active_run,
    discover_module_names,
    get_active_kill_requested,
    load_module_config,
    load_system_module_config,
    read_module_log,
    remove_active_run,
    set_active_plugin,
    ModuleScheduler,
    RUN_TRIGGER_SCHEDULE,
    stream_batch,
    stream_detached_batch,
)
from plugins.base import BasePlugin, PluginEvent, PluginKilledError


@pytest.fixture(autouse=True)
def disable_auth_for_existing_tests() -> Iterator[None]:
    previous_auth_disabled = app.config.get("AUTH_DISABLED")
    previous_scheduler_disabled = app.config.get("SCHEDULER_DISABLED")
    previous_secret_key = app.secret_key
    app.config["AUTH_DISABLED"] = True
    app.config["SCHEDULER_DISABLED"] = True
    yield
    if previous_auth_disabled is None:
        app.config.pop("AUTH_DISABLED", None)
    else:
        app.config["AUTH_DISABLED"] = previous_auth_disabled
    if previous_scheduler_disabled is None:
        app.config.pop("SCHEDULER_DISABLED", None)
    else:
        app.config["SCHEDULER_DISABLED"] = previous_scheduler_disabled
    app.secret_key = previous_secret_key


class FakeKillablePlugin(BasePlugin):
    def __init__(self) -> None:
        super().__init__("fake_plugin", {"type": "fake"})
        self.killed = False

    def _run_once(self) -> Iterator[PluginEvent]:
        yield PluginEvent("info", "fake")

    def kill(self) -> None:
        self.killed = True


def test_uwsgi_config_keeps_single_process_threaded_runtime() -> None:
    config_path = Path(__file__).resolve().parents[1] / "uwsgi.ini"
    config = configparser.ConfigParser()

    assert config.read(config_path) == [str(config_path)]
    uwsgi_config = config["uwsgi"]
    assert uwsgi_config["module"] == "app:app"
    assert uwsgi_config.getint("processes") == 1
    assert uwsgi_config.getint("threads") >= 16
    assert uwsgi_config.getboolean("enable-threads") is True
    assert uwsgi_config.getboolean("lazy-apps") is True
    assert uwsgi_config.getint("http-timeout") >= 604800
    assert uwsgi_config.getint("socket-timeout") >= 604800
    assert uwsgi_config.getboolean("ignore-sigpipe") is True
    assert uwsgi_config.getboolean("ignore-write-errors") is True
    assert uwsgi_config.getboolean("disable-write-exception") is True


class FakeKilledPlugin(BasePlugin):
    def __init__(self, plugin_id: str) -> None:
        super().__init__(plugin_id, {"type": "fake"})

    def _run_once(self) -> Iterator[PluginEvent]:
        raise PluginKilledError("fake killed")
        yield PluginEvent("info", "unreachable")

    def kill(self) -> None:
        return


class BlockingPlugin(BasePlugin):
    def __init__(
        self,
        plugin_id: str,
        started: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(plugin_id, {"type": "fake"})
        self.started = started
        self.release = release

    def _run_once(self) -> Iterator[PluginEvent]:
        self.started.set()
        yield PluginEvent("info", "plugin bloqueado")
        self.release.wait(timeout=2)
        yield PluginEvent("info", "plugin liberado")

    def kill(self) -> None:
        return


class SilentBlockingPlugin(BasePlugin):
    def __init__(
        self,
        plugin_id: str,
        started: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(plugin_id, {"type": "fake"})
        self.started = started
        self.release = release

    def _run_once(self) -> Iterator[PluginEvent]:
        self.started.set()
        self.release.wait(timeout=2)
        yield PluginEvent("info", "plugin liberado")

    def kill(self) -> None:
        return


def parse_sse_data(event: str) -> dict[str, object]:
    data_line = next(line for line in event.splitlines() if line.startswith("data: "))
    return json.loads(data_line.removeprefix("data: "))


def wait_for_event(subscriber: queue.Queue[app_module.AppEvent]) -> app_module.AppEvent:
    return subscriber.get(timeout=1)


def wait_for_event_reason(
    subscriber: queue.Queue[app_module.AppEvent],
    reason: str,
) -> app_module.AppEvent:
    deadline = time.monotonic() + 1
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise queue.Empty
        event = subscriber.get(timeout=remaining)
        if event.reason == reason:
            return event


def assert_latch_branding(response) -> None:
    assert b"Latch" in response.data
    assert app_module.APP_GITHUB_URL.encode() in response.data


def enable_auth_with_settings_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "SETTINGS_PATH", tmp_path / "settings.yaml")
    app.config["AUTH_DISABLED"] = False
    app.secret_key = "test-secret"


def write_auth_settings(
    admin_password: str = "secret",
    user_password: str = "user-secret",
) -> dict[str, object]:
    settings = app_module.build_settings(admin_password, user_password)
    app_module.write_settings(settings)
    app.secret_key = settings["secret_key"]
    return settings


def authenticate_client(client, username: str = "admin") -> None:
    with client.session_transaction() as client_session:
        client_session["user"] = username
        client_session.permanent = True


def configure_temp_runtime_dirs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    modules_root = tmp_path / "modules"
    user_modules_dir = modules_root / "user"
    system_modules_dir = modules_root / "system"
    temp_dir = tmp_path / "temp"
    locks_dir = tmp_path / "locks"
    user_modules_dir.mkdir(parents=True)
    system_modules_dir.mkdir(parents=True)
    temp_dir.mkdir()
    locks_dir.mkdir()
    monkeypatch.setattr(app_module, "MODULES_ROOT", modules_root)
    monkeypatch.setattr(app_module, "USER_MODULES_DIR", user_modules_dir)
    monkeypatch.setattr(app_module, "SYSTEM_MODULES_DIR", system_modules_dir)
    monkeypatch.setattr(app_module, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(app_module, "LOCKS_DIR", locks_dir)
    return user_modules_dir, system_modules_dir, temp_dir, locks_dir


def write_public_test_module(user_modules_dir, module_id: str = "publico") -> None:
    user_modules_dir.joinpath(f"{module_id}.yaml").write_text(
        (
            "name: Publico\n"
            "description: Modulo publico temporario.\n"
            "plugins:\n"
            "  - id: preparar_publico\n"
            "    type: command_line\n"
            "    description: Etapa publica temporaria.\n"
            "    command: echo preparar_publico concluido\n"
            "    error_contains: ERROR\n"
            "    success_contains: concluido\n"
            "  - id: processar_publico\n"
            "    type: command_line\n"
            "    description: Segunda etapa publica temporaria.\n"
            "    command: echo processar_publico concluido\n"
            "    error_contains: ERROR\n"
            "    success_contains: concluido\n"
        ),
        encoding="utf-8",
    )


def test_app_event_hub_publishes_and_replays_events() -> None:
    event_hub = app_module.AppEventHub(history_size=10)
    subscriber = event_hub.subscribe()

    event = event_hub.publish(
        scope="module",
        module_id="teste_automatizado",
        resources=["logs"],
        reason="test_event",
    )

    assert wait_for_event(subscriber) == event
    event_hub.unsubscribe(subscriber)

    replay_subscriber = event_hub.subscribe(str(event.id - 1))
    assert wait_for_event(replay_subscriber) == event
    event_hub.unsubscribe(replay_subscriber)

    current_subscriber = event_hub.subscribe(str(event.id))
    with pytest.raises(queue.Empty):
        current_subscriber.get(timeout=0.01)
    event_hub.unsubscribe(current_subscriber)


def test_application_monitor_coalesces_updates_by_scope_and_module() -> None:
    event_hub = app_module.AppEventHub()
    monitor = app_module.ApplicationMonitor(event_hub, debounce_seconds=0.01)
    subscriber = event_hub.subscribe()

    monitor.signal(
        scope="module",
        module_id="teste_automatizado",
        resources=["logs"],
        reason="batch_log",
    )
    monitor.signal(
        scope="module",
        module_id="teste_automatizado",
        resources=["status"],
        reason="batch_started",
    )

    event = wait_for_event(subscriber)

    assert event.scope == "module"
    assert event.module_id == "teste_automatizado"
    assert event.resources == ["logs", "status"]
    assert event.reason == "batch_started"
    event_hub.unsubscribe(subscriber)


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
    assert b"Initial setup" in setup_response.data
    assert_latch_branding(setup_response)


def test_setup_creates_settings_hash_and_authenticates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)

    client = app.test_client()
    response = client.post(
        "/setup",
        data={
            "admin_password": "admin-secret",
            "admin_password_confirm": "admin-secret",
            "user_password": "user-secret",
            "user_password_confirm": "user-secret",
        },
    )

    settings = app_module.load_settings()

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert settings is not None
    assert set(settings["users"]) == {"admin", "user"}
    assert settings["users"]["admin"]["password_hash"] != "admin-secret"
    assert settings["users"]["user"]["password_hash"] != "user-secret"
    assert check_password_hash(
        settings["users"]["admin"]["password_hash"],
        "admin-secret",
    )
    assert check_password_hash(
        settings["users"]["user"]["password_hash"],
        "user-secret",
    )
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


def test_login_page_renders_latch_branding_and_footer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()

    client = app.test_client()
    response = client.get("/login")

    assert response.status_code == 200
    assert_latch_branding(response)


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


def test_login_required_for_global_events_when_settings_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()

    client = app.test_client()
    response = client.get("/api/events")

    assert response.status_code == 401
    assert response.get_json()["authenticated"] is False


def test_global_events_endpoint_streams_published_updates() -> None:
    client = app.test_client()
    event = app_module.EVENT_HUB.publish(
        scope="module",
        module_id="teste_automatizado",
        resources=["logs"],
        reason="test_stream",
    )
    response = client.get(
        "/api/events",
        buffered=False,
        headers={"Last-Event-ID": str(event.id - 1)},
    )

    try:
        chunk = next(response.response).decode()
    finally:
        response.close()

    assert response.status_code == 200
    assert response.mimetype == "text/event-stream"
    assert f"id: {event.id}" in chunk
    assert "event: app_update" in chunk
    payload = parse_sse_data(chunk)
    assert payload["scope"] == "module"
    assert payload["module_id"] == "teste_automatizado"
    assert payload["resources"] == ["logs"]


@pytest.mark.parametrize("username", ["admin", "user"])
def test_login_rejects_invalid_password(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    username: str,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings("secret", "user-secret")

    client = app.test_client()
    response = client.post(
        "/login",
        data={"username": username, "password": "wrong"},
    )

    assert response.status_code == 200
    assert b"Invalid username or password." in response.data
    with client.session_transaction() as client_session:
        assert "user" not in client_session


def test_login_accepts_admin_password_and_logout_clears_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings("secret", "user-secret")

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


def test_login_accepts_user_password(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings("secret", "user-secret")

    client = app.test_client()
    login_response = client.post(
        "/login?next=/",
        data={"username": "user", "password": "user-secret", "next": "/"},
    )

    assert login_response.status_code == 302
    assert login_response.headers["Location"].endswith("/")
    with client.session_transaction() as client_session:
        assert client_session["user"] == "user"
        assert client_session.permanent is True


def test_load_module_config_reads_configured_plugins() -> None:
    module = load_system_module_config("teste_automatizado")

    assert module["id"] == "teste_automatizado"
    assert module["description"]
    assert module["plugins"][0]["id"] == "preparar_teste"
    assert module["plugins"][0]["description"]


def test_load_module_config_returns_normalized_schedule_without_rewriting_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    content = (
        "name: Agendado\n"
        "schedule_enabled: true\n"
        "schedule: \"  0 * * * *  \"\n"
        "plugins:\n"
        "  - id: etapa\n"
        "    type: command_line\n"
        "    command: echo ok\n"
    )
    module_path = user_modules_dir / "agendado.yaml"
    module_path.write_text(content, encoding="utf-8")

    module = load_module_config("agendado")

    assert module["schedule"] == "0 * * * *"
    assert module["schedule_enabled"] is True
    assert module_path.read_text(encoding="utf-8") == content


def test_load_module_config_keeps_disabled_schedule(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    user_modules_dir.joinpath("agendado.yaml").write_text(
        (
            "name: Agendado\n"
            "schedule_enabled: false\n"
            "schedule: \"* * * * *\"\n"
            "plugins:\n"
            "  - id: etapa\n"
            "    type: command_line\n"
            "    command: echo ok\n"
        ),
        encoding="utf-8",
    )

    module = load_module_config("agendado")

    assert module["schedule"] == "* * * * *"
    assert module["schedule_enabled"] is False


def test_clear_generated_temp_files_removes_module_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    log_path = temp_dir / "temp_publico.jsonl"
    active_path = temp_dir / "active_publico.json"
    unrelated_path = temp_dir / "keep.txt"
    log_path.write_text("log\n", encoding="utf-8")
    active_path.write_text("{}", encoding="utf-8")
    unrelated_path.write_text("keep\n", encoding="utf-8")
    monkeypatch.setattr(app_module, "TEMP_DIR", temp_dir)

    clear_generated_temp_files()

    assert not log_path.exists()
    assert not active_path.exists()
    assert unrelated_path.exists()


def test_discover_module_names_reads_yaml_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    (user_modules_dir / "zeta.yaml").write_text("name: Zeta\nplugins: []\n", encoding="utf-8")
    (user_modules_dir / "alpha.yaml").write_text("name: Alpha\nplugins: []\n", encoding="utf-8")
    (user_modules_dir / "bad.name.yaml").write_text("name: Bad\nplugins: []\n", encoding="utf-8")
    (system_modules_dir / "teste_automatizado.yaml").write_text(
        "name: Sistema\nplugins: []\n",
        encoding="utf-8",
    )

    assert discover_module_names() == ["alpha", "zeta"]


def test_system_module_is_hidden_from_public_surfaces() -> None:
    client = app.test_client()

    index_response = client.get("/")
    status_response = client.get("/api/modules/status")

    assert index_response.status_code == 200
    assert b"teste_automatizado" not in index_response.data
    assert "teste_automatizado" not in status_response.get_json()["modules"]
    assert client.get("/teste_automatizado").status_code == 404
    assert client.get("/modules/teste_automatizado/edit").status_code == 404
    assert client.get("/api/modules/teste_automatizado/status").status_code == 404
    assert client.get("/api/modules/teste_automatizado/logs").status_code == 404
    assert client.get("/api/modules/teste_automatizado/run").status_code == 404
    assert client.delete("/api/modules/teste_automatizado").status_code == 404


def test_module_page_renders_plugin_status_column(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    response = client.get("/publico")

    assert response.status_code == 200
    assert b"Status" in response.data
    assert b"Description" in response.data
    assert b"data-plugin-id=\"preparar_publico\"" in response.data
    assert b"Not started" in response.data


def test_module_page_renders_schedule_notice(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    module_path = user_modules_dir / "publico.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: true\n"
                "schedule: \"0 * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )

    client = app.test_client()
    response = client.get("/publico")

    assert response.status_code == 200
    assert b"Scheduled" in response.data
    assert b"0 * * * *" in response.data
    assert b"nextRunText" in response.data
    assert b"Next run" in response.data


def test_index_renders_add_and_edit_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    response = client.get("/")

    assert response.status_code == 200
    assert b"Add module" in response.data
    assert b"/modules/publico/edit" in response.data


def test_user_index_shows_script_view_without_edit_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    authenticate_client(client, "user")
    response = client.get("/")

    assert response.status_code == 200
    assert b"user" in response.data
    assert b"Add module" not in response.data
    assert b"Edit" not in response.data
    assert b"View script" in response.data
    assert b"/modules/publico/edit" in response.data


def test_user_can_view_module_yaml_readonly_but_cannot_access_edit_apis(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    authenticate_client(client, "user")
    edit_response = client.get("/modules/publico/edit")
    responses = [
        client.get("/modules/new"),
        client.post(
            "/api/modules/validate",
            json={"module_id": "publico", "content": "name: Publico\nplugins: []\n"},
        ),
        client.post(
            "/api/modules",
            json={"module_id": "novo", "content": "name: Novo\nplugins: []\n"},
        ),
        client.put(
            "/api/modules/publico",
            json={"content": "name: Publico\nplugins: []\n"},
        ),
        client.delete("/api/modules/publico"),
    ]

    assert edit_response.status_code == 200
    assert b"View Publico" in edit_response.data
    assert b"readonly" in edit_response.data
    assert b"Validate" not in edit_response.data
    assert b"Save" not in edit_response.data
    assert b"Delete" not in edit_response.data

    for response in responses:
        assert response.status_code == 403

    assert responses[1].get_json()["message"] == "Only admin can edit modules."
    assert (user_modules_dir / "publico.yaml").exists()


def test_user_module_yaml_view_masks_sensitive_variable_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    user_modules_dir.joinpath("seguro.yaml").write_text(
        (
            "name: Seguro\n"
            "variables:\n"
            "  senha:\n"
            "    type: sensitive\n"
            "    value: segredo-real\n"
            "  usuario:\n"
            "    type: string\n"
            "    value: analytics\n"
            "plugins:\n"
            "  - id: etapa\n"
            "    type: command_line\n"
            "    command: echo ok\n"
        ),
        encoding="utf-8",
    )

    client = app.test_client()
    authenticate_client(client, "admin")
    admin_response = client.get("/modules/seguro/edit")
    authenticate_client(client, "user")
    user_response = client.get("/modules/seguro/edit")

    assert admin_response.status_code == 200
    assert b"segredo-real" in admin_response.data
    assert user_response.status_code == 200
    assert b"segredo-real" not in user_response.data
    assert b"****" in user_response.data
    assert b"value: analytics" in user_response.data


def test_user_can_access_operational_module_surfaces(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    enable_auth_with_settings_path(monkeypatch, tmp_path)
    write_auth_settings()
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    authenticate_client(client, "user")
    responses = [
        client.get("/publico"),
        client.get("/api/modules/status"),
        client.get("/api/modules/publico/status"),
        client.get("/api/modules/publico/logs"),
        client.post("/api/modules/publico/logs/clear"),
        client.post("/api/modules/publico/kill"),
    ]

    assert [response.status_code for response in responses] == [200, 200, 200, 200, 200, 409]


def test_status_endpoints_include_schedule_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir)
    module_path = user_modules_dir / "publico.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: true\n"
                "schedule: \"0 * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )

    client = app.test_client()
    status_response = client.get("/api/modules/publico/status")
    all_status_response = client.get("/api/modules/status")

    assert status_response.status_code == 200
    assert status_response.get_json()["scheduled"] is True
    assert status_response.get_json()["schedule"] == "0 * * * *"
    assert status_response.get_json()["schedule_enabled"] is True
    assert status_response.get_json()["next_run"]
    assert all_status_response.get_json()["modules"]["publico"]["scheduled"] is True


def test_status_endpoints_keep_disabled_schedule_without_next_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir)
    module_path = user_modules_dir / "publico.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: false\n"
                "schedule: \"0 * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )

    client = app.test_client()
    status_response = client.get("/api/modules/publico/status")

    assert status_response.status_code == 200
    payload = status_response.get_json()
    assert payload["schedule_configured"] is True
    assert payload["schedule_enabled"] is False
    assert payload["scheduled"] is False
    assert payload["next_run"] == ""


def test_authenticated_pages_render_latch_branding_and_footer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    responses = [
        client.get("/"),
        client.get("/publico"),
        client.get("/modules/new"),
        client.get("/modules/publico/edit"),
    ]

    for response in responses:
        assert response.status_code == 200
        assert_latch_branding(response)


def test_pages_subscribe_to_global_events_instead_of_polling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()

    index_response = client.get("/")
    module_response = client.get("/publico")

    assert b'new EventSource("/api/events")' in index_response.data
    assert b'new EventSource("/api/events")' in module_response.data
    assert b"setInterval(refreshModuleStatuses" not in index_response.data
    assert b"setInterval(refreshModuleStatus" not in module_response.data
    assert b"setInterval(refreshModuleLogs" not in module_response.data


def test_module_page_renders_plugin_timeout_column(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    module_path = user_modules_dir / "publico.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "    command: echo preparar_publico concluido\n",
            (
                "    command: echo preparar_publico concluido\n"
                "    timeout: 30\n"
                "    timeout_retries: 1\n"
            ),
        ),
        encoding="utf-8",
    )

    client = app.test_client()
    response = client.get("/publico")

    assert response.status_code == 200
    assert b"Timeout" in response.data
    assert b"30s" in response.data
    assert b"1 retry" in response.data


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


def test_validate_module_endpoint_accepts_plugin_timeout_config() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
                "    timeout: 30\n"
                "    timeout_retries: 1\n"
            ),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["valid"] is True


def test_validate_module_endpoint_rejects_invalid_plugin_timeout_config() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
                "    timeout: 0\n"
            ),
        },
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload["valid"] is False
    assert "timeout" in payload["message"]


def test_validate_module_endpoint_accepts_client_plugins() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "variables:\n"
                "  clickhouse_user:\n"
                "    type: string\n"
                "    value: analytics\n"
                "  clickhouse_password:\n"
                "    type: sensitive\n"
                "    value: secret-value\n"
                "plugins:\n"
                "  - id: consultar\n"
                "    type: clickhouse_client\n"
                "    user: \"{clickhouse_user}\"\n"
                "    password: \"{clickhouse_password}\"\n"
                "    database: default\n"
                "    query: SELECT 1\n"
                "    error_contains: ERROR\n"
                "    success_contains: null\n"
                "  - id: scan\n"
                "    type: redis_client\n"
                "    host: redis.youeduc.com.br\n"
                "    args:\n"
                "      - --scan\n"
                "      - --pattern\n"
                "      - exp_*\n"
                "    error_contains: ERROR\n"
            ),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["valid"] is True


def test_validate_module_endpoint_accepts_schedule() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "schedule_enabled: true\n"
                "schedule: \"0 * * * *\"\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
            ),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["valid"] is True


def test_validate_module_endpoint_rejects_non_boolean_schedule_enabled() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "schedule_enabled: \"yes\"\n"
                "schedule: \"0 * * * *\"\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
            ),
        },
    )

    assert response.status_code == 400
    assert response.get_json()["valid"] is False
    assert "schedule_enabled" in response.get_json()["message"]


def test_validate_module_endpoint_rejects_invalid_schedule() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": (
                "name: Novo\n"
                "schedule: \"0 * * * * *\"\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
            ),
        },
    )

    assert response.status_code == 400
    assert response.get_json()["valid"] is False
    assert "Invalid schedule" in response.get_json()["message"]


def test_validate_module_endpoint_returns_original_yaml() -> None:
    client = app.test_client()
    content = (
        "name: Novo\n"
        "\n"
        "plugins:\n"
        "  - id: etapa\n"
        "    type: command_line\n"
        "    command: |\n"
        "      printf 'um'\n"
        "      printf 'dois'\n"
    )
    response = client.post(
        "/api/modules/validate",
        json={
            "module_id": "novo",
            "content": content,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["valid"] is True
    assert payload["yaml_content"] == content


def test_module_edit_keeps_editor_content_after_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    response = client.get("/modules/publico/edit")

    assert response.status_code == 200
    page = response.data.decode()
    validate_handler = page.split('validateButton.addEventListener("click", async () => {', 1)[
        1
    ].split('saveButton.addEventListener("click"', 1)[0]
    assert "yamlContent.value = payload.yaml_content;" not in validate_handler
    assert "Valid configuration." in validate_handler


def test_create_module_endpoint_persists_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )

    client = app.test_client()
    content = (
        "name: Novo\n"
        "description: Modulo novo\n"
        "\n"
        "plugins:\n"
        "  - id: etapa\n"
        "    type: command_line\n"
        "    description: Etapa inicial\n"
        "    command: |\n"
        "      printf 'um'\n"
        "      printf 'dois'\n"
    )
    response = client.post(
        "/api/modules",
        json={
            "module_id": "novo",
            "content": content,
        },
    )

    assert response.status_code == 201
    assert response.get_json()["saved"] is True
    assert (user_modules_dir / "novo.yaml").exists()
    assert (user_modules_dir / "novo.yaml").read_text(encoding="utf-8") == content
    assert response.get_json()["yaml_content"] == content


def test_create_module_endpoint_rejects_system_module_id() -> None:
    client = app.test_client()
    response = client.post(
        "/api/modules",
        json={
            "module_id": "teste_automatizado",
            "content": (
                "name: Teste\n"
                "plugins:\n"
                "  - id: etapa\n"
                "    type: command_line\n"
                "    command: echo ok\n"
            ),
        },
    )

    assert response.status_code == 400
    assert response.get_json()["saved"] is False


def test_update_module_endpoint_blocks_running_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    create_active_run("publico", "test-run")

    try:
        client = app.test_client()
        response = client.put(
            "/api/modules/publico",
            json={
                "content": (
                    "name: Publico\n"
                    "plugins:\n"
                    "  - id: etapa\n"
                    "    type: command_line\n"
                    "    command: echo ok\n"
                ),
            },
        )
    finally:
        remove_active_run("publico")

    assert response.status_code == 409
    assert response.get_json()["saved"] is False


def test_update_module_endpoint_persists_original_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    content = (
        "name: Publico\n"
        "description: Modulo publico atualizado.\n"
        "\n"
        "variables:\n"
        "  data_inicio_carga:\n"
        "    type: string\n"
        "    value: \"2026-01-01 00:00:00\"\n"
        "\n"
        "plugins:\n"
        "  - id: etapa\n"
        "    type: command_line\n"
        "    command:\n"
        "      - /bin/echo\n"
        "      - |\n"
        "        linha um\n"
        "        linha dois\n"
    )

    client = app.test_client()
    response = client.put(
        "/api/modules/publico",
        json={"content": content},
    )

    assert response.status_code == 200
    assert response.get_json()["saved"] is True
    assert (user_modules_dir / "publico.yaml").read_text(encoding="utf-8") == content
    assert response.get_json()["yaml_content"] == content


def test_create_active_run_signals_status_update() -> None:
    subscriber = app_module.EVENT_HUB.subscribe()

    try:
        create_active_run("teste_automatizado", "test-run")
        event = wait_for_event_reason(subscriber, "batch_started")
    finally:
        remove_active_run("teste_automatizado")
        app_module.EVENT_HUB.unsubscribe(subscriber)

    assert event.scope == "module"
    assert event.module_id == "teste_automatizado"
    assert event.resources == ["status"]
    assert event.reason == "batch_started"


def test_delete_module_endpoint_removes_module_and_temporary_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, temp_dir, locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )

    module_path = user_modules_dir / "remover.yaml"
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


def test_delete_module_endpoint_blocks_running_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    create_active_run("publico", "test-run")

    try:
        client = app.test_client()
        response = client.delete("/api/modules/publico")
    finally:
        remove_active_run("publico")

    assert response.status_code == 409
    assert response.get_json()["deleted"] is False


def test_stream_batch_reports_locked_module() -> None:
    module = load_system_module_config("teste_automatizado")
    events = stream_batch(module)
    next(events)

    try:
        nested_events = list(stream_batch(module))
    finally:
        events.close()

    assert any('"status": "locked"' in event for event in nested_events)


def test_module_locks_are_independent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    module_yaml = (
        "name: Teste\n"
        "plugins:\n"
        "  - id: etapa\n"
        "    type: command_line\n"
        "    command: echo concluido\n"
        "    success_contains: concluido\n"
    )
    (user_modules_dir / "lock_primario.yaml").write_text(module_yaml, encoding="utf-8")
    (user_modules_dir / "lock_secundario.yaml").write_text(module_yaml, encoding="utf-8")

    primary_events = stream_batch(load_module_config("lock_primario"))
    next(primary_events)

    try:
        secondary_events = list(stream_batch(load_module_config("lock_secundario")))
    finally:
        primary_events.close()

    assert any('"status": "success"' in event for event in secondary_events)
    assert not any('"status": "locked"' in event for event in secondary_events)


def test_stream_batch_persists_last_execution_log() -> None:
    list(stream_batch(load_system_module_config("teste_automatizado")))

    records = read_module_log("teste_automatizado")

    assert records[0]["event"] == "status"
    assert records[0]["run_id"]
    assert records[-1]["event"] == "done"
    assert records[-1]["status"] == "success"


def test_stream_batch_persists_schedule_trigger_metadata() -> None:
    scheduled_for = "2026-01-01T12:00:00+00:00"
    list(
        stream_batch(
            load_system_module_config("teste_automatizado"),
            trigger=RUN_TRIGGER_SCHEDULE,
            scheduled_for=scheduled_for,
        )
    )

    records = read_module_log("teste_automatizado")
    active_events = [record for record in records if record["event"] != "done"]

    assert active_events
    assert all(record["trigger"] == "schedule" for record in records)
    assert all(record["scheduled_for"] == scheduled_for for record in records)
    assert records[0]["message"].startswith("Starting scheduled batch")


def test_stream_batch_emits_plugin_statuses_on_success() -> None:
    events = [parse_sse_data(event) for event in stream_batch(load_system_module_config("teste_automatizado"))]

    status_event = events[0]
    first_plugin_start = next(
        event for event in events if event["event"] == "plugin_start"
    )
    first_plugin_done = next(
        event
        for event in events
        if event["event"] == "plugin_done" and event["plugin"] == "preparar_teste"
    )
    done_event = events[-1]

    assert status_event["plugin_statuses"] == {
        "preparar_teste": "enqueued",
        "processar_teste": "enqueued",
    }
    assert first_plugin_start["plugin_statuses"]["preparar_teste"] == "running"
    assert first_plugin_done["plugin_statuses"]["preparar_teste"] == "success"
    assert done_event["plugin_statuses"] == {
        "preparar_teste": "success",
        "processar_teste": "success",
    }


def test_detached_batch_continues_after_sse_client_closes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir, "publico")
    started = threading.Event()
    release = threading.Event()

    def fake_create_plugin(
        plugin_config: dict[str, object],
        module_variables: dict[str, dict[str, object]] | None = None,
    ) -> BasePlugin:
        return BlockingPlugin(str(plugin_config["id"]), started, release)

    monkeypatch.setattr(app_module, "create_plugin", fake_create_plugin)
    module = {
        "id": "publico",
        "name": "Publico",
        "variables": {},
        "plugins": [{"id": "etapa_longa", "type": "fake"}],
    }
    events = stream_detached_batch(module)

    try:
        first_event = parse_sse_data(next(events))
        assert first_event["event"] == "status"
        assert started.wait(timeout=1)
    finally:
        events.close()

    release.set()
    deadline = time.monotonic() + 2
    records = read_module_log("publico")
    while time.monotonic() < deadline and (
        not records or records[-1].get("event") != "done"
    ):
        time.sleep(0.01)
        records = read_module_log("publico")

    assert records[-1]["event"] == "done"
    assert records[-1]["status"] == "success"
    assert any(
        record["event"] == "plugin_done" and record["plugin"] == "etapa_longa"
        for record in records
    )
    assert not app_module.is_module_running("publico")


def test_detached_batch_stream_sends_heartbeat_while_plugin_is_silent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir, "publico")
    started = threading.Event()
    release = threading.Event()

    def fake_create_plugin(
        plugin_config: dict[str, object],
        module_variables: dict[str, dict[str, object]] | None = None,
    ) -> BasePlugin:
        return SilentBlockingPlugin(str(plugin_config["id"]), started, release)

    monkeypatch.setattr(app_module, "create_plugin", fake_create_plugin)
    module = {
        "id": "publico",
        "name": "Publico",
        "variables": {},
        "plugins": [{"id": "etapa_silenciosa", "type": "fake"}],
    }
    events = stream_detached_batch(module, heartbeat_seconds=0.01)

    try:
        assert parse_sse_data(next(events))["event"] == "status"
        assert parse_sse_data(next(events))["event"] == "plugin_start"
        assert started.wait(timeout=1)
        assert next(events) == ": heartbeat\n\n"
    finally:
        events.close()
        release.set()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and app_module.is_module_running("publico"):
        time.sleep(0.01)

    assert not app_module.is_module_running("publico")


def test_scheduler_triggers_due_scheduled_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir, "agendado")
    module_path = user_modules_dir / "agendado.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: true\n"
                "schedule: \"* * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []

    def fake_start_detached_batch(
        module: dict[str, object],
        *,
        trigger: str,
        scheduled_for: str | None = None,
        event_queue: queue.Queue[str | object] | None = None,
        client_closed: threading.Event | None = None,
    ) -> threading.Thread:
        calls.append(
            {
                "module": module["id"],
                "trigger": trigger,
                "scheduled_for": scheduled_for,
            }
        )
        thread = threading.Thread(target=lambda: None)
        return thread

    monkeypatch.setattr(app_module, "start_detached_batch", fake_start_detached_batch)
    scheduler = ModuleScheduler()
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    scheduler.refresh(now)
    triggered = scheduler.run_due_once(now + timedelta(minutes=1))

    assert triggered == ["agendado"]
    assert calls == [
        {
            "module": "agendado",
            "trigger": "schedule",
            "scheduled_for": "2026-01-01T12:01:00+00:00",
        }
    ]


def test_scheduler_skips_disabled_scheduled_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir, "agendado")
    module_path = user_modules_dir / "agendado.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: false\n"
                "schedule: \"* * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        app_module,
        "start_detached_batch",
        lambda *args, **kwargs: calls.append({"args": args, "kwargs": kwargs}),
    )
    scheduler = ModuleScheduler()
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    scheduler.refresh(now)
    triggered = scheduler.run_due_once(now + timedelta(minutes=1))

    assert triggered == []
    assert calls == []
    assert scheduler.next_run_for("agendado") == ""


def test_scheduler_skips_due_module_when_it_is_already_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(
        monkeypatch,
        tmp_path,
    )
    write_public_test_module(user_modules_dir, "agendado")
    module_path = user_modules_dir / "agendado.yaml"
    module_path.write_text(
        module_path.read_text(encoding="utf-8").replace(
            "description: Modulo publico temporario.\n",
            (
                "description: Modulo publico temporario.\n"
                "schedule_enabled: true\n"
                "schedule: \"* * * * *\"\n"
            ),
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        app_module,
        "start_detached_batch",
        lambda *args, **kwargs: calls.append({"args": args, "kwargs": kwargs}),
    )
    scheduler = ModuleScheduler()
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    scheduler.refresh(now)
    create_active_run("agendado", "test-run")

    try:
        triggered = scheduler.run_due_once(now + timedelta(minutes=1))
    finally:
        remove_active_run("agendado")

    assert triggered == []
    assert calls == []


def test_stream_batch_keeps_future_plugins_enqueued_after_failure() -> None:
    module = {
        "id": "teste_automatizado",
        "name": "Teste automatizado",
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
        "id": "teste_automatizado",
        "name": "Teste automatizado",
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
    events = stream_batch(load_system_module_config("teste_automatizado"))
    first_event = parse_sse_data(next(events))

    try:
        active_snapshot = json.loads(active_run_path("teste_automatizado").read_text(encoding="utf-8"))
        second_event = parse_sse_data(next(events))
        running_snapshot = json.loads(active_run_path("teste_automatizado").read_text(encoding="utf-8"))
    finally:
        events.close()

    assert first_event["plugin_statuses"] == {
        "preparar_teste": "enqueued",
        "processar_teste": "enqueued",
    }
    assert active_snapshot["plugin_statuses"] == first_event["plugin_statuses"]
    assert second_event["event"] == "plugin_start"
    assert running_snapshot["plugin_statuses"]["preparar_teste"] == "running"


def test_stream_batch_masks_sensitive_values_in_persisted_log() -> None:
    module = {
        "id": "teste_automatizado",
        "name": "Teste automatizado",
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
    records = read_module_log("teste_automatizado")
    serialized_records = json.dumps(records, ensure_ascii=False)

    assert "secret-value" not in serialized_records
    assert "****" in serialized_records
    assert records[-1]["status"] == "success"


def test_stream_batch_marks_timed_out_plugin_as_failed() -> None:
    module = {
        "id": "teste_automatizado",
        "name": "Teste automatizado",
        "variables": {},
        "plugins": [
            {
                "id": "lento",
                "type": "command_line",
                "command": [
                    sys.executable,
                    "-c",
                    "import time; time.sleep(5)",
                ],
                "timeout": 1,
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
    assert done_event["plugin"] == "lento"
    assert "timeout" in done_event["message"]
    assert done_event["plugin_statuses"] == {
        "lento": "failed",
        "nao_executar": "enqueued",
    }


def test_module_logs_endpoint_returns_persisted_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    list(stream_batch(load_module_config("publico")))

    client = app.test_client()
    response = client.get("/api/modules/publico/logs")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["id"] == "publico"
    assert payload["run_id"]
    assert payload["latest_sequence"] >= 1
    assert payload["events"][0]["event"] == "status"


def test_module_logs_endpoint_resets_when_new_run_replaces_previous_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    list(stream_batch(load_module_config("publico")))
    previous_records = read_module_log("publico")
    previous_run_id = previous_records[-1]["run_id"]
    previous_sequence = previous_records[-1]["sequence"]

    list(stream_batch(load_module_config("publico")))

    client = app.test_client()
    response = client.get(
        f"/api/modules/publico/logs?run_id={previous_run_id}&since={previous_sequence}"
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["reset"] is True
    assert payload["run_id"] != previous_run_id
    assert payload["events"][0]["sequence"] == 1


def test_clear_module_logs_when_module_is_not_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    list(stream_batch(load_module_config("publico")))

    client = app.test_client()
    response = client.post("/api/modules/publico/logs/clear")

    assert response.status_code == 200
    assert response.get_json()["cleared"] is True
    assert read_module_log("publico") == []


def test_clear_module_logs_is_blocked_while_module_is_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    events = stream_batch(load_module_config("publico"))
    next(events)

    try:
        client = app.test_client()
        response = client.post("/api/modules/publico/logs/clear")
    finally:
        events.close()

    assert response.status_code == 409
    assert response.get_json()["cleared"] is False


def test_kill_module_returns_conflict_when_module_is_not_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)

    client = app.test_client()
    response = client.post("/api/modules/publico/kill")

    assert response.status_code == 409
    assert response.get_json()["killed"] is False


def test_kill_module_calls_active_plugin_kill(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    user_modules_dir, _system_modules_dir, _temp_dir, _locks_dir = configure_temp_runtime_dirs(monkeypatch, tmp_path)
    write_public_test_module(user_modules_dir)
    plugin = FakeKillablePlugin()
    create_active_run("publico", "test-run")
    set_active_plugin("publico", plugin, {"type": "fake"})

    try:
        client = app.test_client()
        response = client.post("/api/modules/publico/kill")
        kill_requested = get_active_kill_requested("publico")
    finally:
        remove_active_run("publico")

    assert response.status_code == 200
    assert response.get_json()["killed"] is True
    assert plugin.killed is True
    assert kill_requested is True
