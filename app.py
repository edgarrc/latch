from __future__ import annotations

import importlib
import json
import os
import queue
import re
import secrets
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import yaml
from filelock import FileLock, Timeout
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from yaml.nodes import MappingNode, Node, ScalarNode

try:
    from croniter import croniter
except ImportError:  # pragma: no cover - production installs requirements.txt.
    croniter = None

from plugins.base import (
    BasePlugin,
    PluginEvent,
    PluginExecutionError,
    PluginKillError,
    PluginKilledError,
)
from plugins.variables import prepare_plugin_config, validate_variable_definitions


BASE_DIR = Path(__file__).resolve().parent
MODULES_ROOT = BASE_DIR / "modules"
USER_MODULES_DIR = MODULES_ROOT / "user"
SYSTEM_MODULES_DIR = MODULES_ROOT / "system"
LOCKS_DIR = BASE_DIR / "locks"
TEMP_DIR = BASE_DIR / "temp"
SETTINGS_PATH = BASE_DIR / "settings.yaml"
APP_NAME = "Latch"
APP_TAGLINE = "Gerenciador batch"
APP_GITHUB_URL = "https://github.com/edgarrc/latch"
ADMIN_USERNAME = "admin"
USER_USERNAME = "user"
KNOWN_USERNAMES = (ADMIN_USERNAME, USER_USERNAME)
SESSION_USER_KEY = "user"
MODULE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
PLUGIN_TYPES = {
    "clickhouse_client": "plugins.clickhouse_client.ClickHouseClientPlugin",
    "command_line": "plugins.command_line.CommandLinePlugin",
    "redis_client": "plugins.redis_client.RedisClientPlugin",
}
ACTIVE_MODULES: set[str] = set()
ACTIVE_RUNS: dict[str, "ActiveRun"] = {}
ACTIVE_MODULES_LOCK = threading.Lock()
GENERATED_TEMP_PATTERNS = ("temp_*.jsonl", "active_*.json")
RUN_TRIGGER_MANUAL = "manual"
RUN_TRIGGER_SCHEDULE = "schedule"
RUN_TRIGGERS = {RUN_TRIGGER_MANUAL, RUN_TRIGGER_SCHEDULE}
SCHEDULER_POLL_SECONDS = 15.0
SSE_HEARTBEAT_SECONDS = 15.0

USER_MODULES_DIR.mkdir(parents=True, exist_ok=True)
SYSTEM_MODULES_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)


def clear_generated_temp_files() -> None:
    for pattern in GENERATED_TEMP_PATTERNS:
        for path in TEMP_DIR.glob(pattern):
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)


clear_generated_temp_files()
app = Flask(__name__)
app.permanent_session_lifetime = timedelta(hours=24)


def load_settings() -> dict[str, Any] | None:
    if not SETTINGS_PATH.exists():
        return None

    with SETTINGS_PATH.open("r", encoding="utf-8") as settings_file:
        settings = yaml.safe_load(settings_file) or {}
    if not isinstance(settings, dict):
        return None
    users = settings.get("users")
    if not isinstance(users, dict):
        return None
    for username in KNOWN_USERNAMES:
        user_settings = users.get(username)
        if not isinstance(user_settings, dict):
            return None
        password_hash = user_settings.get("password_hash")
        if not isinstance(password_hash, str) or not password_hash:
            return None
    return settings


def build_settings(admin_password: str, user_password: str) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "users": {
            ADMIN_USERNAME: {"password_hash": generate_password_hash(admin_password)},
            USER_USERNAME: {
                "password_hash": generate_password_hash(user_password)
            },
        },
        "secret_key": secrets.token_urlsafe(32),
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def write_settings(settings: dict[str, Any]) -> None:
    SETTINGS_PATH.write_text(
        yaml.safe_dump(settings, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def load_persisted_secret_key() -> str | None:
    settings = load_settings()
    if settings is None:
        return None

    secret_key = settings.get("secret_key")
    return secret_key if isinstance(secret_key, str) and secret_key else None


app.secret_key = load_persisted_secret_key() or secrets.token_urlsafe(32)


@app.context_processor
def inject_app_metadata() -> dict[str, Any]:
    return {
        "app_name": APP_NAME,
        "app_tagline": APP_TAGLINE,
        "app_github_url": APP_GITHUB_URL,
        "current_user": current_username(),
        "current_is_admin": is_admin(),
    }


@dataclass
class ActiveRun:
    module_id: str
    run_id: str
    trigger: str = RUN_TRIGGER_MANUAL
    scheduled_for: str | None = None
    sequence: int = 0
    current_plugin: BasePlugin | None = None
    current_plugin_id: str | None = None
    current_plugin_type: str | None = None
    kill_requested: bool = False
    plugin_statuses: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AppEvent:
    id: int
    type: str
    scope: str
    resources: list[str]
    reason: str
    created_at: str
    module_id: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "scope": self.scope,
            "module_id": self.module_id,
            "resources": self.resources,
            "reason": self.reason,
            "version": self.id,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class AppUpdateSignal:
    scope: str
    resources: tuple[str, ...]
    reason: str
    module_id: str | None = None


class AppEventHub:
    def __init__(self, history_size: int = 200) -> None:
        self._history: deque[AppEvent] = deque(maxlen=history_size)
        self._subscribers: set[queue.Queue[AppEvent]] = set()
        self._lock = threading.Lock()
        self._next_id = 1

    def publish(
        self,
        *,
        scope: str,
        resources: list[str],
        reason: str,
        module_id: str | None = None,
    ) -> AppEvent:
        with self._lock:
            event = AppEvent(
                id=self._next_id,
                type="app_update",
                scope=scope,
                module_id=module_id,
                resources=resources,
                reason=reason,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            self._next_id += 1
            self._history.append(event)
            subscribers = list(self._subscribers)

        for subscriber in subscribers:
            self._put_subscriber_event(subscriber, event)
        return event

    def subscribe(self, last_event_id: str | None = None) -> queue.Queue[AppEvent]:
        subscriber: queue.Queue[AppEvent] = queue.Queue(maxsize=512)
        replay_after = self._parse_last_event_id(last_event_id)
        with self._lock:
            replay_events = [
                event for event in self._history if replay_after is not None and event.id > replay_after
            ]
            for event in replay_events:
                self._put_subscriber_event(subscriber, event)
            self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue[AppEvent]) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    @staticmethod
    def _parse_last_event_id(last_event_id: str | None) -> int | None:
        if not last_event_id:
            return None
        try:
            return int(last_event_id)
        except ValueError:
            return None

    @staticmethod
    def _put_subscriber_event(
        subscriber: queue.Queue[AppEvent],
        event: AppEvent,
    ) -> None:
        try:
            subscriber.put_nowait(event)
        except queue.Full:
            try:
                subscriber.get_nowait()
            except queue.Empty:
                pass
            subscriber.put_nowait(event)


class ApplicationMonitor:
    def __init__(self, event_hub: AppEventHub, debounce_seconds: float = 0.05) -> None:
        self._event_hub = event_hub
        self._debounce_seconds = debounce_seconds
        self._signals: queue.Queue[AppUpdateSignal] = queue.Queue()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="application-monitor",
                daemon=True,
            )
            self._thread.start()

    def signal(
        self,
        *,
        scope: str,
        resources: list[str],
        reason: str,
        module_id: str | None = None,
    ) -> None:
        self.start()
        normalized_resources = tuple(sorted(set(resources)))
        if not normalized_resources:
            return
        self._signals.put(
            AppUpdateSignal(
                scope=scope,
                module_id=module_id,
                resources=normalized_resources,
                reason=reason,
            )
        )

    def _run(self) -> None:
        while True:
            first_signal = self._signals.get()
            pending: dict[tuple[str, str | None], dict[str, Any]] = {}
            self._merge_signal(pending, first_signal)
            deadline = time.monotonic() + self._debounce_seconds

            while True:
                timeout = max(0.0, deadline - time.monotonic())
                if timeout == 0:
                    break
                try:
                    signal = self._signals.get(timeout=timeout)
                except queue.Empty:
                    break
                self._merge_signal(pending, signal)

            for (scope, module_id), update in pending.items():
                self._event_hub.publish(
                    scope=scope,
                    module_id=module_id,
                    resources=sorted(update["resources"]),
                    reason=update["reason"],
                )

    @staticmethod
    def _merge_signal(
        pending: dict[tuple[str, str | None], dict[str, Any]],
        signal: AppUpdateSignal,
    ) -> None:
        key = (signal.scope, signal.module_id)
        update = pending.setdefault(
            key,
            {"resources": set(), "reason": signal.reason},
        )
        update["resources"].update(signal.resources)
        update["reason"] = signal.reason


EVENT_HUB = AppEventHub()
APP_MONITOR = ApplicationMonitor(EVENT_HUB)
APP_MONITOR.start()


def signal_app_update(
    *,
    scope: str,
    resources: list[str],
    reason: str,
    module_id: str | None = None,
) -> None:
    APP_MONITOR.signal(
        scope=scope,
        module_id=module_id,
        resources=resources,
        reason=reason,
    )


def local_now() -> datetime:
    return datetime.now().astimezone()


def format_datetime_for_display(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%d/%m/%Y %H:%M")


def validate_schedule_expression(value: Any, context: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{context} 'schedule' deve ser texto.")

    schedule = value.strip()
    if not schedule:
        return ""

    next_schedule_time(schedule, local_now())
    return schedule


def validate_schedule_enabled(value: Any, schedule: str, context: str) -> bool:
    if value is None:
        return bool(schedule)
    if not isinstance(value, bool):
        raise ValueError(f"{context} 'schedule_enabled' deve ser booleano.")
    return bool(schedule and value)


def next_schedule_time(expression: str, base_time: datetime) -> datetime:
    if len(expression.split()) != 5:
        raise ValueError(f"schedule inválido: {expression!r}.")

    if croniter is not None:
        try:
            next_time = croniter(expression, base_time).get_next(datetime)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"schedule inválido: {expression!r}.") from exc
        if next_time.tzinfo is None and base_time.tzinfo is not None:
            next_time = next_time.replace(tzinfo=base_time.tzinfo)
        return next_time

    return next_schedule_time_fallback(expression, base_time)


def next_schedule_time_fallback(expression: str, base_time: datetime) -> datetime:
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(f"schedule inválido: {expression!r}.")

    try:
        minutes, _minute_wildcard = parse_cron_field(fields[0], 0, 59)
        hours, _hour_wildcard = parse_cron_field(fields[1], 0, 23)
        days, day_wildcard = parse_cron_field(fields[2], 1, 31)
        months, _month_wildcard = parse_cron_field(fields[3], 1, 12)
        weekdays, weekday_wildcard = parse_cron_field(fields[4], 0, 7)
    except ValueError as exc:
        raise ValueError(f"schedule inválido: {expression!r}.") from exc
    if 7 in weekdays:
        weekdays.add(0)
        weekdays.discard(7)

    candidate = base_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    deadline = candidate + timedelta(days=366)
    while candidate <= deadline:
        cron_weekday = (candidate.weekday() + 1) % 7
        day_matches = candidate.day in days
        weekday_matches = cron_weekday in weekdays
        if not day_wildcard and not weekday_wildcard:
            calendar_matches = day_matches or weekday_matches
        else:
            calendar_matches = day_matches and weekday_matches

        if (
            candidate.minute in minutes
            and candidate.hour in hours
            and candidate.month in months
            and calendar_matches
        ):
            return candidate
        candidate += timedelta(minutes=1)

    raise ValueError(f"schedule inválido: {expression!r}.")


def parse_cron_field(field: str, minimum: int, maximum: int) -> tuple[set[int], bool]:
    if not field:
        raise ValueError("Campo cron vazio.")

    values: set[int] = set()
    wildcard = False
    for raw_part in field.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError("Campo cron vazio.")

        range_part = part
        step = 1
        if "/" in part:
            range_part, step_part = part.split("/", 1)
            if not step_part.isdigit():
                raise ValueError("Passo cron inválido.")
            step = int(step_part)
            if step <= 0:
                raise ValueError("Passo cron inválido.")

        if range_part == "*":
            start = minimum
            end = maximum
            wildcard = wildcard or step == 1
        elif "-" in range_part:
            start_part, end_part = range_part.split("-", 1)
            if not start_part.isdigit() or not end_part.isdigit():
                raise ValueError("Intervalo cron inválido.")
            start = int(start_part)
            end = int(end_part)
        elif range_part.isdigit():
            start = end = int(range_part)
        else:
            raise ValueError("Campo cron inválido.")

        if start < minimum or end > maximum or start > end:
            raise ValueError("Valor cron fora do intervalo permitido.")
        values.update(range(start, end + 1, step))

    return values, wildcard


def password_hash_for_user(settings: dict[str, Any], username: str) -> str | None:
    if username not in KNOWN_USERNAMES:
        return None

    users = settings.get("users")
    if isinstance(users, dict):
        user_settings = users.get(username)
        if isinstance(user_settings, dict):
            password_hash = user_settings.get("password_hash")
            if isinstance(password_hash, str) and password_hash:
                return password_hash

    return None


def current_username() -> str | None:
    if app.config.get("AUTH_DISABLED"):
        return ADMIN_USERNAME

    username = session.get(SESSION_USER_KEY)
    if not isinstance(username, str):
        return None

    settings = load_settings()
    if settings is None or password_hash_for_user(settings, username) is None:
        return None

    return username


def is_authenticated() -> bool:
    return current_username() is not None


def is_admin() -> bool:
    return current_username() == ADMIN_USERNAME


def can_edit_modules() -> bool:
    return bool(app.config.get("AUTH_DISABLED")) or is_admin()


def authenticate_session(username: str) -> None:
    session.clear()
    session.permanent = True
    session[SESSION_USER_KEY] = username


def require_admin_access() -> Response | tuple[Response, int] | None:
    if can_edit_modules():
        return None

    message = "Apenas admin pode editar módulos."
    if is_api_request():
        return jsonify({"authorized": False, "message": message}), 403

    abort(403, description=message)
    return None


def is_api_request() -> bool:
    return request.path.startswith("/api/")


def safe_next_url(next_url: str | None) -> str:
    if next_url and next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("index")


@app.before_request
def require_authentication() -> Response | tuple[Response, int] | None:
    if app.config.get("AUTH_DISABLED"):
        ensure_scheduler_started()
        return None

    if request.endpoint == "static":
        return None

    ensure_scheduler_started()

    settings = load_settings()
    setup_endpoint = request.endpoint == "setup"
    login_endpoint = request.endpoint == "login"
    logout_endpoint = request.endpoint == "logout"

    if settings is None:
        if setup_endpoint:
            return None
        return redirect(url_for("setup"))

    if is_authenticated():
        if setup_endpoint or login_endpoint:
            return redirect(url_for("index"))
        return None

    if login_endpoint or logout_endpoint:
        return None

    if is_api_request():
        return (
            jsonify({"authenticated": False, "message": "Autenticação requerida."}),
            401,
        )

    return redirect(url_for("login", next=request.full_path.rstrip("?")))


@app.route("/setup", methods=["GET", "POST"])
def setup() -> str | Response:
    if load_settings() is not None:
        return redirect(url_for("index") if is_authenticated() else url_for("login"))

    error = ""
    if request.method == "POST":
        admin_password = request.form.get("admin_password", "")
        admin_password_confirm = request.form.get("admin_password_confirm", "")
        user_password = request.form.get("user_password", "")
        user_password_confirm = request.form.get("user_password_confirm", "")

        if not admin_password:
            error = "Informe a senha do admin."
        elif admin_password != admin_password_confirm:
            error = "A confirmação da senha do admin não confere."
        elif not user_password:
            error = "Informe a senha do user."
        elif user_password != user_password_confirm:
            error = "A confirmação da senha do user não confere."
        else:
            settings = build_settings(admin_password, user_password)
            write_settings(settings)
            app.secret_key = settings["secret_key"]
            authenticate_session(ADMIN_USERNAME)
            return redirect(url_for("index"))

    return render_template("setup.html", error=error)


@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    settings = load_settings()
    if settings is None:
        return redirect(url_for("setup"))

    next_url = safe_next_url(request.values.get("next"))
    error = ""
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        password_hash = password_hash_for_user(settings, username)

        if isinstance(password_hash, str) and check_password_hash(password_hash, password):
            authenticate_session(username)
            return redirect(next_url)

        error = "Usuário ou senha inválidos."

    return render_template(
        "login.html",
        error=error,
        next_url=next_url,
        username=request.form.get("username", ""),
    )


@app.post("/logout")
def logout() -> Response:
    session.clear()
    return redirect(url_for("login") if load_settings() is not None else url_for("setup"))


@app.get("/")
def index() -> str:
    return render_template(
        "index.html",
        modules=[module_with_status(module_name) for module_name in discover_module_names()],
    )


@app.get("/modules/new")
def new_module_page() -> str | Response | tuple[Response, int]:
    admin_response = require_admin_access()
    if admin_response is not None:
        return admin_response

    return render_template(
        "module_edit.html",
        mode="create",
        module_id="",
        module_name="",
        module_running=False,
        readonly=False,
        yaml_content=default_module_yaml(),
    )


@app.get("/modules/<module_name>/edit")
def edit_module_page(module_name: str) -> str | Response | tuple[Response, int]:
    ensure_public_module_exists(module_name)
    module = module_with_status(module_name)
    yaml_content = read_module_yaml(module_name)
    readonly = not can_edit_modules()
    return render_template(
        "module_edit.html",
        mode="edit",
        module_id=module_name,
        module_name=module["name"],
        module_running=module["running"],
        readonly=readonly,
        yaml_content=mask_sensitive_module_yaml(yaml_content) if readonly else yaml_content,
    )


@app.get("/<module_name>")
def module_page(module_name: str) -> str:
    ensure_public_module_exists(module_name)
    module = module_with_status(module_name)
    return render_template("module.html", module=module)


@app.get("/api/modules/status")
def modules_status() -> Response:
    return jsonify(
        {
            "modules": {
                module_name: module_runtime_status(load_module_config(module_name))
                for module_name in discover_module_names()
            }
        }
    )


@app.get("/api/modules/<module_name>/status")
def module_status(module_name: str) -> Response:
    ensure_public_module_exists(module_name)
    module = load_module_config(module_name)
    return jsonify({"id": module_name, **module_runtime_status(module)})


@app.get("/api/events")
def api_events() -> Response:
    last_event_id = request.headers.get("Last-Event-ID")
    subscriber = EVENT_HUB.subscribe(last_event_id)

    def stream_events() -> Iterator[str]:
        try:
            while True:
                try:
                    event = subscriber.get(timeout=15)
                except queue.Empty:
                    yield ": heartbeat\n\n"
                    continue
                yield sse("app_update", event.to_payload(), event_id=event.id)
        finally:
            EVENT_HUB.unsubscribe(subscriber)

    return Response(
        stream_with_context(stream_events()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/modules/validate")
def validate_module() -> Response:
    admin_response = require_admin_access()
    if admin_response is not None:
        return admin_response

    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id") or "new_module"
    content = payload.get("content")
    if not isinstance(content, str):
        return jsonify({"valid": False, "message": "O YAML do módulo é obrigatório."}), 400

    try:
        validate_module_id(str(module_id))
        ensure_not_system_module(str(module_id))
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
            "yaml_content": content,
        }
    )


@app.post("/api/modules")
def create_module_config() -> Response:
    admin_response = require_admin_access()
    if admin_response is not None:
        return admin_response

    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id")
    content = payload.get("content")
    if not isinstance(module_id, str) or not module_id:
        return jsonify({"saved": False, "message": "Informe o ID do módulo."}), 400
    if not isinstance(content, str):
        return jsonify({"saved": False, "message": "O YAML do módulo é obrigatório."}), 400

    try:
        validate_module_id(module_id)
        ensure_not_system_module(module_id)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    config_path = module_config_path(module_id)
    if config_path.exists():
        return jsonify({"saved": False, "message": "Já existe um módulo com esse ID."}), 409

    try:
        config = parse_module_yaml(content)
        validate_module_config(module_id, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    write_module_yaml_content(module_id, content)
    signal_app_update(
        scope="modules",
        module_id=module_id,
        resources=["config", "status"],
        reason="module_created",
    )
    SCHEDULER.wake()
    return jsonify(
        {
            "saved": True,
            "id": module_id,
            "message": "Módulo criado.",
            "redirect": f"/modules/{module_id}/edit",
            "yaml_content": content,
        }
    ), 201


@app.put("/api/modules/<module_name>")
def update_module_config(module_name: str) -> Response:
    admin_response = require_admin_access()
    if admin_response is not None:
        return admin_response

    ensure_public_module_exists(module_name)
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
        validate_module_config(module_name, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    write_module_yaml_content(module_name, content)
    signal_app_update(
        scope="modules",
        module_id=module_name,
        resources=["config", "status"],
        reason="module_updated",
    )
    SCHEDULER.wake()
    return jsonify(
        {
            "saved": True,
            "id": module_name,
            "message": "Módulo salvo.",
            "yaml_content": content,
        }
    )


@app.delete("/api/modules/<module_name>")
def delete_module_config(module_name: str) -> Response:
    admin_response = require_admin_access()
    if admin_response is not None:
        return admin_response

    ensure_public_module_exists(module_name)
    if is_module_running(module_name):
        return jsonify(
            {
                "deleted": False,
                "running": True,
                "message": "Não é possível excluir enquanto o módulo está em execução.",
            }
        ), 409

    delete_module_files(module_name)
    signal_app_update(
        scope="modules",
        module_id=module_name,
        resources=["config", "status"],
        reason="module_deleted",
    )
    SCHEDULER.wake()
    return jsonify({"deleted": True, "id": module_name, "message": "Módulo excluído."})


@app.get("/api/modules/<module_name>/logs")
def module_logs(module_name: str) -> Response:
    ensure_public_module_exists(module_name)
    module = load_module_config(module_name)

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
            **module_runtime_status(module),
            "run_id": latest_run_id,
            "latest_sequence": latest_sequence,
            "reset": reset,
            "events": events,
        }
    )


@app.post("/api/modules/<module_name>/logs/clear")
def clear_module_logs(module_name: str) -> Response:
    ensure_public_module_exists(module_name)

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
    signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["logs", "status"],
        reason="module_logs_cleared",
    )
    return jsonify({"id": module_name, "cleared": True, "running": False})


@app.post("/api/modules/<module_name>/kill")
def kill_module(module_name: str) -> Response:
    ensure_public_module_exists(module_name)

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
    ensure_public_module_exists(module_name)
    module = load_module_config(module_name)
    return Response(
        stream_with_context(stream_detached_batch(module, trigger=RUN_TRIGGER_MANUAL)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

def discover_module_names() -> list[str]:
    module_names = [
        path.stem
        for path in USER_MODULES_DIR.glob("*.yaml")
        if path.is_file() and MODULE_ID_RE.fullmatch(path.stem)
    ]
    return sorted(module_names)


def discover_system_module_names() -> list[str]:
    module_names = [
        path.stem
        for path in SYSTEM_MODULES_DIR.glob("*.yaml")
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
    return USER_MODULES_DIR / f"{module_name}.yaml"


def system_module_config_path(module_name: str) -> Path:
    validate_module_id(module_name)
    return SYSTEM_MODULES_DIR / f"{module_name}.yaml"


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
        raise ValueError("ID reservado para uso interno do sistema.")


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
    (TEMP_DIR / f"temp_{module_name}.jsonl").unlink(missing_ok=True)
    (TEMP_DIR / f"active_{module_name}.json").unlink(missing_ok=True)
    (LOCKS_DIR / f"{module_name}.lock").unlink(missing_ok=True)


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


def module_with_status(module_name: str) -> dict[str, Any]:
    module = load_module_config(module_name)
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
        "next_run_display": format_datetime_for_display(next_run),
        "trigger": active_run.trigger if active_run is not None else "",
        "scheduled_for": active_run.scheduled_for if active_run is not None else "",
        "scheduled_for_display": format_datetime_for_display(
            active_run.scheduled_for if active_run is not None else None
        ),
    }


def scheduled_next_run(module_name: str, schedule: str) -> str:
    now = local_now()
    scheduler = globals().get("SCHEDULER")
    if scheduler is not None:
        next_run = scheduler.next_run_for(module_name)
        if next_run and scheduled_time_is_future(next_run, now):
            return next_run
    try:
        return next_schedule_time(schedule, now).isoformat()
    except ValueError:
        return ""


def scheduled_time_is_future(value: str, now: datetime) -> bool:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    if parsed.tzinfo is None and now.tzinfo is not None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    return parsed > now


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
    *,
    trigger: str = RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
) -> ActiveRun:
    if trigger not in RUN_TRIGGERS:
        raise ValueError(f"Trigger de execução inválido: {trigger!r}.")
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
    signal_app_update(
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
    signal_app_update(
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
    signal_app_update(
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
    signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["logs"],
        reason=f"batch_{event}",
    )
    return record


def active_run_path(module_name: str) -> Path:
    ensure_module_exists(module_name)
    return TEMP_DIR / f"active_{module_name}.json"


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


def stream_batch(
    module: dict[str, Any],
    *,
    trigger: str = RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
) -> Iterator[str]:
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
                "trigger": trigger,
                "scheduled_for": scheduled_for,
                "message": f"O módulo {module['name']} já está em execução.",
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
        return sse(event, record)

    try:
        yield emit(
            "status",
            {
                "level": "info",
                "message": (
                    f"Iniciando batch agendado {module['name']}."
                    if trigger == RUN_TRIGGER_SCHEDULE
                    else f"Iniciando batch {module['name']}."
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


_BATCH_STREAM_DONE = object()


def start_detached_batch(
    module: dict[str, Any],
    *,
    trigger: str = RUN_TRIGGER_MANUAL,
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
    trigger: str = RUN_TRIGGER_MANUAL,
    scheduled_for: str | None = None,
    heartbeat_seconds: float = SSE_HEARTBEAT_SECONDS,
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


class ModuleScheduler:
    def __init__(self, poll_seconds: float = SCHEDULER_POLL_SECONDS) -> None:
        self._poll_seconds = poll_seconds
        self._lock = threading.Lock()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._next_runs: dict[str, datetime] = {}
        self._schedules: dict[str, str] = {}

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="module-scheduler",
                daemon=True,
            )
            self._thread.start()

    def wake(self) -> None:
        self._wake_event.set()

    def next_run_for(self, module_name: str) -> str:
        with self._lock:
            next_run = self._next_runs.get(module_name)
        return next_run.isoformat() if next_run is not None else ""

    def refresh(self, now: datetime | None = None) -> None:
        self._refresh_schedules(now or local_now())

    def run_due_once(self, now: datetime | None = None) -> list[str]:
        return self._run_due(now or local_now())

    def _run(self) -> None:
        while True:
            now = local_now()
            self._refresh_schedules(now)
            self._run_due(now)
            timeout = self._seconds_until_next_run(local_now())
            self._wake_event.wait(timeout=timeout)
            self._wake_event.clear()

    def _refresh_schedules(self, now: datetime) -> None:
        module_names = set(discover_module_names())
        with self._lock:
            for module_name in set(self._schedules) - module_names:
                self._schedules.pop(module_name, None)
                self._next_runs.pop(module_name, None)

        for module_name in sorted(module_names):
            try:
                module = load_module_config(module_name)
            except Exception:
                with self._lock:
                    self._schedules.pop(module_name, None)
                    self._next_runs.pop(module_name, None)
                continue

            schedule = module.get("schedule") or ""
            schedule_enabled = bool(module.get("schedule_enabled"))
            if not schedule or not schedule_enabled:
                with self._lock:
                    self._schedules.pop(module_name, None)
                    self._next_runs.pop(module_name, None)
                continue

            with self._lock:
                current_schedule = self._schedules.get(module_name)
                current_next_run = self._next_runs.get(module_name)
            if current_schedule == schedule and current_next_run is not None:
                continue

            try:
                next_run = next_schedule_time(schedule, now)
            except ValueError:
                continue
            with self._lock:
                self._schedules[module_name] = schedule
                self._next_runs[module_name] = next_run

    def _run_due(self, now: datetime) -> list[str]:
        due: list[tuple[str, datetime, str]] = []
        with self._lock:
            for module_name, next_run in self._next_runs.items():
                schedule = self._schedules.get(module_name)
                if schedule and next_run <= now:
                    due.append((module_name, next_run, schedule))

        triggered: list[str] = []
        for module_name, scheduled_for, schedule in due:
            try:
                next_run = next_schedule_time(schedule, max(now, scheduled_for))
            except ValueError:
                with self._lock:
                    self._schedules.pop(module_name, None)
                    self._next_runs.pop(module_name, None)
                continue

            with self._lock:
                if self._schedules.get(module_name) == schedule:
                    self._next_runs[module_name] = next_run

            try:
                module = load_module_config(module_name)
            except Exception:
                continue
            if not module.get("schedule") or not module.get("schedule_enabled"):
                continue
            if is_module_running(module_name):
                continue

            start_detached_batch(
                module,
                trigger=RUN_TRIGGER_SCHEDULE,
                scheduled_for=scheduled_for.isoformat(),
            )
            triggered.append(module_name)

        return triggered

    def _seconds_until_next_run(self, now: datetime) -> float:
        with self._lock:
            next_runs = list(self._next_runs.values())
        if not next_runs:
            return self._poll_seconds
        seconds = min((next_run - now).total_seconds() for next_run in next_runs)
        return max(0.1, min(self._poll_seconds, seconds))


SCHEDULER = ModuleScheduler()


def ensure_scheduler_started() -> None:
    if app.config.get("SCHEDULER_DISABLED"):
        return
    if os.environ.get("LATCH_SCHEDULER_DISABLED") == "1":
        return
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return
    if not app.config.get("AUTH_DISABLED") and load_settings() is None:
        return
    SCHEDULER.start()


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


def sse(event: str, payload: dict[str, Any], event_id: int | None = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


if __name__ == "__main__":
    app.run(debug=True, threaded=True)
