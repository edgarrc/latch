from __future__ import annotations

import queue
import secrets
from datetime import timedelta
from typing import Any, Iterator

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from werkzeug.security import check_password_hash

from . import auth, config, events, modules, runtime, scheduler, utils
from .plugins.base import PluginKillError

config.ensure_runtime_directories()
runtime.clear_generated_temp_files()
app = Flask(
    __name__,
    template_folder=str(config.BASE_DIR / "templates"),
    static_folder=str(config.BASE_DIR / "static"),
)
app.permanent_session_lifetime = timedelta(hours=24)
app.secret_key = auth.load_persisted_secret_key() or secrets.token_urlsafe(32)


@app.context_processor
def inject_app_metadata() -> dict[str, Any]:
    return {
        "app_name": config.APP_NAME,
        "app_tagline": config.APP_TAGLINE,
        "app_github_url": config.APP_GITHUB_URL,
        "current_user": auth.current_username(),
        "current_is_admin": auth.is_admin(),
    }


@app.before_request
def require_authentication() -> Response | tuple[Response, int] | None:
    if app.config.get("AUTH_DISABLED"):
        scheduler.ensure_scheduler_started()
        return None

    if request.endpoint == "static":
        return None

    scheduler.ensure_scheduler_started()

    settings = auth.load_settings()
    setup_endpoint = request.endpoint == "setup"
    login_endpoint = request.endpoint == "login"
    logout_endpoint = request.endpoint == "logout"

    if settings is None:
        if setup_endpoint:
            return None
        return redirect(url_for("setup"))

    if auth.is_authenticated():
        if setup_endpoint or login_endpoint:
            return redirect(url_for("index"))
        return None

    if login_endpoint or logout_endpoint:
        return None

    if auth.is_api_request():
        return (
            jsonify({"authenticated": False, "message": "Authentication required."}),
            401,
        )

    return redirect(url_for("login", next=request.full_path.rstrip("?")))


@app.route("/setup", methods=["GET", "POST"])
def setup() -> str | Response:
    if auth.load_settings() is not None:
        return redirect(url_for("index") if auth.is_authenticated() else url_for("login"))

    error = ""
    if request.method == "POST":
        admin_password = request.form.get("admin_password", "")
        admin_password_confirm = request.form.get("admin_password_confirm", "")
        user_password = request.form.get("user_password", "")
        user_password_confirm = request.form.get("user_password_confirm", "")

        if not admin_password:
            error = "Enter the admin password."
        elif admin_password != admin_password_confirm:
            error = "Admin password confirmation does not match."
        elif not user_password:
            error = "Enter the user password."
        elif user_password != user_password_confirm:
            error = "User password confirmation does not match."
        else:
            settings = auth.build_settings(admin_password, user_password)
            auth.write_settings(settings)
            app.secret_key = settings["secret_key"]
            auth.authenticate_session(config.ADMIN_USERNAME)
            return redirect(url_for("index"))

    return render_template("setup.html", error=error)


@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    settings = auth.load_settings()
    if settings is None:
        return redirect(url_for("setup"))

    next_url = auth.safe_next_url(request.values.get("next"))
    error = ""
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        password_hash = auth.password_hash_for_user(settings, username)

        if isinstance(password_hash, str) and check_password_hash(password_hash, password):
            auth.authenticate_session(username)
            return redirect(next_url)

        error = "Invalid username or password."

    return render_template(
        "login.html",
        error=error,
        next_url=next_url,
        username=request.form.get("username", ""),
    )


@app.post("/logout")
def logout() -> Response:
    session.clear()
    return redirect(url_for("login") if auth.load_settings() is not None else url_for("setup"))


@app.get("/")
def index() -> str:
    return render_template(
        "index.html",
        modules=[runtime.module_with_status(module_name) for module_name in modules.discover_module_names()],
    )


@app.get("/modules/new")
def new_module_page() -> str | Response | tuple[Response, int]:
    admin_response = auth.require_admin_access()
    if admin_response is not None:
        return admin_response

    return render_template(
        "module_edit.html",
        mode="create",
        module_id="",
        module_name="",
        module_running=False,
        readonly=False,
        yaml_content=modules.default_module_yaml(),
    )


@app.get("/modules/<module_name>/edit")
def edit_module_page(module_name: str) -> str | Response | tuple[Response, int]:
    modules.ensure_public_module_exists(module_name)
    module = runtime.module_with_status(module_name)
    yaml_content = modules.read_module_yaml(module_name)
    readonly = not auth.can_edit_modules()
    return render_template(
        "module_edit.html",
        mode="edit",
        module_id=module_name,
        module_name=module["name"],
        module_running=module["running"],
        readonly=readonly,
        yaml_content=modules.mask_sensitive_module_yaml(yaml_content) if readonly else yaml_content,
    )


@app.get("/<module_name>")
def module_page(module_name: str) -> str:
    modules.ensure_public_module_exists(module_name)
    module = runtime.module_with_status(module_name)
    return render_template("module.html", module=module)


@app.get("/api/modules/status")
def modules_status() -> Response:
    return jsonify(
        {
            "modules": {
                module_name: runtime.module_runtime_status(modules.load_module_config(module_name))
                for module_name in modules.discover_module_names()
            }
        }
    )


@app.get("/api/modules/<module_name>/status")
def module_status(module_name: str) -> Response:
    modules.ensure_public_module_exists(module_name)
    module = modules.load_module_config(module_name)
    return jsonify({"id": module_name, **runtime.module_runtime_status(module)})


@app.get("/api/events")
def api_events() -> Response:
    last_event_id = request.headers.get("Last-Event-ID")
    subscriber = events.EVENT_HUB.subscribe(last_event_id)

    def stream_events() -> Iterator[str]:
        try:
            while True:
                try:
                    event = subscriber.get(timeout=15)
                except queue.Empty:
                    yield ": heartbeat\n\n"
                    continue
                yield utils.sse("app_update", event.to_payload(), event_id=event.id)
        finally:
            events.EVENT_HUB.unsubscribe(subscriber)

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
    admin_response = auth.require_admin_access()
    if admin_response is not None:
        return admin_response

    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id") or "new_module"
    content = payload.get("content")
    if not isinstance(content, str):
        return jsonify({"valid": False, "message": "Module YAML is required."}), 400

    try:
        modules.validate_module_id(str(module_id))
        modules.ensure_not_system_module(str(module_id))
        config = modules.parse_module_yaml(content)
        module = modules.validate_module_config(str(module_id), config)
    except ValueError as exc:
        return jsonify({"valid": False, "message": str(exc)}), 400

    return jsonify(
        {
            "valid": True,
            "message": "Valid configuration.",
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
    admin_response = auth.require_admin_access()
    if admin_response is not None:
        return admin_response

    payload = request.get_json(silent=True) or {}
    module_id = payload.get("module_id")
    content = payload.get("content")
    if not isinstance(module_id, str) or not module_id:
        return jsonify({"saved": False, "message": "Enter the module ID."}), 400
    if not isinstance(content, str):
        return jsonify({"saved": False, "message": "Module YAML is required."}), 400

    try:
        modules.validate_module_id(module_id)
        modules.ensure_not_system_module(module_id)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    config_path = modules.module_config_path(module_id)
    if config_path.exists():
        return jsonify({"saved": False, "message": "A module with this ID already exists."}), 409

    try:
        config = modules.parse_module_yaml(content)
        modules.validate_module_config(module_id, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    modules.write_module_yaml_content(module_id, content)
    events.signal_app_update(
        scope="modules",
        module_id=module_id,
        resources=["config", "status"],
        reason="module_created",
    )
    scheduler.SCHEDULER.wake()
    return jsonify(
        {
            "saved": True,
            "id": module_id,
            "message": "Module created.",
            "redirect": f"/modules/{module_id}/edit",
            "yaml_content": content,
        }
    ), 201


@app.put("/api/modules/<module_name>")
def update_module_config(module_name: str) -> Response:
    admin_response = auth.require_admin_access()
    if admin_response is not None:
        return admin_response

    modules.ensure_public_module_exists(module_name)
    if runtime.is_module_running(module_name):
        return jsonify(
            {
                "saved": False,
                "running": True,
                "message": "Cannot save while the module is running.",
            }
        ), 409

    payload = request.get_json(silent=True) or {}
    content = payload.get("content")
    if not isinstance(content, str):
        return jsonify({"saved": False, "message": "Module YAML is required."}), 400

    try:
        config = modules.parse_module_yaml(content)
        modules.validate_module_config(module_name, config)
    except ValueError as exc:
        return jsonify({"saved": False, "message": str(exc)}), 400

    modules.write_module_yaml_content(module_name, content)
    events.signal_app_update(
        scope="modules",
        module_id=module_name,
        resources=["config", "status"],
        reason="module_updated",
    )
    scheduler.SCHEDULER.wake()
    return jsonify(
        {
            "saved": True,
            "id": module_name,
            "message": "Module saved.",
            "yaml_content": content,
        }
    )


@app.delete("/api/modules/<module_name>")
def delete_module_config(module_name: str) -> Response:
    admin_response = auth.require_admin_access()
    if admin_response is not None:
        return admin_response

    modules.ensure_public_module_exists(module_name)
    if runtime.is_module_running(module_name):
        return jsonify(
            {
                "deleted": False,
                "running": True,
                "message": "Cannot delete while the module is running.",
            }
        ), 409

    modules.delete_module_files(module_name)
    events.signal_app_update(
        scope="modules",
        module_id=module_name,
        resources=["config", "status"],
        reason="module_deleted",
    )
    scheduler.SCHEDULER.wake()
    return jsonify({"deleted": True, "id": module_name, "message": "Module deleted."})


@app.get("/api/modules/<module_name>/logs")
def module_logs(module_name: str) -> Response:
    modules.ensure_public_module_exists(module_name)
    module = modules.load_module_config(module_name)

    since = request.args.get("since", default=0, type=int) or 0
    current_run_id = request.args.get("run_id", default="", type=str)
    since = max(since, 0)
    all_events = runtime.read_module_log(module_name)
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
            **runtime.module_runtime_status(module),
            "run_id": latest_run_id,
            "latest_sequence": latest_sequence,
            "reset": reset,
            "events": events,
        }
    )


@app.post("/api/modules/<module_name>/logs/clear")
def clear_module_logs(module_name: str) -> Response:
    modules.ensure_public_module_exists(module_name)

    if runtime.is_module_running(module_name):
        return jsonify(
            {
                "id": module_name,
                "cleared": False,
                "running": True,
                "message": "Cannot clear the log while the module is running.",
            }
        ), 409

    runtime.clear_module_log(module_name)
    events.signal_app_update(
        scope="module",
        module_id=module_name,
        resources=["logs", "status"],
        reason="module_logs_cleared",
    )
    return jsonify({"id": module_name, "cleared": True, "running": False})


@app.post("/api/modules/<module_name>/kill")
def kill_module(module_name: str) -> Response:
    modules.ensure_public_module_exists(module_name)

    active_run = runtime.get_active_run(module_name)
    if active_run is None:
        return jsonify(
            {
                "id": module_name,
                "killed": False,
                "running": False,
                "message": "The module is not running.",
            }
        ), 409

    plugin = active_run.current_plugin
    if plugin is not None:
        try:
            plugin.kill()
        except PluginKillError as exc:
            runtime.append_active_log(
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
                    "running": runtime.is_module_running(module_name),
                    "message": str(exc),
                }
            ), 409

    runtime.mark_kill_requested(module_name)
    runtime.append_active_log(
        module_name,
        "log",
        {
            "level": "error",
            "plugin": active_run.current_plugin_id,
            "message": "Kill requested by the user.",
        },
    )

    return jsonify(
        {
            "id": module_name,
            "killed": True,
            "running": True,
            "message": "Kill requested.",
        }
    )


@app.get("/api/modules/<module_name>/run")
def run_module(module_name: str) -> Response:
    modules.ensure_public_module_exists(module_name)
    module = modules.load_module_config(module_name)
    return Response(
        stream_with_context(runtime.stream_detached_batch(module, trigger=config.RUN_TRIGGER_MANUAL)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )



__all__ = ["app"]
