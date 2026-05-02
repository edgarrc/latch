from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

import yaml
from flask import Response, abort, current_app, jsonify, request, session, url_for
from werkzeug.security import generate_password_hash

from . import config


def load_settings() -> dict[str, Any] | None:
    if not config.SETTINGS_PATH.exists():
        return None

    with config.SETTINGS_PATH.open("r", encoding="utf-8") as settings_file:
        settings = yaml.safe_load(settings_file) or {}
    if not isinstance(settings, dict):
        return None
    users = settings.get("users")
    if not isinstance(users, dict):
        return None
    for username in config.KNOWN_USERNAMES:
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
            config.ADMIN_USERNAME: {"password_hash": generate_password_hash(admin_password)},
            config.USER_USERNAME: {
                "password_hash": generate_password_hash(user_password)
            },
        },
        "secret_key": secrets.token_urlsafe(32),
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def write_settings(settings: dict[str, Any]) -> None:
    config.SETTINGS_PATH.write_text(
        yaml.safe_dump(settings, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def load_persisted_secret_key() -> str | None:
    settings = load_settings()
    if settings is None:
        return None

    secret_key = settings.get("secret_key")
    return secret_key if isinstance(secret_key, str) and secret_key else None


def password_hash_for_user(settings: dict[str, Any], username: str) -> str | None:
    if username not in config.KNOWN_USERNAMES:
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
    if current_app.config.get("AUTH_DISABLED"):
        return config.ADMIN_USERNAME

    username = session.get(config.SESSION_USER_KEY)
    if not isinstance(username, str):
        return None

    settings = load_settings()
    if settings is None or password_hash_for_user(settings, username) is None:
        return None

    return username


def is_authenticated() -> bool:
    return current_username() is not None


def is_admin() -> bool:
    return current_username() == config.ADMIN_USERNAME


def can_edit_modules() -> bool:
    return bool(current_app.config.get("AUTH_DISABLED")) or is_admin()


def authenticate_session(username: str) -> None:
    session.clear()
    session.permanent = True
    session[config.SESSION_USER_KEY] = username


def require_admin_access() -> Response | tuple[Response, int] | None:
    if can_edit_modules():
        return None

    message = "Only admin can edit modules."
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
