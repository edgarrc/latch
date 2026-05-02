from __future__ import annotations

import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
MODULES_ROOT = BASE_DIR / "modules"
USER_MODULES_DIR = MODULES_ROOT / "user"
SYSTEM_MODULES_DIR = MODULES_ROOT / "system"
LOCKS_DIR = BASE_DIR / "locks"
TEMP_DIR = BASE_DIR / "temp"
SETTINGS_PATH = BASE_DIR / "settings.yaml"
APP_NAME = "Latch"
APP_TAGLINE = "Batch manager"
APP_GITHUB_URL = "https://github.com/edgarrc/latch"
ADMIN_USERNAME = "admin"
USER_USERNAME = "user"
KNOWN_USERNAMES = (ADMIN_USERNAME, USER_USERNAME)
SESSION_USER_KEY = "user"
MODULE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
GENERATED_TEMP_PATTERNS = ("temp_*.jsonl", "active_*.json")
RUN_TRIGGER_MANUAL = "manual"
RUN_TRIGGER_SCHEDULE = "schedule"
RUN_TRIGGERS = {RUN_TRIGGER_MANUAL, RUN_TRIGGER_SCHEDULE}
SCHEDULER_POLL_SECONDS = 15.0
SSE_HEARTBEAT_SECONDS = 15.0


def ensure_runtime_directories() -> None:
    USER_MODULES_DIR.mkdir(parents=True, exist_ok=True)
    SYSTEM_MODULES_DIR.mkdir(parents=True, exist_ok=True)
    LOCKS_DIR.mkdir(exist_ok=True)
    TEMP_DIR.mkdir(exist_ok=True)
