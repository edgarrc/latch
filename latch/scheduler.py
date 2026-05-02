from __future__ import annotations

import os
import threading
from datetime import datetime

from flask import current_app

from . import config, modules, runtime, utils
from .auth import load_settings


class ModuleScheduler:
    def __init__(self, poll_seconds: float = config.SCHEDULER_POLL_SECONDS) -> None:
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
        self._refresh_schedules(now or utils.local_now())

    def run_due_once(self, now: datetime | None = None) -> list[str]:
        return self._run_due(now or utils.local_now())

    def _run(self) -> None:
        while True:
            now = utils.local_now()
            self._refresh_schedules(now)
            self._run_due(now)
            timeout = self._seconds_until_next_run(utils.local_now())
            self._wake_event.wait(timeout=timeout)
            self._wake_event.clear()

    def _refresh_schedules(self, now: datetime) -> None:
        module_names = set(modules.discover_module_names())
        with self._lock:
            for module_name in set(self._schedules) - module_names:
                self._schedules.pop(module_name, None)
                self._next_runs.pop(module_name, None)

        for module_name in sorted(module_names):
            try:
                module = modules.load_module_config(module_name)
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
                next_run = utils.next_schedule_time(schedule, now)
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
                next_run = utils.next_schedule_time(schedule, max(now, scheduled_for))
            except ValueError:
                with self._lock:
                    self._schedules.pop(module_name, None)
                    self._next_runs.pop(module_name, None)
                continue

            with self._lock:
                if self._schedules.get(module_name) == schedule:
                    self._next_runs[module_name] = next_run

            try:
                module = modules.load_module_config(module_name)
            except Exception:
                continue
            if not module.get("schedule") or not module.get("schedule_enabled"):
                continue
            if runtime.is_module_running(module_name):
                continue

            runtime.start_detached_batch(
                module,
                trigger=config.RUN_TRIGGER_SCHEDULE,
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
    if current_app.config.get("SCHEDULER_DISABLED"):
        return
    if os.environ.get("LATCH_SCHEDULER_DISABLED") == "1":
        return
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return
    if not current_app.config.get("AUTH_DISABLED") and load_settings() is None:
        return
    SCHEDULER.start()
