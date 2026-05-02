from __future__ import annotations

import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


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
