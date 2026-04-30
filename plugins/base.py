from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
import queue
import threading
import time
from typing import Any, Iterator


@dataclass(frozen=True)
class PluginEvent:
    level: str
    message: str
    stream: str | None = None


class PluginExecutionError(RuntimeError):
    """Raised when a plugin fails and the batch must stop."""


class PluginKillError(RuntimeError):
    """Raised when a plugin could not be interrupted."""


class PluginKilledError(RuntimeError):
    """Raised when a plugin was interrupted by user request."""


class _PluginTimeoutError(RuntimeError):
    """Internal signal used to retry or fail a timed-out plugin."""


_PLUGIN_RUN_DONE = object()
_PLUGIN_TIMEOUT_CLEANUP_SECONDS = 10.0


class BasePlugin(ABC):
    plugin_type: str

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        self.plugin_id = plugin_id
        self.config = config
        self.timeout = self._parse_timeout(plugin_id, config.get("timeout"))
        self.timeout_retries = self._parse_timeout_retries(
            plugin_id,
            config.get("timeout_retries"),
        )
        self.module_id: str | None = None
        self.run_id: str | None = None
        self._metadata_callback: Callable[[dict[str, Any]], None] | None = None

    @staticmethod
    def _parse_timeout(plugin_id: str, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"Plugin {plugin_id!r} timeout must be a positive integer.")
        if value <= 0:
            raise ValueError(f"Plugin {plugin_id!r} timeout must be a positive integer.")
        return value

    @staticmethod
    def _parse_timeout_retries(plugin_id: str, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(
                f"Plugin {plugin_id!r} timeout_retries must be a non-negative integer."
            )
        if value < 0:
            raise ValueError(
                f"Plugin {plugin_id!r} timeout_retries must be a non-negative integer."
            )
        return value

    def set_runtime_context(
        self,
        module_id: str,
        run_id: str,
        metadata_callback: Callable[[dict[str, Any]], None],
    ) -> None:
        self.module_id = module_id
        self.run_id = run_id
        self._metadata_callback = metadata_callback

    def update_runtime_metadata(self, metadata: dict[str, Any]) -> None:
        if self._metadata_callback is not None:
            self._metadata_callback(metadata)

    def run(self) -> Iterator[PluginEvent]:
        """Run the plugin and yield real-time execution events."""
        attempts = 1 + (self.timeout_retries if self.timeout is not None else 0)

        for attempt in range(1, attempts + 1):
            if attempt > 1:
                self._prepare_retry()
                yield PluginEvent(
                    "info",
                    (
                        f"Tentando novamente plugin {self.plugin_id!r} apos timeout "
                        f"({attempt}/{attempts})."
                    ),
                )

            try:
                yield from self._run_once_with_timeout()
                return
            except _PluginTimeoutError:
                if attempt < attempts:
                    yield PluginEvent(
                        "error",
                        (
                            f"Plugin {self.plugin_id!r} excedeu timeout de "
                            f"{self.timeout} segundo(s); retry {attempt}/{self.timeout_retries}."
                        ),
                    )
                    continue
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} excedeu timeout de {self.timeout} segundo(s)."
                )

    def _run_once_with_timeout(self) -> Iterator[PluginEvent]:
        if self.timeout is None:
            yield from self._run_once()
            return

        event_queue: queue.Queue[PluginEvent | BaseException | object] = queue.Queue()

        def worker() -> None:
            try:
                for event in self._run_once():
                    event_queue.put(event)
            except BaseException as exc:
                event_queue.put(exc)
            finally:
                event_queue.put(_PLUGIN_RUN_DONE)

        thread = threading.Thread(
            target=worker,
            name=f"plugin-timeout-{self.plugin_id}",
            daemon=True,
        )
        thread.start()
        deadline = time.monotonic() + self.timeout

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                item = event_queue.get(timeout=remaining)
            except queue.Empty:
                break

            if item is _PLUGIN_RUN_DONE:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

        self.kill()
        self.update_runtime_metadata(
            {
                "timeout": self.timeout,
                "timeout_retries": self.timeout_retries,
                "timeout_triggered": True,
            }
        )
        self._wait_for_timed_out_worker(thread, event_queue)
        raise _PluginTimeoutError()

    def _wait_for_timed_out_worker(
        self,
        thread: threading.Thread,
        event_queue: queue.Queue[PluginEvent | BaseException | object],
    ) -> None:
        deadline = time.monotonic() + _PLUGIN_TIMEOUT_CLEANUP_SECONDS
        while time.monotonic() < deadline:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                item = event_queue.get(timeout=timeout)
            except queue.Empty:
                break

            if item is _PLUGIN_RUN_DONE:
                thread.join(timeout=0)
                return
            if isinstance(item, PluginKilledError):
                continue
            if isinstance(item, BaseException):
                continue

        thread.join(timeout=0)
        if thread.is_alive():
            raise PluginExecutionError(
                f"Plugin {self.plugin_id!r} excedeu timeout e nao encerrou apos kill."
            )

    def _prepare_retry(self) -> None:
        """Reset per-attempt state before a retry."""

    @abstractmethod
    def _run_once(self) -> Iterator[PluginEvent]:
        """Run one plugin attempt and yield real-time execution events."""

    @abstractmethod
    def kill(self) -> None:
        """Interrupt the currently running plugin execution."""
