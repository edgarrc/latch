from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections.abc import Callable
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


class BasePlugin(ABC):
    plugin_type: str

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        self.plugin_id = plugin_id
        self.config = config
        self.module_id: str | None = None
        self.run_id: str | None = None
        self._metadata_callback: Callable[[dict[str, Any]], None] | None = None

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

    @abstractmethod
    def run(self) -> Iterator[PluginEvent]:
        """Run the plugin and yield real-time execution events."""

    @abstractmethod
    def kill(self) -> None:
        """Interrupt the currently running plugin execution."""
