from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterator


@dataclass(frozen=True)
class PluginEvent:
    level: str
    message: str
    stream: str | None = None


class PluginExecutionError(RuntimeError):
    """Raised when a plugin fails and the batch must stop."""


class BasePlugin(ABC):
    plugin_type: str

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        self.plugin_id = plugin_id
        self.config = config

    @abstractmethod
    def run(self) -> Iterator[PluginEvent]:
        """Run the plugin and yield real-time execution events."""
