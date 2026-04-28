from __future__ import annotations

import queue
import subprocess
import threading
from collections.abc import Iterator
from typing import Any

from .base import BasePlugin, PluginEvent, PluginExecutionError


class CommandLinePlugin(BasePlugin):
    plugin_type = "command_line"

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        super().__init__(plugin_id, config)
        self.command = config.get("command")
        self.error_contains = config.get("error_contains")
        self.success_contains = config.get("success_contains")

        if not isinstance(self.command, (str, list)) or not self.command:
            raise ValueError(f"Plugin {plugin_id!r} must define a non-empty command.")
        if isinstance(self.command, list) and not all(
            isinstance(part, str) and part for part in self.command
        ):
            raise ValueError(
                f"Plugin {plugin_id!r} command list must contain only non-empty strings."
            )
        if self.error_contains is not None and not isinstance(self.error_contains, str):
            raise ValueError(f"Plugin {plugin_id!r} error_contains must be a string.")
        if self.success_contains is not None and not isinstance(self.success_contains, str):
            raise ValueError(f"Plugin {plugin_id!r} success_contains must be a string.")

    def run(self) -> Iterator[PluginEvent]:
        yield PluginEvent("info", f"Iniciando comando: {self._display_command()}")

        process: subprocess.Popen[str] | None = None
        try:
            try:
                process = subprocess.Popen(
                    self.command,
                    shell=isinstance(self.command, str),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
            except (OSError, TypeError, ValueError) as exc:
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} falhou ao iniciar comando: {exc}"
                ) from exc

            output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()
            stdout_thread = threading.Thread(
                target=self._read_stream,
                args=("stdout", process.stdout, output_queue),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=self._read_stream,
                args=("stderr", process.stderr, output_queue),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()

            combined_output: list[str] = []
            active_readers = 2

            while active_readers:
                stream_name, line = output_queue.get()
                if line is None:
                    active_readers -= 1
                    continue

                combined_output.append(line)
                level = "error" if stream_name == "stderr" else "output"
                yield PluginEvent(level, line.rstrip("\n"), stream_name)

            stdout_thread.join()
            stderr_thread.join()
            exit_code = process.wait()
            full_output = "".join(combined_output)

            if self.error_contains and self.error_contains in full_output:
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} falhou: encontrou a string de erro "
                    f"{self.error_contains!r} no output."
                )

            if exit_code != 0:
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} falhou com exit code {exit_code}."
                )

            if self.success_contains and self.success_contains not in full_output:
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} falhou: a string de sucesso "
                    f"{self.success_contains!r} não apareceu no output."
                )

            yield PluginEvent("success", f"Comando finalizado com sucesso: exit code {exit_code}")
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    @staticmethod
    def _read_stream(
        stream_name: str,
        stream: Any,
        output_queue: queue.Queue[tuple[str, str | None]],
    ) -> None:
        try:
            if stream is not None:
                for line in iter(stream.readline, ""):
                    output_queue.put((stream_name, line))
        finally:
            output_queue.put((stream_name, None))

    def _display_command(self) -> str:
        if isinstance(self.command, list):
            return " ".join(str(part) for part in self.command)
        return self.command
