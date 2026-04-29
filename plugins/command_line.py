from __future__ import annotations

import os
import queue
import shlex
import subprocess
import threading
from collections.abc import Iterator
from typing import Any

from .base import (
    BasePlugin,
    PluginEvent,
    PluginExecutionError,
    PluginKillError,
    PluginKilledError,
)
from .variables import mask_sensitive_text


COMMAND_LOG_PREVIEW_LIMIT = 500


class CommandLinePlugin(BasePlugin):
    plugin_type = "command_line"

    def __init__(self, plugin_id: str, config: dict[str, Any]) -> None:
        super().__init__(plugin_id, config)
        self.command = config.get("command")
        self.pipeline = config.get("pipeline")
        self.error_contains = config.get("error_contains")
        self.success_contains = config.get("success_contains")
        self.display_command = config.get("_display_command")
        self.display_pipeline = config.get("_display_pipeline")
        self.sensitive_values = tuple(config.get("_sensitive_values", ()))
        self._process: subprocess.Popen[str] | None = None
        self._process_group_id: int | None = None
        self._kill_requested = False
        self._process_lock = threading.Lock()

        if not isinstance(self.command, (str, list)) or not self.command:
            raise ValueError(f"Plugin {plugin_id!r} must define a non-empty command.")
        if isinstance(self.command, list) and not all(
            isinstance(part, str) and part for part in self.command
        ):
            raise ValueError(
                f"Plugin {plugin_id!r} command list must contain only non-empty strings."
            )
        if self.pipeline is not None:
            if not isinstance(self.pipeline, str) or not self.pipeline.strip():
                raise ValueError(f"Plugin {plugin_id!r} pipeline must be a non-empty string.")
        if self.error_contains is not None and not isinstance(self.error_contains, str):
            raise ValueError(f"Plugin {plugin_id!r} error_contains must be a string.")
        if self.success_contains is not None and not isinstance(self.success_contains, str):
            raise ValueError(f"Plugin {plugin_id!r} success_contains must be a string.")
        if self.display_command is not None and not isinstance(self.display_command, str):
            raise ValueError(f"Plugin {plugin_id!r} display command must be a string.")
        if self.display_pipeline is not None and not isinstance(self.display_pipeline, str):
            raise ValueError(f"Plugin {plugin_id!r} display pipeline must be a string.")
        if not all(isinstance(value, str) for value in self.sensitive_values):
            raise ValueError(f"Plugin {plugin_id!r} sensitive values must be strings.")

    def run(self) -> Iterator[PluginEvent]:
        yield PluginEvent("info", f"Iniciando comando: {self._display_command_preview()}")

        process: subprocess.Popen[str] | None = None
        try:
            try:
                popen_command, use_shell = self._popen_command()
                process = subprocess.Popen(
                    popen_command,
                    shell=use_shell,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    start_new_session=True,
                )
            except (OSError, TypeError, ValueError) as exc:
                raise PluginExecutionError(
                    self._mask_text(
                        f"Plugin {self.plugin_id!r} falhou ao iniciar comando: {exc}"
                    )
                ) from exc

            process_group_id = os.getpgid(process.pid)
            with self._process_lock:
                self._process = process
                self._process_group_id = process_group_id

            self.update_runtime_metadata(
                {
                    "pid": process.pid,
                    "pgid": process_group_id,
                    "command": self._display_command(),
                }
            )
            yield PluginEvent(
                "info",
                f"Processo iniciado: pid={process.pid}, pgid={process_group_id}",
            )
            if self._was_kill_requested():
                self._kill_process_group(process_group_id)
                yield PluginEvent("error", "Kill pendente aplicado ao processo recém-iniciado.")

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
                yield PluginEvent(level, self._mask_text(line.rstrip("\n")), stream_name)

            stdout_thread.join()
            stderr_thread.join()
            exit_code = process.wait()
            full_output = "".join(combined_output)

            if self._was_kill_requested():
                raise PluginKilledError(
                    f"Plugin {self.plugin_id!r} foi interrompido por solicitação do usuário."
                )

            if self.error_contains and self.error_contains in full_output:
                raise PluginExecutionError(
                    self._mask_text(
                        f"Plugin {self.plugin_id!r} falhou: encontrou a string de erro "
                        f"{self.error_contains!r} no output."
                    )
                )

            if exit_code != 0:
                raise PluginExecutionError(
                    f"Plugin {self.plugin_id!r} falhou com exit code {exit_code}."
                )

            if self.success_contains and self.success_contains not in full_output:
                raise PluginExecutionError(
                    self._mask_text(
                        f"Plugin {self.plugin_id!r} falhou: a string de sucesso "
                        f"{self.success_contains!r} não apareceu no output."
                    )
                )

            yield PluginEvent("success", f"Comando finalizado com sucesso: exit code {exit_code}")
        finally:
            if process is not None and process.poll() is None:
                process_group_id = self._process_group_id
                if process_group_id is not None:
                    subprocess.run(
                        f"kill -TERM -{process_group_id}",
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                else:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if process_group_id is not None:
                        subprocess.run(
                            f"kill -KILL -{process_group_id}",
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                    else:
                        process.kill()
                    process.wait()
            with self._process_lock:
                if self._process is process:
                    self._process = None
                    self._process_group_id = None

    def kill(self) -> None:
        with self._process_lock:
            process = self._process
            process_group_id = self._process_group_id
            self._kill_requested = True

        if process is None or process.poll() is not None or process_group_id is None:
            self.update_runtime_metadata({"kill_requested": True, "kill_pending": True})
            return

        self._kill_process_group(process_group_id)
        self.update_runtime_metadata({"kill_requested": True})

    def _kill_process_group(self, process_group_id: int) -> None:
        kill_command = f"kill -KILL -{process_group_id}"
        result = subprocess.run(
            kill_command,
            shell=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            with self._process_lock:
                self._kill_requested = False
            raise PluginKillError(
                f"Falha ao interromper plugin {self.plugin_id!r} com {kill_command!r}: "
                f"{result.stderr.strip() or result.stdout.strip() or 'sem detalhes'}"
            )
        self.update_runtime_metadata({"kill_command": kill_command})

    def _was_kill_requested(self) -> bool:
        with self._process_lock:
            return self._kill_requested

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
        command = self._display_command_head()
        if self.pipeline is None:
            return self._mask_text(command)

        return self._mask_text(f"{command} | {self._display_pipeline()}")

    def _display_command_head(self) -> str:
        if self.display_command is not None:
            return self.display_command
        if isinstance(self.command, list):
            if self.pipeline is not None:
                return shlex.join(self.command)
            return " ".join(str(part) for part in self.command)
        return self.command

    def _display_pipeline(self) -> str:
        if self.display_pipeline is not None:
            return self.display_pipeline
        return self.pipeline or ""

    def _display_command_preview(self) -> str:
        command = self._display_command()
        if len(command) <= COMMAND_LOG_PREVIEW_LIMIT:
            return command
        return f"{command[:COMMAND_LOG_PREVIEW_LIMIT]}[...]"

    def _popen_command(self) -> tuple[str | list[str], bool]:
        if self.pipeline is None:
            return self.command, isinstance(self.command, str)

        if isinstance(self.command, str):
            command_text = self.command
        else:
            command_text = shlex.join(self.command)
        return [
            "/bin/bash",
            "-o",
            "pipefail",
            "-c",
            f"{command_text} | {self.pipeline}",
        ], False

    def _mask_text(self, text: str) -> str:
        return mask_sensitive_text(text, self.sensitive_values)
