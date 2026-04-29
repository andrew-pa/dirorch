from __future__ import annotations

import asyncio
import codecs
import logging
import os
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from .constants import SELECTOR_PIPE_ENV_VAR
from .entity_logging import (
    EntityLogEvent,
    EntityLogEmitter,
    EntityLogMetadataValue,
    NullEntityLogEmitter,
    utc_now,
)
from .models import HookConfig
from .pauses import ActiveShellCommandRegistry, terminate_process_group
from .template_engine import TemplateRenderError, TemplateRenderer


@dataclass(frozen=True)
class HookRunnerConfig:
    root: Path
    base_env: dict[str, str]
    template_env: dict[str, str]
    retries: int
    logger: logging.Logger
    cwd: str | None = None
    entity_log_emitter: EntityLogEmitter = NullEntityLogEmitter()
    is_entity_paused: Callable[[str], bool] = lambda _entity_id: False
    is_workflow_pause_requested: Callable[[], bool] = lambda: False
    command_registry: ActiveShellCommandRegistry | None = None


@dataclass(frozen=True)
class HookExecutionContext:
    entity_id: str | None
    phase_name: str | None
    source_state: str | None
    destination_state: str | None
    command_role: str
    context_label: str


@dataclass(frozen=True)
class CommandResult:
    succeeded: bool
    paused: bool = False
    exit_code: int | None = None
    selector_output: str | None = None
    attempts_used: int = 0


@dataclass(frozen=True)
class SelectorPipe:
    path: Path
    read_fd: int
    temp_dir: Path


class HookRunner:
    """Executes shell hooks with retry semantics."""

    def __init__(self, config: HookRunnerConfig) -> None:
        self._root = config.root
        self._base_env = config.base_env
        self._template_env = config.template_env
        self._retries = config.retries
        self._logger = config.logger
        self._cwd = config.cwd
        self._entity_log_emitter = config.entity_log_emitter
        self._is_entity_paused = config.is_entity_paused
        self._is_workflow_pause_requested = config.is_workflow_pause_requested
        self._command_registry = config.command_registry
        self._template_renderer = TemplateRenderer(self._root)

    async def run(
        self,
        hook: HookConfig,
        extra_env: dict[str, str],
        context: str,
        *,
        cwd: str | None = None,
        execution_context: HookExecutionContext | None = None,
    ) -> bool:
        return (
            await self.run_command(
                hook,
                extra_env,
                context,
                cwd=cwd,
                execution_context=execution_context,
            )
        ).succeeded

    async def run_command(
        self,
        hook: HookConfig,
        extra_env: dict[str, str],
        context: str,
        *,
        cwd: str | None = None,
        capture_selector_output: bool = False,
        execution_context: HookExecutionContext | None = None,
    ) -> CommandResult:
        attempts = self._retries + 1
        base_env = self._base_env | extra_env
        base_template_env = self._template_env | extra_env
        last_exit_code: int | None = None
        for attempt in range(1, attempts + 1):
            if self._should_pause(execution_context):
                return CommandResult(
                    succeeded=False,
                    paused=True,
                    exit_code=None,
                    attempts_used=attempt - 1,
                )
            selector_pipe = (
                self._create_selector_pipe() if capture_selector_output else None
            )
            try:
                selector_env = (
                    {SELECTOR_PIPE_ENV_VAR: str(selector_pipe.path)}
                    if selector_pipe is not None
                    else {}
                )
                env = base_env | selector_env
                template_env = base_template_env | selector_env
                try:
                    rendered_cmd = self._render_cmd(hook, template_env)
                except TemplateRenderError as exc:
                    last_exit_code = None
                    await self._emit_command_finished(
                        hook.cmd,
                        attempt,
                        execution_context,
                        exit_code=None,
                        error=f"cmd template error: {exc}",
                    )
                    self._logger.warning(
                        "%s failed (attempt %d/%d): cmd template error: %s",
                        context,
                        attempt,
                        attempts,
                        exc,
                    )
                    if attempt < attempts:
                        await self._emit_command_retrying(
                            attempt,
                            attempts,
                            execution_context,
                            reason=f"cmd template error: {exc}",
                        )
                    continue
                await self._emit_command_started(rendered_cmd, attempt, execution_context)
                try:
                    stdin_payload = self._render_stdin(hook, template_env)
                except TemplateRenderError as exc:
                    last_exit_code = None
                    await self._emit_command_finished(
                        rendered_cmd,
                        attempt,
                        execution_context,
                        exit_code=None,
                        error=f"stdin template error: {exc}",
                    )
                    self._logger.warning(
                        "%s failed (attempt %d/%d): stdin template error: %s",
                        context,
                        attempt,
                        attempts,
                        exc,
                    )
                    if attempt < attempts:
                        await self._emit_command_retrying(
                            attempt,
                            attempts,
                            execution_context,
                            reason=f"stdin template error: {exc}",
                        )
                    continue
                try:
                    rendered_cwd = self._render_cwd(hook, cwd, template_env)
                except TemplateRenderError as exc:
                    last_exit_code = None
                    await self._emit_command_finished(
                        rendered_cmd,
                        attempt,
                        execution_context,
                        exit_code=None,
                        error=f"cwd template error: {exc}",
                    )
                    self._logger.warning(
                        "%s failed (attempt %d/%d): cwd template error: %s",
                        context,
                        attempt,
                        attempts,
                        exc,
                    )
                    if attempt < attempts:
                        await self._emit_command_retrying(
                            attempt,
                            attempts,
                            execution_context,
                            reason=f"cwd template error: {exc}",
                        )
                    continue
                result = await self._run_once(
                    rendered_cmd,
                    stdin_payload,
                    env,
                    cwd=rendered_cwd,
                    selector_pipe=selector_pipe,
                    attempt=attempt,
                    execution_context=execution_context,
                )
                if result.paused:
                    await self._emit_command_terminated(
                        rendered_cmd,
                        attempt,
                        execution_context,
                    )
                    self._logger.info("%s interrupted because entity is paused", context)
                    return CommandResult(
                        succeeded=False,
                        paused=True,
                        exit_code=result.exit_code,
                        attempts_used=attempt,
                    )
                last_exit_code = result.exit_code
                await self._emit_command_finished(
                    rendered_cmd,
                    attempt,
                    execution_context,
                    exit_code=result.exit_code,
                )
                if result.succeeded:
                    return CommandResult(
                        succeeded=True,
                        exit_code=result.exit_code,
                        selector_output=result.selector_output,
                        attempts_used=attempt,
                    )
                self._logger.warning(
                    "%s failed (attempt %d/%d, exit=%s)",
                    context,
                    attempt,
                    attempts,
                    result.exit_code,
                )
                if attempt < attempts:
                    await self._emit_command_retrying(
                        attempt,
                        attempts,
                        execution_context,
                        reason=f"exit={result.exit_code}",
                    )
            finally:
                if selector_pipe is not None:
                    self._cleanup_selector_pipe(selector_pipe)
        return CommandResult(
            succeeded=False,
            exit_code=last_exit_code,
            attempts_used=attempts,
        )

    def _render_cmd(self, hook: HookConfig, env_vars: dict[str, str]) -> str:
        return self._template_renderer.render(hook.cmd, env_vars)

    def _render_stdin(self, hook: HookConfig, env_vars: dict[str, str]) -> str | None:
        if hook.stdin is None:
            return None
        return self._template_renderer.render(hook.stdin, env_vars)

    def _render_cwd(
        self,
        hook: HookConfig,
        default_cwd: str | None,
        env_vars: dict[str, str],
    ) -> Path:
        raw_cwd = hook.cwd if hook.cwd is not None else default_cwd
        raw_cwd = self._cwd if raw_cwd is None else raw_cwd
        if raw_cwd is None:
            return self._root
        rendered = self._template_renderer.render(raw_cwd, env_vars)
        path = Path(rendered).expanduser()
        if path.is_absolute():
            return path
        return self._root / path

    async def _run_once(
        self,
        cmd: str,
        stdin_payload: str | None,
        env: dict[str, str],
        *,
        cwd: Path,
        selector_pipe: SelectorPipe | None,
        attempt: int,
        execution_context: HookExecutionContext | None,
    ) -> CommandResult:
        try:
            process = await asyncio.create_subprocess_shell(
                cmd,
                cwd=str(cwd),
                env=env,
                stdin=asyncio.subprocess.PIPE if stdin_payload is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            self._logger.warning("Failed to start command in cwd '%s': %s", cwd, exc)
            return CommandResult(
                succeeded=False,
                exit_code=None,
                attempts_used=attempt,
            )
        registration = await self._register_process(
            execution_context,
            process,
            cmd,
            attempt,
        )

        assert process.stdout is not None
        assert process.stderr is not None

        stdout_task = asyncio.create_task(
            self._drain_stream(
                process.stdout,
                "stdout",
                cmd,
                attempt,
                execution_context,
            )
        )
        stderr_task = asyncio.create_task(
            self._drain_stream(
                process.stderr,
                "stderr",
                cmd,
                attempt,
                execution_context,
            )
        )
        stdin_task = asyncio.create_task(self._write_stdin(process, stdin_payload))

        try:
            await process.wait()
            await stdin_task
            await asyncio.gather(stdout_task, stderr_task)
        finally:
            for task in (stdin_task, stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdin_task, stdout_task, stderr_task, return_exceptions=True)
            await self._unregister_process(execution_context, registration)

        selector_output = None
        if selector_pipe is not None:
            selector_output = await asyncio.to_thread(
                self._read_selector_output,
                selector_pipe.read_fd,
            )
        return CommandResult(
            succeeded=process.returncode == 0,
            paused=registration.pause_requested if registration is not None else False,
            exit_code=process.returncode,
            selector_output=selector_output,
            attempts_used=attempt,
        )

    async def _write_stdin(
        self,
        process: asyncio.subprocess.Process,
        stdin_payload: str | None,
    ) -> None:
        if process.stdin is None:
            return
        if stdin_payload is not None:
            process.stdin.write(stdin_payload.encode("utf-8"))
            await process.stdin.drain()
        process.stdin.close()
        await process.stdin.wait_closed()

    async def _drain_stream(
        self,
        stream: asyncio.StreamReader,
        stream_name: str,
        cmd: str,
        attempt: int,
        execution_context: HookExecutionContext | None,
    ) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        while True:
            chunk = await stream.read(1024)
            if not chunk:
                tail = decoder.decode(b"", final=True)
                if tail:
                    await self._emit_stream_chunk(
                        cmd,
                        attempt,
                        execution_context,
                        stream_name,
                        tail,
                    )
                return
            text = decoder.decode(chunk, final=False)
            if text:
                await self._emit_stream_chunk(
                    cmd,
                    attempt,
                    execution_context,
                    stream_name,
                    text,
                )

    def _create_selector_pipe(self) -> SelectorPipe:
        temp_dir = Path(tempfile.mkdtemp(prefix="dirorch-selector-"))
        pipe_path = temp_dir / "signal.pipe"
        os.mkfifo(pipe_path, 0o600)
        read_fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
        return SelectorPipe(path=pipe_path, read_fd=read_fd, temp_dir=temp_dir)

    def _cleanup_selector_pipe(self, selector_pipe: SelectorPipe) -> None:
        with suppress(OSError):
            os.close(selector_pipe.read_fd)
        with suppress(FileNotFoundError):
            selector_pipe.path.unlink()
        with suppress(OSError):
            selector_pipe.temp_dir.rmdir()

    def _read_selector_output(self, pipe_read: int) -> str:
        os.set_blocking(pipe_read, True)
        with os.fdopen(pipe_read, "rb", closefd=False) as stream:
            raw = stream.read()
        output = raw.decode("utf-8").strip()
        if not output:
            return ""
        return output.splitlines()[0]

    async def _emit_command_started(
        self,
        command: str,
        attempt: int,
        execution_context: HookExecutionContext | None,
    ) -> None:
        await self._emit_event(
            execution_context,
            kind="command.started",
            command=command,
            attempt=attempt,
        )

    async def _emit_command_terminated(
        self,
        command: str,
        attempt: int,
        execution_context: HookExecutionContext | None,
    ) -> None:
        await self._emit_event(
            execution_context,
            kind="command.terminated",
            command=command,
            attempt=attempt,
            metadata={"reason": "entity paused"},
        )

    async def _emit_command_finished(
        self,
        command: str,
        attempt: int,
        execution_context: HookExecutionContext | None,
        *,
        exit_code: int | None,
        error: str | None = None,
    ) -> None:
        metadata: dict[str, str | int | bool | None] = {}
        if error is not None:
            metadata["error"] = error
        await self._emit_event(
            execution_context,
            kind="command.finished",
            command=command,
            attempt=attempt,
            returncode=exit_code,
            metadata=metadata,
        )

    async def _emit_command_retrying(
        self,
        attempt: int,
        attempts: int,
        execution_context: HookExecutionContext | None,
        *,
        reason: str,
    ) -> None:
        if attempt >= attempts:
            return
        await self._emit_event(
            execution_context,
            kind="command.retrying",
            attempt=attempt,
            metadata={
                "next_attempt": attempt + 1,
                "reason": reason,
            },
        )

    async def _emit_stream_chunk(
        self,
        cmd: str,
        attempt: int,
        execution_context: HookExecutionContext | None,
        stream_name: str,
        text: str,
    ) -> None:
        await self._emit_event(
            execution_context,
            kind="command.stream",
            command=cmd,
            attempt=attempt,
            stream=stream_name,
            text=text,
        )

    async def _emit_event(
        self,
        execution_context: HookExecutionContext | None,
        *,
        kind: str,
        command: str | None = None,
        attempt: int | None = None,
        returncode: int | None = None,
        stream: str | None = None,
        text: str | None = None,
        metadata: dict[str, EntityLogMetadataValue] | None = None,
    ) -> None:
        if execution_context is None or execution_context.entity_id is None:
            return
        event_metadata: dict[str, EntityLogMetadataValue] = {
            "command_role": execution_context.command_role,
            "context_label": execution_context.context_label,
        }
        if execution_context.command_role == "transition_selector_destination":
            event_metadata["selector_kind"] = "destination"
        elif execution_context.command_role == "transition_selector_jump":
            event_metadata["selector_kind"] = "jump"
        if metadata is not None:
            event_metadata.update(metadata)
        await self._entity_log_emitter.emit(
            EntityLogEvent(
                entity_id=execution_context.entity_id,
                timestamp=utc_now(),
                kind=kind,
                phase=execution_context.phase_name,
                source_state=execution_context.source_state,
                destination_state=execution_context.destination_state,
                command=command,
                attempt=attempt,
                returncode=returncode,
                stream=stream,
                text=text,
                metadata=event_metadata,
            )
        )

    def _should_pause(self, execution_context: HookExecutionContext | None) -> bool:
        if self._is_workflow_pause_requested():
            return True
        return (
            execution_context is not None
            and execution_context.entity_id is not None
            and self._is_entity_paused(execution_context.entity_id)
        )

    async def _register_process(
        self,
        execution_context: HookExecutionContext | None,
        process: asyncio.subprocess.Process,
        cmd: str,
        attempt: int,
    ):
        if (
            self._command_registry is None
            or execution_context is None
            or execution_context.entity_id is None
        ):
            return None
        return await self._command_registry.register(
            execution_context.entity_id,
            cmd,
            attempt,
            lambda: terminate_process_group(process.pid),
        )

    async def _unregister_process(
        self,
        execution_context: HookExecutionContext | None,
        registration,
    ) -> None:
        if (
            registration is None
            or self._command_registry is None
            or execution_context is None
            or execution_context.entity_id is None
        ):
            return
        await self._command_registry.unregister(execution_context.entity_id, registration)
