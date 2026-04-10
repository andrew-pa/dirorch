from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from .models import HookConfig
from .template_engine import TemplateRenderError, TemplateRenderer


@dataclass(frozen=True)
class HookRunnerConfig:
    root: Path
    base_env: dict[str, str]
    template_env: dict[str, str]
    retries: int
    logger: logging.Logger


@dataclass(frozen=True)
class CommandResult:
    succeeded: bool
    exit_code: int | None = None
    selector_output: str | None = None


class HookRunner:
    """Executes shell hooks with retry semantics."""

    def __init__(self, config: HookRunnerConfig) -> None:
        self._root = config.root
        self._base_env = config.base_env
        self._template_env = config.template_env
        self._retries = config.retries
        self._logger = config.logger
        self._stdin_renderer = TemplateRenderer(self._root)

    async def run(
        self, hook: HookConfig, extra_env: dict[str, str], context: str
    ) -> bool:
        return (await self.run_command(hook, extra_env, context)).succeeded

    async def run_command(
        self,
        hook: HookConfig,
        extra_env: dict[str, str],
        context: str,
        *,
        capture_selector_output: bool = False,
    ) -> CommandResult:
        attempts = self._retries + 1
        env = self._base_env | extra_env
        template_env = self._template_env | extra_env
        for attempt in range(1, attempts + 1):
            try:
                stdin_payload = self._render_stdin(hook, template_env)
            except TemplateRenderError as exc:
                self._logger.warning(
                    "%s failed (attempt %d/%d): stdin template error: %s",
                    context,
                    attempt,
                    attempts,
                    exc,
                )
                continue
            result = await self._run_once(
                hook.cmd,
                stdin_payload,
                env,
                capture_selector_output=capture_selector_output,
            )
            if result.succeeded:
                return result
            self._logger.warning(
                "%s failed (attempt %d/%d, exit=%s)",
                context,
                attempt,
                attempts,
                result.exit_code,
            )
        return CommandResult(succeeded=False)

    def _render_stdin(self, hook: HookConfig, env_vars: dict[str, str]) -> str | None:
        if hook.stdin is None:
            return None
        return self._stdin_renderer.render(hook.stdin, env_vars)

    async def _run_once(
        self,
        cmd: str,
        stdin_payload: str | None,
        env: dict[str, str],
        *,
        capture_selector_output: bool,
    ) -> CommandResult:
        if not capture_selector_output:
            process = await asyncio.create_subprocess_shell(
                cmd,
                cwd=str(self._root),
                env=env,
                stdin=asyncio.subprocess.PIPE if stdin_payload is not None else None,
            )
            if stdin_payload is None:
                await process.wait()
            else:
                await process.communicate(input=stdin_payload.encode("utf-8"))
            return CommandResult(
                succeeded=process.returncode == 0,
                exit_code=process.returncode,
            )

        pipe_read, pipe_write = os.pipe()
        try:
            wrapped_cmd = self._wrap_selector_command(cmd, pipe_write)
            process = await asyncio.create_subprocess_shell(
                wrapped_cmd,
                cwd=str(self._root),
                env=env,
                stdin=asyncio.subprocess.PIPE if stdin_payload is not None else None,
                pass_fds=(pipe_write,),
            )
            os.close(pipe_write)
            pipe_write = -1

            if stdin_payload is None:
                await process.wait()
            else:
                await process.communicate(input=stdin_payload.encode("utf-8"))

            selector_output = await asyncio.to_thread(self._read_selector_output, pipe_read)
            return CommandResult(
                succeeded=process.returncode == 0,
                exit_code=process.returncode,
                selector_output=selector_output,
            )
        finally:
            if pipe_write >= 0:
                os.close(pipe_write)
            os.close(pipe_read)

    def _wrap_selector_command(self, cmd: str, selector_fd: int) -> str:
        return f"exec 3>&{selector_fd}; exec {selector_fd}>&-; {cmd}"

    def _read_selector_output(self, pipe_read: int) -> str:
        with os.fdopen(pipe_read, "rb", closefd=False) as stream:
            raw = stream.read()
        output = raw.decode("utf-8").strip()
        if not output:
            return ""
        return output.splitlines()[0]
