from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from .models import HookConfig
from .template_engine import StdinTemplateError, StdinTemplateRenderer


@dataclass(frozen=True)
class HookRunnerConfig:
    root: Path
    base_env: dict[str, str]
    template_env: dict[str, str]
    retries: int
    logger: logging.Logger


class HookRunner:
    """Executes shell hooks with retry semantics."""

    def __init__(self, config: HookRunnerConfig) -> None:
        self._root = config.root
        self._base_env = config.base_env
        self._template_env = config.template_env
        self._retries = config.retries
        self._logger = config.logger
        self._stdin_renderer = StdinTemplateRenderer(self._root)

    async def run(
        self, hook: HookConfig, extra_env: dict[str, str], context: str
    ) -> bool:
        attempts = self._retries + 1
        env = self._base_env | extra_env
        template_env = self._template_env | extra_env
        for attempt in range(1, attempts + 1):
            try:
                stdin_payload = self._render_stdin(hook, template_env)
            except StdinTemplateError as exc:
                self._logger.warning(
                    "%s failed (attempt %d/%d): stdin template error: %s",
                    context,
                    attempt,
                    attempts,
                    exc,
                )
                continue
            process = await asyncio.create_subprocess_shell(
                hook.cmd,
                cwd=str(self._root),
                env=env,
                stdin=asyncio.subprocess.PIPE if stdin_payload is not None else None,
            )
            if stdin_payload is None:
                await process.wait()
            else:
                await process.communicate(input=stdin_payload.encode("utf-8"))
            if process.returncode == 0:
                return True
            self._logger.warning(
                "%s failed (attempt %d/%d, exit=%s)",
                context,
                attempt,
                attempts,
                process.returncode,
            )
        return False

    def _render_stdin(self, hook: HookConfig, env_vars: dict[str, str]) -> str | None:
        if hook.stdin is None:
            return None
        return self._stdin_renderer.render(hook.stdin, env_vars)
