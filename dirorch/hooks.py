from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from .models import HookConfig


@dataclass(frozen=True)
class HookRunnerConfig:
    root: Path
    base_env: dict[str, str]
    retries: int
    logger: logging.Logger


class HookRunner:
    """Executes shell hooks with retry semantics."""

    def __init__(self, config: HookRunnerConfig) -> None:
        self._root = config.root
        self._base_env = config.base_env
        self._retries = config.retries
        self._logger = config.logger

    async def run(
        self, hook: HookConfig, extra_env: dict[str, str], context: str
    ) -> bool:
        attempts = self._retries + 1
        env = self._base_env | extra_env
        for attempt in range(1, attempts + 1):
            process = await asyncio.create_subprocess_shell(
                hook.cmd,
                cwd=str(self._root),
                env=env,
            )
            await process.wait()
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
