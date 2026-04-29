from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .errors import WorkflowError


class EntityPauseStore:
    """Persists entity pause state independent of runtime execution snapshot."""

    def __init__(self, root: Path, file_name: str) -> None:
        self._path = root / file_name
        self._paused: set[str] | None = None

    def is_paused(self, entity_id: str) -> bool:
        return entity_id in self._load()

    def list_paused(self) -> set[str]:
        return set(self._load())

    def set_paused(self, entity_id: str, paused: bool) -> bool:
        paused_entities = self._load()
        changed = False
        if paused:
            if entity_id not in paused_entities:
                paused_entities.add(entity_id)
                changed = True
        elif entity_id in paused_entities:
            paused_entities.remove(entity_id)
            changed = True
        if changed:
            self._save(paused_entities)
        return paused

    def clear(self, entity_id: str) -> None:
        self.set_paused(entity_id, False)

    def _load(self) -> set[str]:
        if self._paused is not None:
            return self._paused

        if not self._path.exists():
            self._paused = set()
            return self._paused

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkflowError(
                f"Unable to read pause file {self._path}: {exc}"
            ) from exc

        if not isinstance(payload, list) or not all(
            isinstance(item, str) and item for item in payload
        ):
            raise WorkflowError(
                f"Invalid pause file {self._path}: expected list of non-empty strings"
            )

        self._paused = set(payload)
        return self._paused

    def _save(self, paused_entities: set[str]) -> None:
        self._path.write_text(
            json.dumps(sorted(paused_entities), indent=2),
            encoding="utf-8",
        )


class WorkflowPauseController:
    """Coordinates process-wide workflow pause state."""

    def __init__(self) -> None:
        self._pause_requested = False
        self._paused = False
        self._condition = asyncio.Condition()

    def is_pause_requested(self) -> bool:
        return self._pause_requested

    def is_paused(self) -> bool:
        return self._paused

    def state(self) -> str:
        if self._paused:
            return "paused"
        if self._pause_requested:
            return "pausing"
        return "running"

    async def request_pause(self) -> None:
        async with self._condition:
            self._pause_requested = True
            self._paused = False
            self._condition.notify_all()

    async def mark_paused(self) -> None:
        async with self._condition:
            if self._pause_requested:
                self._paused = True
            self._condition.notify_all()

    async def resume(self) -> None:
        async with self._condition:
            self._pause_requested = False
            self._paused = False
            self._condition.notify_all()

    async def wait_if_paused(self) -> None:
        async with self._condition:
            while self._pause_requested:
                await self._condition.wait()


@dataclass
class ActiveShellCommandHandle:
    terminate: Callable[[], None]
    command: str
    attempt: int
    started_at: str
    pause_requested: bool = False

    def request_pause(self) -> None:
        self.pause_requested = True
        self.terminate()


class ActiveShellCommandRegistry:
    """Tracks per-entity shell commands so pause can terminate them promptly."""

    def __init__(self, should_pause: Callable[[str], bool]) -> None:
        self._should_pause = should_pause
        self._processes: dict[str, dict[int, ActiveShellCommandHandle]] = {}
        self._condition = asyncio.Condition()

    async def register(
        self,
        entity_id: str,
        command: str,
        attempt: int,
        terminate: Callable[[], None],
    ) -> ActiveShellCommandHandle:
        handle = ActiveShellCommandHandle(
            terminate=terminate,
            command=command,
            attempt=attempt,
            started_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        async with self._condition:
            handles = self._processes.setdefault(entity_id, {})
            handles[id(handle)] = handle
            paused = self._should_pause(entity_id)
            self._condition.notify_all()
        if paused:
            handle.request_pause()
        return handle

    async def unregister(
        self,
        entity_id: str,
        handle: ActiveShellCommandHandle,
    ) -> None:
        async with self._condition:
            handles = self._processes.get(entity_id)
            if handles is None:
                return
            handles.pop(id(handle), None)
            if not handles:
                self._processes.pop(entity_id, None)
            self._condition.notify_all()

    async def terminate_for_entity(self, entity_id: str) -> None:
        async with self._condition:
            handles = list(self._processes.get(entity_id, {}).values())
        for handle in handles:
            handle.request_pause()

    async def terminate_for_entities(self, entity_ids: set[str]) -> None:
        async with self._condition:
            handles = [
                handle
                for entity_id in entity_ids
                for handle in self._processes.get(entity_id, {}).values()
            ]
        for handle in handles:
            handle.request_pause()

    async def active_entity_ids(self) -> set[str]:
        async with self._condition:
            return set(self._processes)

    def active_command_for_entity(self, entity_id: str) -> dict[str, object] | None:
        handles = list(self._processes.get(entity_id, {}).values())
        if not handles:
            return None
        handle = min(handles, key=lambda item: item.started_at)
        return {
            "command": handle.command,
            "attempt": handle.attempt,
            "started_at": handle.started_at,
        }

    async def wait_until_idle(self) -> None:
        async with self._condition:
            while self._processes:
                await self._condition.wait()


def terminate_process_group(pid: int | None) -> None:
    if pid is None:
        return
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGTERM)
