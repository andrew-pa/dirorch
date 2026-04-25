from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
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


@dataclass
class ActiveShellCommandHandle:
    terminate: Callable[[], None]
    pause_requested: bool = False

    def request_pause(self) -> None:
        self.pause_requested = True
        self.terminate()


class ActiveShellCommandRegistry:
    """Tracks per-entity shell commands so pause can terminate them promptly."""

    def __init__(self, is_entity_paused: Callable[[str], bool]) -> None:
        self._is_entity_paused = is_entity_paused
        self._processes: dict[str, dict[int, ActiveShellCommandHandle]] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        entity_id: str,
        terminate: Callable[[], None],
    ) -> ActiveShellCommandHandle:
        handle = ActiveShellCommandHandle(terminate=terminate)
        async with self._lock:
            handles = self._processes.setdefault(entity_id, {})
            handles[id(handle)] = handle
            paused = self._is_entity_paused(entity_id)
        if paused:
            handle.request_pause()
        return handle

    async def unregister(
        self,
        entity_id: str,
        handle: ActiveShellCommandHandle,
    ) -> None:
        async with self._lock:
            handles = self._processes.get(entity_id)
            if handles is None:
                return
            handles.pop(id(handle), None)
            if not handles:
                self._processes.pop(entity_id, None)

    async def terminate_for_entity(self, entity_id: str) -> None:
        async with self._lock:
            handles = list(self._processes.get(entity_id, {}).values())
        for handle in handles:
            handle.request_pause()

    async def terminate_for_entities(self, entity_ids: set[str]) -> None:
        async with self._lock:
            handles = [
                handle
                for entity_id in entity_ids
                for handle in self._processes.get(entity_id, {}).values()
            ]
        for handle in handles:
            handle.request_pause()


def terminate_process_group(pid: int | None) -> None:
    if pid is None:
        return
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGTERM)
