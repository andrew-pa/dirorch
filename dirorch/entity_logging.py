from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

from .constants import ENTITY_LOGS_DIR_NAME


EntityLogMetadataValue = str | int | bool | None


@dataclass(frozen=True)
class EntityLogEvent:
    entity_id: str
    timestamp: datetime
    kind: str
    phase: str | None = None
    source_state: str | None = None
    destination_state: str | None = None
    command: str | None = None
    attempt: int | None = None
    returncode: int | None = None
    stream: str | None = None
    text: str | None = None
    metadata: dict[str, EntityLogMetadataValue] = field(default_factory=dict)


class EntityLogEmitter(Protocol):
    async def emit(self, event: EntityLogEvent) -> None: ...


class EntityLogSink(Protocol):
    async def emit(self, event: EntityLogEvent) -> None: ...


class NullEntityLogEmitter:
    async def emit(self, event: EntityLogEvent) -> None:
        return None


class EntityLogRouter:
    def __init__(self, sinks: tuple[EntityLogSink, ...] = ()) -> None:
        self._sinks = sinks

    async def emit(self, event: EntityLogEvent) -> None:
        for sink in self._sinks:
            await sink.emit(event)


@dataclass(frozen=True)
class RenderedLogChunk:
    entity_id: str
    text: str
    offset_start: int
    offset_end: int
    timestamp: datetime


@dataclass(frozen=True)
class EntityTranscriptReadResult:
    entity_id: str
    text: str
    offset: int
    next_offset: int
    exists: bool
    modified_at: datetime | None = None


@dataclass(frozen=True)
class EntityTranscriptMetadata:
    entity_id: str
    exists: bool
    size_bytes: int
    modified_at: datetime | None


class EntityTranscriptFormatter:
    def render(self, event: EntityLogEvent) -> str:
        if event.kind == "command.stream":
            return event.text or ""

        timestamp = _format_timestamp(event.timestamp)
        line = self._format_line(event)
        if line is None:
            return ""
        return f"[{timestamp}] {line}\n"

    def _format_line(self, event: EntityLogEvent) -> str | None:
        if event.kind == "entity.created":
            return (
                "entity created "
                f"phase={_metadata_text(event.metadata, 'phase')} "
                f"state={_metadata_text(event.metadata, 'state')}"
            )
        if event.kind == "entity.updated":
            changed = event.metadata.get("changed")
            if isinstance(changed, str) and changed:
                return f"entity updated fields={changed}"
            return "entity updated"
        if event.kind == "entity.locked":
            return "entity locked"
        if event.kind == "entity.unlocked":
            return "entity unlocked"
        if event.kind == "entity.deleted":
            return "entity deleted"
        if event.kind == "entity.moved":
            return (
                "manually moved "
                f"{_metadata_text(event.metadata, 'from_phase')}/"
                f"{_metadata_text(event.metadata, 'from_state')} -> "
                f"{_metadata_text(event.metadata, 'to_phase')}/"
                f"{_metadata_text(event.metadata, 'to_state')}"
            )
        if event.kind == "transition.started":
            return (
                "transition started "
                f"{event.phase}:{event.source_state} -> "
                f"{_metadata_text(event.metadata, 'configured_destination')}"
            )
        if event.kind == "transition.implicit":
            return "implicit transition; no command configured"
        if event.kind == "command.started":
            return self._format_command_started(event)
        if event.kind == "command.finished":
            return self._format_command_finished(event)
        if event.kind == "command.retrying":
            return self._format_command_retrying(event)
        if event.kind == "selector.resolved":
            selector_kind = _metadata_text(event.metadata, "selector_kind")
            value = _metadata_text(event.metadata, "selected")
            if selector_kind == "jump":
                return f"selector resolved jump={value}"
            return f"selector resolved destination={value}"
        if event.kind == "selector.empty":
            selector_kind = _metadata_text(event.metadata, "selector_kind")
            if selector_kind == "jump":
                return "selector produced no jump"
            return "selector produced no destination"
        if event.kind == "transition.failed":
            reason = event.metadata.get("reason")
            if isinstance(reason, str) and reason:
                return f'transition failed reason="{reason}"'
            return "transition failed"
        if event.kind == "transition.moved":
            destination_phase = event.metadata.get("destination_phase")
            if not isinstance(destination_phase, str) or not destination_phase:
                destination_phase = event.phase
            return f"moved to {destination_phase}/{event.destination_state}"
        if event.kind == "transition.jump":
            return f"jumping to phase {_metadata_text(event.metadata, 'target_phase')}"
        return None

    def _format_command_started(self, event: EntityLogEvent) -> str:
        prefix = self._command_prefix(event)
        parts = [f"{prefix} started"]
        if event.metadata.get("selector_kind") is not None:
            parts.append(f"kind={_metadata_text(event.metadata, 'selector_kind')}")
        if event.attempt is not None:
            parts.append(f"attempt={event.attempt}")
        if event.command is not None:
            parts.append(f'cmd="{event.command}"')
        return " ".join(parts)

    def _format_command_finished(self, event: EntityLogEvent) -> str:
        prefix = self._command_prefix(event)
        parts = [f"{prefix} finished"]
        if event.metadata.get("selector_kind") is not None:
            parts.append(f"kind={_metadata_text(event.metadata, 'selector_kind')}")
        if event.returncode is not None:
            parts.append(f"exit={event.returncode}")
        error = event.metadata.get("error")
        if isinstance(error, str) and error:
            parts.append(f'error="{error}"')
        return " ".join(parts)

    def _format_command_retrying(self, event: EntityLogEvent) -> str:
        prefix = self._command_prefix(event)
        parts = [f"retrying {prefix}"]
        if event.metadata.get("selector_kind") is not None:
            parts.append(f"kind={_metadata_text(event.metadata, 'selector_kind')}")
        parts.append(f"next_attempt={_metadata_text(event.metadata, 'next_attempt')}")
        reason = event.metadata.get("reason")
        if isinstance(reason, str) and reason:
            parts.append(f'reason="{reason}"')
        return " ".join(parts)

    def _command_prefix(self, event: EntityLogEvent) -> str:
        role = event.metadata.get("command_role")
        if isinstance(role, str) and role.startswith("transition_selector_"):
            return "selector"
        return "command"


class EntityTranscriptStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._logs_dir = root / ENTITY_LOGS_DIR_NAME
        self._locks: dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        entity_id: str,
        text: str,
        timestamp: datetime,
    ) -> RenderedLogChunk:
        entity_lock = await self._entity_lock(entity_id)
        async with entity_lock:
            return await asyncio.to_thread(self._append_sync, entity_id, text, timestamp)

    async def read(
        self,
        entity_id: str,
        offset: int = 0,
        limit_bytes: int | None = None,
    ) -> EntityTranscriptReadResult:
        entity_lock = await self._entity_lock(entity_id)
        async with entity_lock:
            return await asyncio.to_thread(self._read_sync, entity_id, offset, limit_bytes)

    async def metadata(self, entity_id: str) -> EntityTranscriptMetadata:
        entity_lock = await self._entity_lock(entity_id)
        async with entity_lock:
            return await asyncio.to_thread(self._metadata_sync, entity_id)

    async def exists(self, entity_id: str) -> bool:
        return (await self.metadata(entity_id)).exists

    def _append_sync(
        self,
        entity_id: str,
        text: str,
        timestamp: datetime,
    ) -> RenderedLogChunk:
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        path = self._path_for(entity_id)
        start = path.stat().st_size if path.exists() else 0
        with path.open("a", encoding="utf-8") as handle:
            handle.write(text)
        end = start + len(text.encode("utf-8"))
        return RenderedLogChunk(
            entity_id=entity_id,
            text=text,
            offset_start=start,
            offset_end=end,
            timestamp=timestamp,
        )

    def _read_sync(
        self,
        entity_id: str,
        offset: int,
        limit_bytes: int | None,
    ) -> EntityTranscriptReadResult:
        path = self._path_for(entity_id)
        if not path.exists():
            return EntityTranscriptReadResult(
                entity_id=entity_id,
                text="",
                offset=max(offset, 0),
                next_offset=max(offset, 0),
                exists=False,
                modified_at=None,
            )

        start = max(offset, 0)
        size = path.stat().st_size
        if start > size:
            start = size
        with path.open("rb") as handle:
            handle.seek(start)
            raw = handle.read() if limit_bytes is None else handle.read(limit_bytes)
        next_offset = start + len(raw)
        stat = path.stat()
        return EntityTranscriptReadResult(
            entity_id=entity_id,
            text=raw.decode("utf-8", errors="replace"),
            offset=start,
            next_offset=next_offset,
            exists=True,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        )

    def _metadata_sync(self, entity_id: str) -> EntityTranscriptMetadata:
        path = self._path_for(entity_id)
        if not path.exists():
            return EntityTranscriptMetadata(
                entity_id=entity_id,
                exists=False,
                size_bytes=0,
                modified_at=None,
            )
        stat = path.stat()
        return EntityTranscriptMetadata(
            entity_id=entity_id,
            exists=True,
            size_bytes=stat.st_size,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        )

    async def _entity_lock(self, entity_id: str) -> asyncio.Lock:
        async with self._lock:
            lock = self._locks.get(entity_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[entity_id] = lock
            return lock

    def _path_for(self, entity_id: str) -> Path:
        return self._logs_dir / f"{quote(entity_id, safe='')}.log"


class EntityLogBroadcaster:
    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue[RenderedLogChunk]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def publish(self, chunk: RenderedLogChunk) -> None:
        async with self._lock:
            queues = list(self._subscribers.get(chunk.entity_id, ()))
        stale: list[asyncio.Queue[RenderedLogChunk]] = []
        for queue in queues:
            try:
                queue.put_nowait(chunk)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            await self.unsubscribe(chunk.entity_id, queue)

    async def subscribe(
        self,
        entity_id: str,
        *,
        max_queue_size: int = 256,
    ) -> asyncio.Queue[RenderedLogChunk]:
        queue: asyncio.Queue[RenderedLogChunk] = asyncio.Queue(maxsize=max_queue_size)
        async with self._lock:
            self._subscribers[entity_id].add(queue)
        return queue

    async def unsubscribe(
        self,
        entity_id: str,
        queue: asyncio.Queue[RenderedLogChunk],
    ) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(entity_id)
            if subscribers is None:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(entity_id, None)


class EntityTranscriptSink:
    def __init__(
        self,
        formatter: EntityTranscriptFormatter,
        store: EntityTranscriptStore,
        broadcaster: EntityLogBroadcaster,
    ) -> None:
        self._formatter = formatter
        self._store = store
        self._broadcaster = broadcaster

    async def emit(self, event: EntityLogEvent) -> None:
        text = self._formatter.render(event)
        if not text:
            return
        chunk = await self._store.append(event.entity_id, text, event.timestamp)
        await self._broadcaster.publish(chunk)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _format_timestamp(timestamp: datetime) -> str:
    return timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _metadata_text(metadata: dict[str, EntityLogMetadataValue], key: str) -> str:
    value = metadata.get(key)
    if value is None:
        return "<unknown>"
    return str(value)
