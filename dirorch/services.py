from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from .entities import EntityStore
from .entity_logging import (
    EntityLogBroadcaster,
    EntityLogEmitter,
    EntityLogEvent,
    EntityTranscriptStore,
    RenderedLogChunk,
    NullEntityLogEmitter,
    utc_now,
)
from .execution import ExecutionStatusTracker
from .files import FileStore
from .locks import EntityLockStore
from .models import NamedTargetConfig, WorkflowConfig
from .pauses import ActiveShellCommandRegistry, EntityPauseStore, WorkflowPauseController
from .state import RuntimeStateStore
from .errors import ConflictError, NotFoundError, ValidationError


class MutationCoordinator:
    """Serializes API-originated write operations."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def mutate(self):
        async with self._lock:
            yield


class WorkflowDefinitionService:
    def __init__(self, config: WorkflowConfig, workflow_path: Path) -> None:
        self._config = config
        self._workflow_path = workflow_path

    def describe(self) -> dict[str, Any]:
        phases: list[dict[str, Any]] = []
        for phase in self._config.phases:
            phases.append(
                {
                    "name": phase.name,
                    "mode": phase.mode,
                    "cwd": phase.cwd,
                    "states": [*phase.states],
                    "reserved_states": ["_failed"],
                    "transitions": [
                        {
                            "from": transition.source,
                            "to": self._serialize_named_target(transition.destination),
                            "cmd": transition.cmd,
                            "cwd": transition.cwd,
                            "jump": None
                            if transition.jump_target is None
                            else self._serialize_named_target(transition.jump_target),
                        }
                        for transition in phase.transitions
                    ],
                    "completions": [
                        {"cmd": hook.cmd, "stdin": hook.stdin, "cwd": hook.cwd}
                        for hook in phase.completions
                    ],
                }
            )
        return {
            "workflow_file": self._workflow_file_label(),
            "phase_order": list(self._config.phase_order),
            "environment": self._config.environment,
            "retries": self._config.retries,
            "cwd": self._config.cwd,
            "init": None
            if self._config.init is None
            else {
                "cmd": self._config.init.cmd,
                "stdin": self._config.init.stdin,
                "cwd": self._config.init.cwd,
            },
            "phases": phases,
        }

    def _workflow_file_label(self) -> str:
        parent_name = self._workflow_path.parent.name
        if not parent_name:
            return self._workflow_path.name
        return f"{parent_name}/{self._workflow_path.name}"

    def _serialize_named_target(
        self,
        target: NamedTargetConfig,
    ) -> str | dict[str, str | None]:
        if target.constant is not None:
            return target.constant
        assert target.hook is not None
        return {
            "cmd": target.hook.cmd,
            "stdin": target.hook.stdin,
            "cwd": target.hook.cwd,
        }


class EntityAdminService:
    def __init__(
        self,
        entities: EntityStore,
        locks: EntityLockStore,
        pauses: EntityPauseStore,
        tracker: ExecutionStatusTracker,
        coordinator: MutationCoordinator,
        command_registry: ActiveShellCommandRegistry,
        entity_log_emitter: EntityLogEmitter = NullEntityLogEmitter(),
    ) -> None:
        self._entities = entities
        self._locks = locks
        self._pauses = pauses
        self._tracker = tracker
        self._coordinator = coordinator
        self._command_registry = command_registry
        self._entity_log_emitter = entity_log_emitter

    def list_entities(self) -> list[dict[str, Any]]:
        return [self._serialize_entity(path) for path in self._entities.list_all_entities()]

    def get_entity(self, entity_id: str) -> dict[str, Any]:
        entity = self._require_unique_entity(entity_id)
        content = self._entities.read_text(entity)
        payload = self._serialize_entity(entity)
        payload["content"] = content
        json_value = _parse_json_value(content)
        if json_value is not None:
            payload["json"] = json_value
            payload["format"] = "json"
        return payload

    async def create_entity(
        self,
        entity_id: str,
        phase_name: str,
        state_name: str,
        content: str,
        format_name: str,
    ) -> dict[str, Any]:
        self._validate_entity_payload(entity_id, phase_name, state_name, format_name, content)
        async with self._coordinator.mutate():
            if self._entities.locate_entities(entity_id):
                raise ConflictError(f"Entity '{entity_id}' already exists")
            entity = self._entities.create(phase_name, state_name, entity_id, content)
        await self._emit_entity_event(
            entity_id,
            "entity.created",
            metadata={
                "phase": phase_name,
                "state": state_name,
            },
        )
        return self.get_entity(entity.name)

    async def update_entity(
        self,
        entity_id: str,
        phase_name: str | None,
        state_name: str | None,
        content: str | None,
        format_name: str | None,
    ) -> dict[str, Any]:
        entity = self._require_unique_entity(entity_id)
        if self._tracker.is_processing(entity_id):
            raise ConflictError(f"Entity '{entity_id}' is currently being processed")

        target_phase, target_state = self._entities.phase_state_for(entity)
        if phase_name is not None:
            target_phase = phase_name
        if state_name is not None:
            target_state = state_name
        if not self._entities.is_valid_state(target_phase, target_state):
            raise ValidationError(
                f"Unknown phase/state '{target_phase}/{target_state}'"
            )
        if format_name is not None and content is None:
            raise ValidationError("content is required when format is provided")
        if content is not None:
            _validate_format_content(format_name or "text", content)

        changed_fields: list[str] = []
        async with self._coordinator.mutate():
            current = self._require_unique_entity(entity_id)
            current_phase, current_state = self._entities.phase_state_for(current)
            if content is not None:
                current = self._entities.update_contents(current, content)
            if (current_phase, current_state) != (target_phase, target_state):
                await self._entities.move_to_state(target_phase, target_state, current)
            if content is not None:
                changed_fields.append("content")
                if format_name is not None:
                    changed_fields.append("format")
        if changed_fields:
            await self._emit_entity_event(
                entity_id,
                "entity.updated",
                metadata={"changed": ",".join(changed_fields)},
            )
        if (current_phase, current_state) != (target_phase, target_state):
            await self._emit_entity_event(
                entity_id,
                "entity.moved",
                metadata={
                    "from_phase": current_phase,
                    "from_state": current_state,
                    "to_phase": target_phase,
                    "to_state": target_state,
                },
            )
        return self.get_entity(entity_id)

    async def set_locked(self, entity_id: str, locked: bool) -> dict[str, Any]:
        self._require_unique_entity(entity_id)
        if self._tracker.is_processing(entity_id):
            raise ConflictError(f"Entity '{entity_id}' is currently being processed")
        async with self._coordinator.mutate():
            self._locks.set_locked(entity_id, locked)
        await self._emit_entity_event(
            entity_id,
            "entity.locked" if locked else "entity.unlocked",
        )
        return self.get_entity(entity_id)

    async def set_paused(self, entity_id: str, paused: bool) -> dict[str, Any]:
        self._require_unique_entity(entity_id)
        async with self._coordinator.mutate():
            self._pauses.set_paused(entity_id, paused)
        if paused:
            await self._command_registry.terminate_for_entity(entity_id)
        await self._emit_entity_event(
            entity_id,
            "entity.paused" if paused else "entity.resumed",
        )
        return self.get_entity(entity_id)

    async def delete_entity(self, entity_id: str) -> None:
        entity = self._require_unique_entity(entity_id)
        if self._tracker.is_processing(entity_id):
            raise ConflictError(f"Entity '{entity_id}' is currently being processed")
        async with self._coordinator.mutate():
            current = self._require_unique_entity(entity_id)
            await self._emit_entity_event(entity_id, "entity.deleted")
            self._entities.delete(current)
            self._locks.clear(entity_id)
            self._pauses.clear(entity_id)

    def _require_unique_entity(self, entity_id: str) -> Path:
        matches = self._entities.locate_entities(entity_id)
        if not matches:
            raise NotFoundError(f"Entity '{entity_id}' was not found")
        if len(matches) > 1:
            raise ConflictError(f"Entity id '{entity_id}' is ambiguous")
        return matches[0]

    def _serialize_entity(self, entity: Path) -> dict[str, Any]:
        phase_name, state_name = self._entities.phase_state_for(entity)
        active_command = self._command_registry.active_command_for_entity(entity.name)
        return {
            "id": entity.name,
            "phase": phase_name,
            "state": state_name,
            "locked": self._locks.is_locked(entity.name),
            "paused": self._pauses.is_paused(entity.name),
            "processing": self._tracker.is_processing(entity.name),
            "active_command": active_command,
            "format": "json" if _parse_json_value(self._entities.read_text(entity)) is not None else "text",
        }

    def _validate_entity_payload(
        self,
        entity_id: str,
        phase_name: str,
        state_name: str,
        format_name: str,
        content: str,
    ) -> None:
        if not entity_id:
            raise ValidationError("Entity id must not be empty")
        if not self._entities.is_valid_state(phase_name, state_name):
            raise ValidationError(f"Unknown phase/state '{phase_name}/{state_name}'")
        _validate_format_content(format_name, content)

    async def _emit_entity_event(
        self,
        entity_id: str,
        kind: str,
        *,
        metadata: dict[str, str | int | bool | None] | None = None,
    ) -> None:
        await self._entity_log_emitter.emit(
            EntityLogEvent(
                entity_id=entity_id,
                timestamp=utc_now(),
                kind=kind,
                metadata={} if metadata is None else metadata,
            )
        )


class FileAdminService:
    def __init__(
        self,
        files: FileStore,
        coordinator: MutationCoordinator,
    ) -> None:
        self._files = files
        self._coordinator = coordinator

    def get_file(self, relative_path: str) -> dict[str, Any]:
        path, content = self._files.read(relative_path)
        payload = {
            "path": relative_path,
            "format": "text",
            "content": content,
        }
        json_value = _parse_json_value(content)
        if json_value is not None:
            payload["format"] = "json"
            payload["json"] = json_value
        return payload

    async def create_file(
        self,
        relative_path: str,
        content: str,
        format_name: str,
    ) -> dict[str, Any]:
        async with self._coordinator.mutate():
            self._files.create(relative_path, content, format_name)
        return self.get_file(relative_path)

    async def update_file(
        self,
        relative_path: str,
        content: str,
        format_name: str,
    ) -> dict[str, Any]:
        async with self._coordinator.mutate():
            self._files.update(relative_path, content, format_name)
        return self.get_file(relative_path)

    async def delete_file(self, relative_path: str) -> None:
        async with self._coordinator.mutate():
            self._files.delete(relative_path)


class WorkflowControlService:
    def __init__(
        self,
        entities: EntityAdminService,
        pauses: EntityPauseStore,
        coordinator: MutationCoordinator,
        command_registry: ActiveShellCommandRegistry,
        workflow_pause: WorkflowPauseController,
        entity_log_emitter: EntityLogEmitter = NullEntityLogEmitter(),
    ) -> None:
        self._entities = entities
        self._pauses = pauses
        self._coordinator = coordinator
        self._command_registry = command_registry
        self._workflow_pause = workflow_pause
        self._entity_log_emitter = entity_log_emitter

    async def pause(self) -> dict[str, Any]:
        await self._workflow_pause.request_pause()
        entity_ids = await self._command_registry.active_entity_ids()
        async with self._coordinator.mutate():
            for entity_id in entity_ids:
                self._pauses.set_paused(entity_id, True)
        await self._command_registry.terminate_for_entities(entity_ids)
        await self._command_registry.wait_until_idle()
        await self._workflow_pause.mark_paused()
        await asyncio.gather(
            *(
                self._emit_entity_event(entity_id, "entity.paused")
                for entity_id in entity_ids
            )
        )
        return {
            "workflow_pause_state": self._workflow_pause.state(),
            "paused_entities": len(entity_ids),
            "entities": self._entities.list_entities(),
        }

    async def resume(self) -> dict[str, Any]:
        await self._workflow_pause.resume()
        return {
            "workflow_pause_state": self._workflow_pause.state(),
            "paused_entities": len(self._pauses.list_paused()),
            "entities": self._entities.list_entities(),
        }

    async def _emit_entity_event(self, entity_id: str, kind: str) -> None:
        await self._entity_log_emitter.emit(
            EntityLogEvent(
                entity_id=entity_id,
                timestamp=utc_now(),
                kind=kind,
            )
        )


class WorkflowStatusService:
    def __init__(
        self,
        state: RuntimeStateStore,
        tracker: ExecutionStatusTracker,
        entities: EntityAdminService,
        locks: EntityLockStore,
        pauses: EntityPauseStore,
        workflow_pause: WorkflowPauseController,
        config: WorkflowConfig,
    ) -> None:
        self._state = state
        self._tracker = tracker
        self._entities = entities
        self._locks = locks
        self._pauses = pauses
        self._workflow_pause = workflow_pause
        self._config = config

    def workflow_status(self) -> dict[str, Any]:
        entities = self._entities.list_entities()
        counts: dict[str, dict[str, int]] = {}
        for phase in self._config.phases:
            state_counts = {state: 0 for state in phase.states}
            state_counts["_failed"] = 0
            counts[phase.name] = state_counts
        for entity in entities:
            counts[entity["phase"]][entity["state"]] += 1

        snapshot = self._state.load_snapshot()
        snapshot_payload: dict[str, Any] | None
        if snapshot is None:
            snapshot_payload = None
        else:
            snapshot_payload = {
                "schema_version": snapshot.schema_version,
                "current_phase": snapshot.current_phase,
                "jump_stack": [
                    {
                        "source_phase": frame.source_phase,
                        "target_phase": frame.target_phase,
                        "source_entity_name": frame.source_entity_name,
                    }
                    for frame in snapshot.jump_stack
                ],
                "entity_cursor": None
                if snapshot.entity_cursor is None
                else {
                    "phase": snapshot.entity_cursor.phase,
                    "entity_name": snapshot.entity_cursor.entity_name,
                },
            }

        return {
            "runtime_snapshot": snapshot_payload,
            "counts": counts,
            "locked_entities": len(self._locks.list_locks()),
            "paused_entities": len(self._pauses.list_paused()),
            "workflow_pause_state": self._workflow_pause.state(),
            "execution": self._tracker.snapshot(),
        }

    def entity_status(self) -> dict[str, Any]:
        return {"entities": self._entities.list_entities()}


class EntityLogService:
    def __init__(
        self,
        entities: EntityStore,
        tracker: ExecutionStatusTracker,
        store: EntityTranscriptStore,
        broadcaster: EntityLogBroadcaster,
    ) -> None:
        self._entities = entities
        self._tracker = tracker
        self._store = store
        self._broadcaster = broadcaster

    async def get_log(
        self,
        entity_id: str,
        offset: int = 0,
        limit_bytes: int | None = None,
    ) -> dict[str, Any]:
        await self._ensure_entity_or_log_exists(entity_id)
        result = await self._store.read(entity_id, offset=offset, limit_bytes=limit_bytes)
        return {
            "entity_id": entity_id,
            "text": result.text,
            "offset": result.offset,
            "next_offset": result.next_offset,
            "exists": result.exists,
            "processing": self._tracker.is_processing(entity_id),
        }

    async def log_exists(self, entity_id: str) -> bool:
        await self._ensure_entity_or_log_exists(entity_id)
        return await self._store.exists(entity_id)

    async def subscribe(self, entity_id: str) -> asyncio.Queue[RenderedLogChunk]:
        await self._ensure_entity_or_log_exists(entity_id)
        return await self._broadcaster.subscribe(entity_id)

    async def unsubscribe(
        self,
        entity_id: str,
        queue: asyncio.Queue[RenderedLogChunk],
    ) -> None:
        await self._broadcaster.unsubscribe(entity_id, queue)

    def is_processing(self, entity_id: str) -> bool:
        return self._tracker.is_processing(entity_id)

    async def _ensure_entity_or_log_exists(self, entity_id: str) -> None:
        if self._entities.locate_entities(entity_id):
            return
        if await self._store.exists(entity_id):
            return
        raise NotFoundError(f"Entity '{entity_id}' was not found")


def _validate_format_content(format_name: str, content: str) -> None:
    if format_name not in {"text", "json"}:
        raise ValidationError("format must be 'text' or 'json'")
    if format_name == "json":
        try:
            json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"Invalid JSON content: {exc.msg}") from exc


def _parse_json_value(content: str) -> Any | None:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return None
