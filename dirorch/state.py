from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .errors import WorkflowError

STATE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class JumpFrame:
    source_phase: str
    target_phase: str
    source_entity_name: str | None = None


@dataclass(frozen=True)
class EntityCursor:
    phase: str
    entity_name: str


@dataclass(frozen=True)
class RuntimeSnapshot:
    schema_version: int
    current_phase: str
    jump_stack: tuple[JumpFrame, ...]
    entity_cursor: EntityCursor | None


class RuntimeStateStore:
    """Persists runtime execution context so runs can resume safely."""

    def __init__(self, root: Path, state_file_name: str) -> None:
        self._state_path = root / state_file_name

    def load_snapshot(self) -> RuntimeSnapshot | None:
        if not self._state_path.exists():
            return None
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise WorkflowError(
                f"Unable to read state file {self._state_path}: {exc}"
            ) from exc

        return _parse_snapshot(self._state_path, payload)

    def save_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        payload = {
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
            "entity_cursor": (
                None
                if snapshot.entity_cursor is None
                else {
                    "phase": snapshot.entity_cursor.phase,
                    "entity_name": snapshot.entity_cursor.entity_name,
                }
            ),
        }
        self._state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_snapshot(state_path: Path, payload: object) -> RuntimeSnapshot:
    if not isinstance(payload, dict):
        raise WorkflowError(
            f"Invalid state file {state_path}: expected object payload"
        )

    expected_fields = {"schema_version", "current_phase", "jump_stack", "entity_cursor"}
    payload_fields = set(payload.keys())
    if payload_fields != expected_fields:
        raise WorkflowError(
            f"Invalid state file {state_path}: expected fields "
            f"{sorted(expected_fields)}, got {sorted(payload_fields)}"
        )

    schema_version = payload.get("schema_version")
    if schema_version != STATE_SCHEMA_VERSION:
        raise WorkflowError(
            f"Invalid state file {state_path}: unsupported schema_version "
            f"'{schema_version}', expected {STATE_SCHEMA_VERSION}"
        )

    current_phase = payload.get("current_phase")
    if not isinstance(current_phase, str) or not current_phase:
        raise WorkflowError(
            f"Invalid state file {state_path}: 'current_phase' must be a non-empty string"
        )

    jump_stack_raw = payload.get("jump_stack")
    if not isinstance(jump_stack_raw, list):
        raise WorkflowError(
            f"Invalid state file {state_path}: 'jump_stack' must be a list"
        )

    jump_stack: list[JumpFrame] = []
    for index, item in enumerate(jump_stack_raw):
        if not isinstance(item, dict):
            raise WorkflowError(
                f"Invalid state file {state_path}: jump_stack[{index}] must be an object"
            )
        frame_fields = {"source_phase", "target_phase", "source_entity_name"}
        if set(item.keys()) != frame_fields:
            raise WorkflowError(
                f"Invalid state file {state_path}: jump_stack[{index}] must contain "
                f"fields {sorted(frame_fields)}"
            )
        source_phase = item.get("source_phase")
        target_phase = item.get("target_phase")
        source_entity_name = item.get("source_entity_name")
        if not isinstance(source_phase, str) or not source_phase:
            raise WorkflowError(
                f"Invalid state file {state_path}: jump_stack[{index}].source_phase "
                "must be a non-empty string"
            )
        if not isinstance(target_phase, str) or not target_phase:
            raise WorkflowError(
                f"Invalid state file {state_path}: jump_stack[{index}].target_phase "
                "must be a non-empty string"
            )
        if source_entity_name is not None and (
            not isinstance(source_entity_name, str) or not source_entity_name
        ):
            raise WorkflowError(
                f"Invalid state file {state_path}: jump_stack[{index}].source_entity_name "
                "must be null or a non-empty string"
            )
        jump_stack.append(
            JumpFrame(
                source_phase=source_phase,
                target_phase=target_phase,
                source_entity_name=source_entity_name,
            )
        )

    entity_cursor_raw = payload.get("entity_cursor")
    entity_cursor: EntityCursor | None
    if entity_cursor_raw is None:
        entity_cursor = None
    elif isinstance(entity_cursor_raw, dict):
        cursor_fields = {"phase", "entity_name"}
        if set(entity_cursor_raw.keys()) != cursor_fields:
            raise WorkflowError(
                f"Invalid state file {state_path}: 'entity_cursor' must contain "
                f"fields {sorted(cursor_fields)}"
            )
        phase = entity_cursor_raw.get("phase")
        entity_name = entity_cursor_raw.get("entity_name")
        if not isinstance(phase, str) or not phase:
            raise WorkflowError(
                f"Invalid state file {state_path}: entity_cursor.phase must be a "
                "non-empty string"
            )
        if not isinstance(entity_name, str) or not entity_name:
            raise WorkflowError(
                f"Invalid state file {state_path}: entity_cursor.entity_name must be "
                "a non-empty string"
            )
        entity_cursor = EntityCursor(phase=phase, entity_name=entity_name)
    else:
        raise WorkflowError(
            f"Invalid state file {state_path}: 'entity_cursor' must be null or an object"
        )

    return RuntimeSnapshot(
        schema_version=STATE_SCHEMA_VERSION,
        current_phase=current_phase,
        jump_stack=tuple(jump_stack),
        entity_cursor=entity_cursor,
    )
