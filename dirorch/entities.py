from __future__ import annotations

import asyncio
import re
import shutil
from pathlib import Path

from .constants import FAILED_STATE
from .models import Group, PhaseConfig, WorkflowConfig

GROUP_PATTERN = re.compile(r"^(\d+)-")


class EntityStore:
    """Owns phase/state directories and entity file movement."""

    def __init__(self, root: Path, config: WorkflowConfig) -> None:
        self._root = root
        self._config = config
        self._phase_state_dirs = self._build_phase_dirs(config)

    def ensure_layout(self) -> None:
        for directory in self._phase_state_dirs.values():
            directory.mkdir(parents=True, exist_ok=True)

    def dir_for(self, phase_name: str, state_name: str) -> Path:
        return self._phase_state_dirs[(phase_name, state_name)]

    async def move_to_state(
        self, phase_name: str, state_name: str, entity: Path
    ) -> None:
        destination = self.dir_for(phase_name, state_name) / entity.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.move, str(entity), str(destination))

    def list_transition_entities(
        self, phase_name: str, source_state: str
    ) -> list[Path]:
        source_dir = self.dir_for(phase_name, source_state)
        return self._list_entities(source_dir)

    def list_phase_entities(self, phase: PhaseConfig) -> list[Path]:
        entities: list[Path] = []
        for state in phase.states:
            entities.extend(self._list_entities(self.dir_for(phase.name, state)))
        return sorted(entities, key=lambda path: (path.name, str(path.parent)))

    def entity_layout(self) -> tuple[str, ...]:
        layout: list[str] = []
        for phase_state, directory in sorted(self._phase_state_dirs.items()):
            phase_name, state_name = phase_state
            for entity in self._list_entities(directory):
                layout.append(f"{phase_name}/{state_name}/{entity.name}")
        return tuple(layout)

    def group_entities(self, entities: list[Path]) -> list[Group]:
        groups: list[Group] = []
        pending: list[Path] = []
        pending_key: str | None = None

        for entity in entities:
            key = _group_key(entity.name)
            if not pending:
                pending = [entity]
                pending_key = key
                continue
            if key is not None and key == pending_key:
                pending.append(entity)
                continue
            groups.append(Group(tuple(pending), pending_key))
            pending = [entity]
            pending_key = key

        if pending:
            groups.append(Group(tuple(pending), pending_key))
        return groups

    def list_all_entities(self) -> list[Path]:
        entities: list[Path] = []
        for directory in self._phase_state_dirs.values():
            entities.extend(self._list_entities(directory))
        return sorted(entities, key=lambda path: (path.name, str(path.parent)))

    def count_entities_in_state(self, state_name: str) -> int:
        return sum(
            len(self._list_entities(directory))
            for (_phase_name, candidate_state), directory in self._phase_state_dirs.items()
            if candidate_state == state_name
        )

    def locate_entities(self, entity_id: str) -> list[Path]:
        matches = [
            entity for entity in self.list_all_entities() if entity.name == entity_id
        ]
        return matches

    def phase_state_for(self, entity: Path) -> tuple[str, str]:
        for (phase_name, state_name), directory in self._phase_state_dirs.items():
            if entity.parent == directory:
                return phase_name, state_name
        raise ValueError(f"Entity {entity} is not under a known phase/state directory")

    def read_text(self, entity: Path) -> str:
        return entity.read_text(encoding="utf-8")

    def create(self, phase_name: str, state_name: str, entity_id: str, content: str) -> Path:
        path = self.dir_for(phase_name, state_name) / entity_id
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def update_contents(self, entity: Path, content: str) -> Path:
        entity.write_text(content, encoding="utf-8")
        return entity

    def delete(self, entity: Path) -> None:
        entity.unlink()

    def is_valid_state(self, phase_name: str, state_name: str) -> bool:
        return (phase_name, state_name) in self._phase_state_dirs

    def phase_names(self) -> tuple[str, ...]:
        return self._config.phase_order

    def _list_entities(self, source_dir: Path) -> list[Path]:
        if not source_dir.exists():
            return []
        entities = [path for path in source_dir.iterdir() if path.is_file()]
        return sorted(entities, key=lambda path: path.name)

    def _build_phase_dirs(self, config: WorkflowConfig) -> dict[tuple[str, str], Path]:
        directories: dict[tuple[str, str], Path] = {}
        for phase in config.phases:
            for state in phase.states:
                directories[(phase.name, state)] = self._root / phase.name / state
            directories[(phase.name, FAILED_STATE)] = (
                self._root / phase.name / FAILED_STATE
            )
        return directories


def _group_key(name: str) -> str | None:
    match = GROUP_PATTERN.match(name)
    if match is None:
        return None
    return match.group(1)
