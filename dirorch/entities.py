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

    def _list_entities(self, source_dir: Path) -> list[Path]:
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
