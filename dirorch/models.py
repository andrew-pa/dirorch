from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .constants import PHASE_MODE_TRANSITIONS


@dataclass(frozen=True)
class HookConfig:
    cmd: str
    stdin: str | None = None


@dataclass(frozen=True)
class TransitionConfig:
    source: str
    destination: str
    cmd: str | None = None
    stdin: str | None = None
    jump: str | None = None


@dataclass(frozen=True)
class PhaseConfig:
    name: str
    states: tuple[str, ...]
    transitions: tuple[TransitionConfig, ...]
    completions: tuple[HookConfig, ...]
    mode: str = PHASE_MODE_TRANSITIONS


@dataclass(frozen=True)
class WorkflowConfig:
    phases: tuple[PhaseConfig, ...]
    environment: dict[str, str]
    retries: int
    init: HookConfig | None

    @property
    def phase_order(self) -> tuple[str, ...]:
        return tuple(phase.name for phase in self.phases)


@dataclass(frozen=True)
class CliOptions:
    workflow: Path
    root: Path
    retries_override: int | None
    state_file: str
    log_level: str


@dataclass(frozen=True)
class TransitionResult:
    moved: bool
    jump: str | None


@dataclass(frozen=True)
class Group:
    entities: tuple[Path, ...]
    key: str | None

    @property
    def concurrent(self) -> bool:
        return self.key is not None and len(self.entities) > 1
