from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .constants import (
    DEFAULT_WEB_HOST,
    DEFAULT_WEB_PORT,
    PHASE_MODE_TRANSITIONS,
)


@dataclass(frozen=True)
class HookConfig:
    cmd: str
    stdin: str | None = None
    cwd: str | None = None


@dataclass(frozen=True)
class NamedTargetConfig:
    constant: str | None = None
    hook: HookConfig | None = None

    def __post_init__(self) -> None:
        if (self.constant is None) == (self.hook is None):
            raise ValueError("NamedTargetConfig requires exactly one target source")

    @property
    def dynamic(self) -> bool:
        return self.hook is not None

    @property
    def display_name(self) -> str:
        if self.constant is not None:
            return self.constant
        return "<dynamic>"


@dataclass(frozen=True)
class TransitionConfig:
    source: str
    destination: NamedTargetConfig
    cmd: str | None = None
    stdin: str | None = None
    cwd: str | None = None
    jump_target: NamedTargetConfig | None = None


@dataclass(frozen=True)
class PhaseConfig:
    name: str
    states: tuple[str, ...]
    transitions: tuple[TransitionConfig, ...]
    completions: tuple[HookConfig, ...]
    mode: str = PHASE_MODE_TRANSITIONS
    cwd: str | None = None


@dataclass(frozen=True)
class WorkflowConfig:
    phases: tuple[PhaseConfig, ...]
    environment: dict[str, str]
    retries: int
    init: HookConfig | None
    cwd: str | None = None

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
    watch: bool = False
    web: bool = False
    web_log: bool = False
    web_host: str = DEFAULT_WEB_HOST
    web_port: int = DEFAULT_WEB_PORT


@dataclass(frozen=True)
class TransitionResult:
    moved: bool
    failed: bool
    paused: bool
    destination_phase: str | None
    destination_state: str | None
    jump_phase: str | None


@dataclass(frozen=True)
class Group:
    entities: tuple[Path, ...]
    key: str | None

    @property
    def concurrent(self) -> bool:
        return self.key is not None and len(self.entities) > 1
