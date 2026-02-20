from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

FAILED_STATE = "_failed"
GROUP_PATTERN = re.compile(r"^(\d+)-")


class WorkflowError(RuntimeError):
    """Raised when a workflow definition is invalid."""


@dataclass(frozen=True)
class HookConfig:
    cmd: str


@dataclass(frozen=True)
class TransitionConfig:
    source: str
    destination: str
    cmd: str | None = None
    jump: str | None = None


@dataclass(frozen=True)
class PhaseConfig:
    name: str
    states: tuple[str, ...]
    transitions: tuple[TransitionConfig, ...]
    completions: tuple[HookConfig, ...]


@dataclass(frozen=True)
class WorkflowConfig:
    phases: tuple[PhaseConfig, ...]
    environment: dict[str, str]
    retries: int

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


class RuntimeStateStore:
    def __init__(self, root: Path, state_file_name: str) -> None:
        self._state_path = root / state_file_name

    def load_current_phase(self) -> str | None:
        if not self._state_path.exists():
            return None
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise WorkflowError(f"Unable to read state file {self._state_path}: {exc}") from exc
        current = payload.get("current_phase")
        if current is None:
            return None
        if not isinstance(current, str):
            raise WorkflowError(f"Invalid state file {self._state_path}: 'current_phase' must be a string")
        return current

    def save_current_phase(self, current_phase: str) -> None:
        payload = {"current_phase": current_phase}
        self._state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class HookRunner:
    def __init__(self, root: Path, base_env: dict[str, str], retries: int, logger: logging.Logger) -> None:
        self._root = root
        self._base_env = base_env
        self._retries = retries
        self._logger = logger

    async def run(self, hook: HookConfig, extra_env: dict[str, str], context: str) -> bool:
        attempts = self._retries + 1
        env = self._base_env | extra_env
        for attempt in range(1, attempts + 1):
            process = await asyncio.create_subprocess_shell(
                hook.cmd,
                cwd=str(self._root),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await process.communicate()
            stdout = stdout_bytes.decode(errors="replace").strip()
            stderr = stderr_bytes.decode(errors="replace").strip()
            if process.returncode == 0:
                if stdout:
                    self._logger.debug("%s stdout:\n%s", context, stdout)
                if stderr:
                    self._logger.debug("%s stderr:\n%s", context, stderr)
                return True
            self._logger.warning(
                "%s failed (attempt %d/%d, exit=%s)",
                context,
                attempt,
                attempts,
                process.returncode,
            )
            if stdout:
                self._logger.warning("%s stdout:\n%s", context, stdout)
            if stderr:
                self._logger.warning("%s stderr:\n%s", context, stderr)
        return False


class DirectoryWorkflowEngine:
    def __init__(
        self,
        config: WorkflowConfig,
        root: Path,
        state: RuntimeStateStore,
        hook_runner: HookRunner,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._root = root
        self._state = state
        self._hook_runner = hook_runner
        self._logger = logger
        self._phases = {phase.name: phase for phase in config.phases}
        self._phase_state_dirs = self._build_phase_dirs(config)

    async def run(self) -> None:
        self._ensure_layout()
        phase_order = self._config.phase_order
        first_phase = phase_order[0]
        current_phase = self._state.load_current_phase()
        if current_phase is None:
            current_index = 0
            self._state.save_current_phase(phase_order[current_index])
        else:
            if current_phase not in self._phases:
                raise WorkflowError(
                    f"State file references unknown phase '{current_phase}'. "
                    f"Known phases: {', '.join(phase_order)}"
                )
            current_index = phase_order.index(current_phase)

        wrapped_to_first = False
        while True:
            phase_name = phase_order[current_index]
            self._state.save_current_phase(phase_name)
            moved = await self._run_phase(phase_name)
            if wrapped_to_first and phase_name == first_phase and moved == 0:
                self._logger.info("Reached stable fixpoint at first phase '%s'; exiting", first_phase)
                return
            current_index = (current_index + 1) % len(phase_order)
            if current_index == 0:
                wrapped_to_first = True

    async def _run_phase(self, phase_name: str) -> int:
        phase = self._phases[phase_name]
        self._logger.info("Processing phase '%s'", phase_name)
        moved_total = 0
        while True:
            moved_this_pass = 0
            for transition in phase.transitions:
                moved, jumps = await self._apply_transition(phase, transition)
                moved_this_pass += moved
                moved_total += moved
                for jump_name in jumps:
                    await self._run_jump(jump_name, phase_name)
            if moved_this_pass == 0:
                break
        await self._run_completions(phase)
        self._logger.info("Phase '%s' reached fixpoint; transitions=%d", phase_name, moved_total)
        return moved_total

    async def _run_jump(self, target_phase: str, source_phase: str) -> None:
        if target_phase == source_phase:
            self._logger.warning("Ignoring self-jump from phase '%s'", source_phase)
            return
        self._logger.info("Jumping from phase '%s' to phase '%s'", source_phase, target_phase)
        self._state.save_current_phase(target_phase)
        await self._run_phase(target_phase)
        self._state.save_current_phase(source_phase)
        self._logger.info("Returning to phase '%s' from jump phase '%s'", source_phase, target_phase)

    async def _run_completions(self, phase: PhaseConfig) -> None:
        for index, hook in enumerate(phase.completions, start=1):
            context = f"completion hook {phase.name}[{index}]"
            self._logger.info("Running %s", context)
            success = await self._hook_runner.run(hook, {}, context)
            if not success:
                raise WorkflowError(f"{context} failed after retries")

    async def _apply_transition(
        self,
        phase: PhaseConfig,
        transition: TransitionConfig,
    ) -> tuple[int, list[str]]:
        source_dir = self._phase_state_dirs[(phase.name, transition.source)]
        entities = self._list_entities(source_dir)
        if not entities:
            return 0, []

        moved = 0
        jumps: list[str] = []
        for group in self._group_entities(entities):
            results = await self._process_group(phase, transition, group)
            for result in results:
                if result.moved:
                    moved += 1
                    if result.jump is not None:
                        jumps.append(result.jump)
        return moved, jumps

    async def _process_group(
        self,
        phase: PhaseConfig,
        transition: TransitionConfig,
        group: Group,
    ) -> list[TransitionResult]:
        if group.concurrent:
            self._logger.info(
                "Running transition %s.%s -> %s for %d concurrent entities (group=%s)",
                phase.name,
                transition.source,
                transition.destination,
                len(group.entities),
                group.key,
            )
            tasks = [
                self._process_entity(phase, transition, entity)
                for entity in group.entities
            ]
            return list(await asyncio.gather(*tasks))
        results: list[TransitionResult] = []
        for entity in group.entities:
            results.append(await self._process_entity(phase, transition, entity))
        return results

    async def _process_entity(
        self,
        phase: PhaseConfig,
        transition: TransitionConfig,
        entity: Path,
    ) -> TransitionResult:
        if not entity.exists():
            return TransitionResult(moved=False, jump=None)

        destination_dir = self._phase_state_dirs[(phase.name, transition.destination)]
        failed_dir = self._phase_state_dirs[(phase.name, FAILED_STATE)]
        context = (
            f"transition hook {phase.name}:{transition.source}->{transition.destination} "
            f"entity={entity.name}"
        )
        extra_env = {"INPUT_ENTITY": str(entity.resolve())}
        if transition.cmd is not None:
            success = await self._hook_runner.run(HookConfig(transition.cmd), extra_env, context)
        else:
            success = True

        if success:
            await self._move_entity(entity, destination_dir / entity.name)
            self._logger.info(
                "Moved entity '%s' to %s/%s",
                entity.name,
                phase.name,
                transition.destination,
            )
            return TransitionResult(moved=True, jump=transition.jump)

        await self._move_entity(entity, failed_dir / entity.name)
        self._logger.error(
            "Transition failed for '%s'; moved to %s/%s",
            entity.name,
            phase.name,
            FAILED_STATE,
        )
        return TransitionResult(moved=False, jump=None)

    async def _move_entity(self, source: Path, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.move, str(source), str(destination))

    def _ensure_layout(self) -> None:
        for directory in self._phase_state_dirs.values():
            directory.mkdir(parents=True, exist_ok=True)

    def _list_entities(self, source_dir: Path) -> list[Path]:
        entities = [path for path in source_dir.iterdir() if path.is_file()]
        return sorted(entities, key=lambda path: path.name)

    def _group_entities(self, entities: list[Path]) -> list[Group]:
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

    def _build_phase_dirs(self, config: WorkflowConfig) -> dict[tuple[str, str], Path]:
        directories: dict[tuple[str, str], Path] = {}
        for phase in config.phases:
            for state in phase.states:
                directories[(phase.name, state)] = self._root / phase.name / state
            directories[(phase.name, FAILED_STATE)] = self._root / phase.name / FAILED_STATE
        return directories


def parse_args() -> CliOptions:
    parser = argparse.ArgumentParser(description="Run directory-based workflow orchestration")
    parser.add_argument("workflow", type=Path, help="Path to workflow YAML file")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root directory for workflow state directories (default: current directory)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Retries for hooks (overrides YAML retries; retries count excludes first attempt)",
    )
    parser.add_argument(
        "--state-file",
        default=".dirorch_runtime.json",
        help="Runtime state file name under --root",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity",
    )
    args = parser.parse_args()
    if args.retries is not None and args.retries < 0:
        raise SystemExit("--retries must be 0 or greater")
    return CliOptions(
        workflow=args.workflow,
        root=args.root,
        retries_override=args.retries,
        state_file=args.state_file,
        log_level=args.log_level,
    )


def configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("dirorch")


def load_workflow(path: Path) -> WorkflowConfig:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise WorkflowError(f"Failed to read workflow file {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise WorkflowError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise WorkflowError("Workflow YAML root must be a mapping")

    raw_phases = payload.get("phases")
    if not isinstance(raw_phases, dict) or not raw_phases:
        raise WorkflowError("Workflow must include non-empty 'phases' mapping")

    environment = _load_environment(payload)
    retries = _load_retries(payload)
    phases = _parse_phases(raw_phases)
    _validate_workflow(phases)
    return WorkflowConfig(phases=phases, environment=environment, retries=retries)


def _load_environment(payload: dict[str, Any]) -> dict[str, str]:
    raw = payload.get("env", payload.get("environment", {}))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise WorkflowError("'env' or 'environment' must be a mapping of string keys and values")
    environment: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            raise WorkflowError("Environment variable names must be strings")
        if not isinstance(value, str):
            raise WorkflowError(f"Environment variable '{key}' value must be a string")
        environment[key] = value
    return environment


def _load_retries(payload: dict[str, Any]) -> int:
    retries = payload.get("retries", 3)
    if not isinstance(retries, int) or retries < 0:
        raise WorkflowError("'retries' must be a non-negative integer")
    return retries


def _parse_phases(raw_phases: dict[str, Any]) -> tuple[PhaseConfig, ...]:
    phases: list[PhaseConfig] = []
    for phase_name, raw_phase in raw_phases.items():
        if not isinstance(phase_name, str) or not phase_name:
            raise WorkflowError("Phase names must be non-empty strings")
        if not isinstance(raw_phase, dict):
            raise WorkflowError(f"Phase '{phase_name}' must be a mapping")

        states = _parse_states(phase_name, raw_phase)
        transitions = _parse_transitions(phase_name, raw_phase)
        completions = _parse_completions(phase_name, raw_phase)
        phases.append(
            PhaseConfig(
                name=phase_name,
                states=states,
                transitions=transitions,
                completions=completions,
            )
        )
    return tuple(phases)


def _parse_states(phase_name: str, raw_phase: dict[str, Any]) -> tuple[str, ...]:
    raw_states = raw_phase.get("states")
    if not isinstance(raw_states, list) or not raw_states:
        raise WorkflowError(f"Phase '{phase_name}' must include non-empty 'states' list")
    states: list[str] = []
    for state in raw_states:
        if not isinstance(state, str) or not state:
            raise WorkflowError(f"Phase '{phase_name}' contains invalid state name")
        if state == FAILED_STATE:
            raise WorkflowError(
                f"Phase '{phase_name}' cannot include reserved state '{FAILED_STATE}' in 'states'"
            )
        if state in states:
            raise WorkflowError(f"Phase '{phase_name}' has duplicate state '{state}'")
        states.append(state)
    return tuple(states)


def _parse_transitions(phase_name: str, raw_phase: dict[str, Any]) -> tuple[TransitionConfig, ...]:
    raw_transitions = raw_phase.get("transitions", [])
    if raw_transitions is None:
        raw_transitions = []
    if not isinstance(raw_transitions, list):
        raise WorkflowError(f"Phase '{phase_name}' field 'transitions' must be a list")

    transitions: list[TransitionConfig] = []
    for item in raw_transitions:
        if not isinstance(item, dict):
            raise WorkflowError(f"Phase '{phase_name}' transition entries must be mappings")
        source = item.get("from")
        destination = item.get("to")
        cmd = item.get("cmd")
        jump = item.get("jump")
        if not isinstance(source, str) or not source:
            raise WorkflowError(f"Phase '{phase_name}' transition is missing valid 'from'")
        if not isinstance(destination, str) or not destination:
            raise WorkflowError(f"Phase '{phase_name}' transition is missing valid 'to'")
        if cmd is not None and (not isinstance(cmd, str) or not cmd.strip()):
            raise WorkflowError(f"Phase '{phase_name}' transition '{source}->{destination}' has invalid 'cmd'")
        if jump is not None and (not isinstance(jump, str) or not jump):
            raise WorkflowError(f"Phase '{phase_name}' transition '{source}->{destination}' has invalid 'jump'")
        transitions.append(
            TransitionConfig(source=source, destination=destination, cmd=cmd, jump=jump)
        )
    return tuple(transitions)


def _parse_completions(phase_name: str, raw_phase: dict[str, Any]) -> tuple[HookConfig, ...]:
    raw_completions = raw_phase.get("completions", raw_phase.get("completion", []))
    if raw_completions is None:
        raw_completions = []
    if not isinstance(raw_completions, list):
        raise WorkflowError(f"Phase '{phase_name}' field 'completions' must be a list")

    completions: list[HookConfig] = []
    for item in raw_completions:
        if isinstance(item, str):
            cmd = item
        elif isinstance(item, dict):
            cmd = item.get("cmd")
        else:
            raise WorkflowError(f"Phase '{phase_name}' completion entries must be strings or mappings")
        if not isinstance(cmd, str) or not cmd.strip():
            raise WorkflowError(f"Phase '{phase_name}' completion hook has invalid 'cmd'")
        completions.append(HookConfig(cmd=cmd))
    return tuple(completions)


def _validate_workflow(phases: tuple[PhaseConfig, ...]) -> None:
    phase_names = {phase.name for phase in phases}
    for phase in phases:
        states = set(phase.states)
        for transition in phase.transitions:
            if transition.source not in states:
                raise WorkflowError(
                    f"Phase '{phase.name}' transition source '{transition.source}' is not a phase state"
                )
            if transition.destination not in states:
                raise WorkflowError(
                    f"Phase '{phase.name}' transition destination '{transition.destination}' is not a phase state"
                )
            if transition.jump is not None and transition.jump not in phase_names:
                raise WorkflowError(
                    f"Phase '{phase.name}' transition jump target '{transition.jump}' is undefined"
                )


def build_hook_env(config: WorkflowConfig, root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(config.environment)
    for phase in config.phases:
        for state in phase.states:
            key = f"DIR_{_sanitize_token(phase.name)}_{_sanitize_token(state)}"
            env[key] = str((root / phase.name / state).resolve())
    return env


def _sanitize_token(raw: str) -> str:
    upper = raw.upper()
    return re.sub(r"[^A-Z0-9]", "_", upper)


def _group_key(name: str) -> str | None:
    match = GROUP_PATTERN.match(name)
    if match is None:
        return None
    return match.group(1)


async def run(options: CliOptions) -> None:
    logger = configure_logging(options.log_level)
    config = load_workflow(options.workflow)
    retries = options.retries_override if options.retries_override is not None else config.retries
    base_env = build_hook_env(config, options.root)
    state = RuntimeStateStore(options.root, options.state_file)
    hook_runner = HookRunner(options.root, base_env, retries, logger)
    engine = DirectoryWorkflowEngine(config, options.root, state, hook_runner, logger)
    await engine.run()


def main() -> None:
    options = parse_args()
    try:
        asyncio.run(run(options))
    except WorkflowError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
