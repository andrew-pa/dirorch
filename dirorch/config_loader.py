from __future__ import annotations

from typing import Any
from pathlib import Path

import yaml

from .constants import (
    FAILED_STATE,
    PHASE_MODE_ENTITY,
    PHASE_MODE_PARALLEL,
    PHASE_MODE_TRANSITIONS,
)
from .errors import WorkflowError
from .models import (
    HookConfig,
    NamedTargetConfig,
    PhaseConfig,
    TransitionConfig,
    WorkflowConfig,
)


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
    cwd = _parse_optional_cwd(payload, "Workflow")
    init = _parse_optional_hook(payload, "init")
    phases = _parse_phases(raw_phases)
    _validate_workflow(phases)

    return WorkflowConfig(
        phases=phases,
        environment=environment,
        retries=retries,
        init=init,
        cwd=cwd,
    )


def _load_environment(payload: dict[str, Any]) -> dict[str, str]:
    raw = payload.get("env", payload.get("environment", {}))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise WorkflowError(
            "'env' or 'environment' must be a mapping of string keys and values"
        )

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


def _parse_optional_hook(payload: dict[str, Any], field_name: str) -> HookConfig | None:
    raw_hook = payload.get(field_name)
    if raw_hook is None:
        return None

    try:
        return _parse_hook(raw_hook)
    except ValueError:
        raise WorkflowError(f"'{field_name}' must be a string or a mapping with 'cmd'")
    except WorkflowError as exc:
        raise WorkflowError(f"'{field_name}' hook {exc}") from exc


def _parse_optional_cwd(payload: dict[str, Any], label: str) -> str | None:
    cwd = payload.get("cwd")
    if cwd is None:
        return None
    if not isinstance(cwd, str) or not cwd.strip():
        raise WorkflowError(f"{label} has invalid 'cwd'")
    return cwd


def _parse_named_target(
    raw_target: Any,
    *,
    phase_name: str,
    transition_label: str,
    field_name: str,
    required: bool,
) -> NamedTargetConfig | None:
    if raw_target is None:
        if required:
            raise WorkflowError(
                f"Phase '{phase_name}' transition '{transition_label}' is missing valid '{field_name}'"
            )
        return None

    if isinstance(raw_target, str):
        if not raw_target:
            raise WorkflowError(
                f"Phase '{phase_name}' transition '{transition_label}' has invalid '{field_name}'"
            )
        return NamedTargetConfig(constant=raw_target)

    if not isinstance(raw_target, dict):
        raise WorkflowError(
            f"Phase '{phase_name}' transition '{transition_label}' has invalid '{field_name}'"
        )

    try:
        hook = _parse_hook(raw_target)
    except WorkflowError as exc:
        raise WorkflowError(
            f"Phase '{phase_name}' transition '{transition_label}' {field_name} selector {exc}"
        ) from exc
    return NamedTargetConfig(hook=hook)


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
        mode = _parse_phase_mode(phase_name, raw_phase)
        cwd = _parse_optional_cwd(raw_phase, f"Phase '{phase_name}'")

        phases.append(
            PhaseConfig(
                name=phase_name,
                states=states,
                transitions=transitions,
                completions=completions,
                mode=mode,
                cwd=cwd,
            )
        )
    return tuple(phases)


def _parse_states(phase_name: str, raw_phase: dict[str, Any]) -> tuple[str, ...]:
    raw_states = raw_phase.get("states")
    if not isinstance(raw_states, list) or not raw_states:
        raise WorkflowError(
            f"Phase '{phase_name}' must include non-empty 'states' list"
        )

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


def _parse_transitions(
    phase_name: str,
    raw_phase: dict[str, Any],
) -> tuple[TransitionConfig, ...]:
    raw_transitions = raw_phase.get("transitions", [])
    if raw_transitions is None:
        raw_transitions = []
    if not isinstance(raw_transitions, list):
        raise WorkflowError(f"Phase '{phase_name}' field 'transitions' must be a list")

    transitions: list[TransitionConfig] = []
    for item in raw_transitions:
        if not isinstance(item, dict):
            raise WorkflowError(
                f"Phase '{phase_name}' transition entries must be mappings"
            )

        source = item.get("from")
        cmd = item.get("cmd")
        stdin = item.get("stdin")
        cwd = item.get("cwd")

        if not isinstance(source, str) or not source:
            raise WorkflowError(
                f"Phase '{phase_name}' transition is missing valid 'from'"
            )
        transition_label = f"{source}->?"
        destination = _parse_named_target(
            item.get("to"),
            phase_name=phase_name,
            transition_label=transition_label,
            field_name="to",
            required=True,
        )
        assert destination is not None
        transition_label = f"{source}->{destination.display_name}"
        jump_target = _parse_named_target(
            item.get("jump"),
            phase_name=phase_name,
            transition_label=transition_label,
            field_name="jump",
            required=False,
        )
        if cmd is not None and (not isinstance(cmd, str) or not cmd.strip()):
            raise WorkflowError(
                f"Phase '{phase_name}' transition '{transition_label}' has invalid 'cmd'"
            )
        if stdin is not None and not isinstance(stdin, str):
            raise WorkflowError(
                f"Phase '{phase_name}' transition '{transition_label}' has invalid 'stdin'"
            )
        if cmd is None and stdin is not None:
            raise WorkflowError(
                f"Phase '{phase_name}' transition '{transition_label}' requires 'cmd' when 'stdin' is set"
            )
        if cwd is not None:
            if cmd is None:
                raise WorkflowError(
                    f"Phase '{phase_name}' transition '{transition_label}' requires 'cmd' when 'cwd' is set"
                )
            if not isinstance(cwd, str) or not cwd.strip():
                raise WorkflowError(
                    f"Phase '{phase_name}' transition '{transition_label}' has invalid 'cwd'"
                )

        transitions.append(
            TransitionConfig(
                source=source,
                destination=destination,
                cmd=cmd,
                stdin=stdin,
                cwd=cwd,
                jump_target=jump_target,
            )
        )
    return tuple(transitions)


def _parse_completions(
    phase_name: str,
    raw_phase: dict[str, Any],
) -> tuple[HookConfig, ...]:
    raw_completions = raw_phase.get("completions", raw_phase.get("completion", []))
    if raw_completions is None:
        raw_completions = []
    if not isinstance(raw_completions, list):
        raise WorkflowError(f"Phase '{phase_name}' field 'completions' must be a list")

    completions: list[HookConfig] = []
    for item in raw_completions:
        try:
            hook = _parse_hook(item)
        except ValueError:
            raise WorkflowError(
                f"Phase '{phase_name}' completion entries must be strings or mappings"
            )
        except WorkflowError as exc:
            raise WorkflowError(
                f"Phase '{phase_name}' completion hook {exc}"
            ) from exc
        completions.append(hook)
    return tuple(completions)


def _parse_phase_mode(phase_name: str, raw_phase: dict[str, Any]) -> str:
    raw_mode = raw_phase.get("mode", PHASE_MODE_TRANSITIONS)
    if not isinstance(raw_mode, str):
        raise WorkflowError(f"Phase '{phase_name}' field 'mode' must be a string")

    mode = raw_mode.strip().lower()
    supported_modes = {
        PHASE_MODE_TRANSITIONS,
        PHASE_MODE_PARALLEL,
        PHASE_MODE_ENTITY,
    }
    if mode not in supported_modes:
        raise WorkflowError(
            f"Phase '{phase_name}' has invalid mode '{raw_mode}'. "
            "Supported modes: "
            f"'{PHASE_MODE_TRANSITIONS}', '{PHASE_MODE_PARALLEL}', '{PHASE_MODE_ENTITY}'"
        )
    return mode


def _validate_workflow(phases: tuple[PhaseConfig, ...]) -> None:
    phase_names = {phase.name for phase in phases}
    for phase in phases:
        states = set(phase.states)
        for transition in phase.transitions:
            transition_label = f"{transition.source}->{transition.destination.display_name}"
            if transition.source not in states:
                raise WorkflowError(
                    f"Phase '{phase.name}' transition source '{transition.source}' is not a phase state"
                )
            if (
                transition.destination.constant is not None
                and transition.destination.constant not in states
            ):
                raise WorkflowError(
                    f"Phase '{phase.name}' transition destination '{transition.destination.constant}' is not a phase state"
                )
            if phase.mode == PHASE_MODE_PARALLEL and transition.jump_target is not None:
                raise WorkflowError(
                    f"Phase '{phase.name}' transition '{transition_label}' cannot define 'jump' in parallel mode"
                )
            if (
                transition.jump_target is not None
                and transition.jump_target.constant is not None
                and transition.jump_target.constant not in phase_names
            ):
                raise WorkflowError(
                    f"Phase '{phase.name}' transition jump target '{transition.jump_target.constant}' is undefined"
                )


def _parse_hook(raw_hook: Any) -> HookConfig:
    if isinstance(raw_hook, str):
        cmd = raw_hook
        stdin = None
        cwd = None
    elif isinstance(raw_hook, dict):
        cmd = raw_hook.get("cmd")
        stdin = raw_hook.get("stdin")
        cwd = raw_hook.get("cwd")
    else:
        raise ValueError("hook must be string or mapping")

    if not isinstance(cmd, str) or not cmd.strip():
        raise WorkflowError("has invalid 'cmd'")
    if stdin is not None and not isinstance(stdin, str):
        raise WorkflowError("has invalid 'stdin'")
    if cwd is not None and (not isinstance(cwd, str) or not cwd.strip()):
        raise WorkflowError("has invalid 'cwd'")
    return HookConfig(cmd=cmd, stdin=stdin, cwd=cwd)
