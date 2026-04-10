from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionActivity:
    kind: str | None
    phase: str | None
    phase_mode: str | None
    source_state: str | None
    destination_state: str | None
    entity_ids: tuple[str, ...]
    details: str | None


class NullExecutionObserver:
    """No-op observer used when runtime activity tracking is disabled."""

    def phase_started(self, phase_name: str, mode: str) -> None:
        return None

    def phase_completed(self, phase_name: str) -> None:
        return None

    def init_started(self) -> None:
        return None

    def init_finished(self) -> None:
        return None

    def completion_started(self, phase_name: str, hook_index: int) -> None:
        return None

    def completion_finished(self, phase_name: str, hook_index: int) -> None:
        return None

    def transition_started(
        self,
        phase_name: str,
        phase_mode: str,
        source_state: str,
        destination_state: str | None,
        entity_ids: tuple[str, ...],
    ) -> None:
        return None

    def transition_finished(
        self,
        phase_name: str,
        source_state: str,
        destination_state: str | None,
        entity_ids: tuple[str, ...],
    ) -> None:
        return None

    def jump_started(self, source_phase: str, target_phase: str) -> None:
        return None

    def jump_finished(self, source_phase: str, target_phase: str) -> None:
        return None


class ExecutionStatusTracker(NullExecutionObserver):
    """Tracks workflow runner and current execution activity for status endpoints."""

    def __init__(self) -> None:
        self._runner_state = "idle"
        self._current_phase: str | None = None
        self._current_phase_mode: str | None = None
        self._activity = ExecutionActivity(
            kind=None,
            phase=None,
            phase_mode=None,
            source_state=None,
            destination_state=None,
            entity_ids=(),
            details=None,
        )
        self._jump_stack: list[tuple[str, str]] = []
        self._last_error: str | None = None

    def runner_started(self) -> None:
        self._runner_state = "running"
        self._last_error = None

    def runner_waiting(self) -> None:
        if self._runner_state != "failed":
            self._runner_state = "idle"
        self._clear_activity()

    def runner_stopped(self) -> None:
        if self._runner_state != "failed":
            self._runner_state = "stopped"
        self._clear_activity()

    def runner_failed(self, message: str) -> None:
        self._runner_state = "failed"
        self._last_error = message
        self._clear_activity()

    def phase_started(self, phase_name: str, mode: str) -> None:
        self._current_phase = phase_name
        self._current_phase_mode = mode

    def phase_completed(self, phase_name: str) -> None:
        if self._current_phase == phase_name:
            self._clear_activity()

    def init_started(self) -> None:
        self._activity = ExecutionActivity(
            kind="init",
            phase=None,
            phase_mode=None,
            source_state=None,
            destination_state=None,
            entity_ids=(),
            details="init",
        )

    def init_finished(self) -> None:
        self._clear_activity()

    def completion_started(self, phase_name: str, hook_index: int) -> None:
        self._activity = ExecutionActivity(
            kind="completion",
            phase=phase_name,
            phase_mode=self._current_phase_mode,
            source_state=None,
            destination_state=None,
            entity_ids=(),
            details=f"completion[{hook_index}]",
        )

    def completion_finished(self, phase_name: str, hook_index: int) -> None:
        if self._activity.kind == "completion" and self._activity.phase == phase_name:
            self._clear_activity()

    def transition_started(
        self,
        phase_name: str,
        phase_mode: str,
        source_state: str,
        destination_state: str | None,
        entity_ids: tuple[str, ...],
    ) -> None:
        self._activity = ExecutionActivity(
            kind="transition",
            phase=phase_name,
            phase_mode=phase_mode,
            source_state=source_state,
            destination_state=destination_state,
            entity_ids=entity_ids,
            details=None,
        )

    def transition_finished(
        self,
        phase_name: str,
        source_state: str,
        destination_state: str | None,
        entity_ids: tuple[str, ...],
    ) -> None:
        activity = self._activity
        if (
            activity.kind == "transition"
            and activity.phase == phase_name
            and activity.source_state == source_state
            and activity.destination_state == destination_state
            and activity.entity_ids == entity_ids
        ):
            self._clear_activity()

    def jump_started(self, source_phase: str, target_phase: str) -> None:
        self._jump_stack.append((source_phase, target_phase))

    def jump_finished(self, source_phase: str, target_phase: str) -> None:
        if self._jump_stack and self._jump_stack[-1] == (source_phase, target_phase):
            self._jump_stack.pop()

    def is_processing(self, entity_id: str) -> bool:
        return entity_id in self._activity.entity_ids

    def snapshot(self) -> dict[str, object]:
        return {
            "runner_state": self._runner_state,
            "current_phase": self._current_phase,
            "current_phase_mode": self._current_phase_mode,
            "activity": {
                "kind": self._activity.kind,
                "phase": self._activity.phase,
                "phase_mode": self._activity.phase_mode,
                "source_state": self._activity.source_state,
                "destination_state": self._activity.destination_state,
                "entity_ids": list(self._activity.entity_ids),
                "details": self._activity.details,
            },
            "jump_stack": [
                {"source_phase": source, "target_phase": target}
                for source, target in self._jump_stack
            ],
            "last_error": self._last_error,
        }

    def _clear_activity(self) -> None:
        self._activity = ExecutionActivity(
            kind=None,
            phase=self._current_phase,
            phase_mode=self._current_phase_mode,
            source_state=None,
            destination_state=None,
            entity_ids=(),
            details=None,
        )
