from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path

from .constants import FAILED_STATE, PHASE_MODE_ENTITY, PHASE_MODE_TRANSITIONS
from .entities import EntityStore
from .execution import NullExecutionObserver
from .errors import WorkflowError
from .hooks import HookRunner
from .models import (
    Group,
    HookConfig,
    PhaseConfig,
    TransitionConfig,
    TransitionResult,
    WorkflowConfig,
)
from .state import (
    STATE_SCHEMA_VERSION,
    EntityCursor,
    JumpFrame,
    RuntimeSnapshot,
    RuntimeStateStore,
)

JumpHandler = Callable[[str, str], Awaitable[None]]
EntityCursorLoader = Callable[[str], str | None]
EntityCursorSaver = Callable[[str, str], None]
EntityCursorClearer = Callable[[], None]
EntityLockChecker = Callable[[str], bool]


@dataclass(frozen=True)
class PhaseProcessorDeps:
    hook_runner: HookRunner
    entities: EntityStore
    logger: logging.Logger
    jump_handler: JumpHandler
    load_entity_cursor: EntityCursorLoader
    save_entity_cursor: EntityCursorSaver
    clear_entity_cursor: EntityCursorClearer
    execution_observer: NullExecutionObserver
    is_entity_locked: EntityLockChecker


class PhaseProcessor:
    """Runs a single phase to fixpoint, independent of global phase scheduling."""

    def __init__(self, deps: PhaseProcessorDeps, config: PhaseConfig) -> None:
        self._hook_runner = deps.hook_runner
        self._entities = deps.entities
        self._logger = deps.logger
        self._jump_handler = deps.jump_handler
        self._load_entity_cursor = deps.load_entity_cursor
        self._save_entity_cursor = deps.save_entity_cursor
        self._clear_entity_cursor = deps.clear_entity_cursor
        self._execution_observer = deps.execution_observer
        self._is_entity_locked = deps.is_entity_locked
        self.config = config

    async def _run(self) -> int:
        raise NotImplementedError()

    async def run_phase(self) -> int:
        self._execution_observer.phase_started(self.config.name, self.config.mode)
        self._logger.info("Processing phase '%s' (mode: %s)", self.config.name, self.config.mode)
        try:
            moved_total = await self._run()
            await self._run_completions()
            self._logger.info(
                "Phase '%s' reached fixpoint; transitions=%d", self.config.name, moved_total
            )
            return moved_total
        finally:
            self._execution_observer.phase_completed(self.config.name)

    async def _run_completions(self) -> None:
        for index, hook in enumerate(self.config.completions, start=1):
            context = f"completion hook {self.config.name}[{index}]"
            self._logger.info("Running %s", context)
            self._execution_observer.completion_started(self.config.name, index)
            try:
                success = await self._hook_runner.run(hook, {}, context)
                if not success:
                    raise WorkflowError(f"{context} failed after retries")
            finally:
                self._execution_observer.completion_finished(self.config.name, index)

    async def _process_entity(
        self,
        transition: TransitionConfig,
        entity: Path,
    ) -> TransitionResult:
        if not entity.exists():
            return TransitionResult(moved=False, jump=None)

        context = (
            f"transition hook {self.config.name}:{transition.source}->{transition.destination} "
            f"entity={entity.name}"
        )
        extra_env = {"INPUT_ENTITY": str(entity.resolve())}

        if transition.cmd is None:
            success = True
        else:
            success = await self._hook_runner.run(
                HookConfig(cmd=transition.cmd, stdin=transition.stdin), extra_env, context
            )

        if success:
            await self._entities.move_to_state(
                self.config.name, transition.destination, entity
            )
            self._logger.info(
                "Moved entity '%s' to %s/%s",
                entity.name,
                self.config.name,
                transition.destination,
            )
            return TransitionResult(moved=True, jump=transition.jump)

        await self._entities.move_to_state(self.config.name, FAILED_STATE, entity)
        self._logger.error(
            "Transition failed for '%s'; moved to %s/%s",
            entity.name,
            self.config.name,
            FAILED_STATE,
        )
        return TransitionResult(moved=False, jump=None)


class AllAtOncePhaseProcessor(PhaseProcessor):
    """Processes entities together in groups, applying each transition to all applicable entities before moving on to the next transition."""

    async def _run(self) -> int:
        moved_total = 0
        while True:
            moved_this_pass = 0
            for transition in self.config.transitions:
                moved, jumps = await self._apply_transition(transition)
                moved_this_pass += moved
                moved_total += moved
                for jump_name in jumps:
                    await self._jump_handler(jump_name, self.config.name)
            if moved_this_pass == 0:
                return moved_total

    async def _apply_transition(
        self,
        transition: TransitionConfig,
    ) -> tuple[int, list[str]]:
        entities = self._entities.list_transition_entities(
            self.config.name, transition.source
        )
        entities = [
            entity for entity in entities if not self._is_entity_locked(entity.name)
        ]
        if not entities:
            return 0, []

        moved = 0
        jumps: list[str] = []
        for group in self._entities.group_entities(entities):
            results = await self._process_group(transition, group)
            for result in results:
                if result.moved:
                    moved += 1
                    if result.jump is not None:
                        jumps.append(result.jump)
        return moved, jumps

    async def _process_group(
        self,
        transition: TransitionConfig,
        group: Group,
    ) -> list[TransitionResult]:
        if group.concurrent:
            self._logger.info(
                "Running transition %s.%s -> %s for %d concurrent entities (group=%s)",
                self.config.name,
                transition.source,
                transition.destination,
                len(group.entities),
                group.key,
            )
            entity_ids = tuple(entity.name for entity in group.entities)
            self._execution_observer.transition_started(
                self.config.name,
                self.config.mode,
                transition.source,
                transition.destination,
                entity_ids,
            )
            tasks = [
                self._process_entity(transition, entity)
                for entity in group.entities
            ]
            try:
                return list(await asyncio.gather(*tasks))
            finally:
                self._execution_observer.transition_finished(
                    self.config.name,
                    transition.source,
                    transition.destination,
                    entity_ids,
                )
        entity_ids = (group.entities[0].name,)
        self._execution_observer.transition_started(
            self.config.name,
            self.config.mode,
            transition.source,
            transition.destination,
            entity_ids,
        )
        try:
            return [
                await self._process_entity(transition, entity)
                for entity in group.entities
            ]
        finally:
            self._execution_observer.transition_finished(
                self.config.name,
                transition.source,
                transition.destination,
                entity_ids,
            )


class OneAtATimePhaseProcessor(PhaseProcessor):
    """Processes entities singularly, applying all possible transitions to a single entity before moving to the next one."""

    async def _run(self) -> int:
        moved_total = 0
        while True:
            moved_this_pass = 0
            for entity in self._entities_for_pass():
                moved = await self._flow_entity_to_rest(entity)
                moved_this_pass += moved
                moved_total += moved
            if moved_this_pass == 0:
                return moved_total

    def _entities_for_pass(self) -> list[Path]:
        entities = self._entities.list_phase_entities(self.config)
        unlocked_entities = [
            entity for entity in entities if not self._is_entity_locked(entity.name)
        ]
        cursor_name = self._load_entity_cursor(self.config.name)
        if cursor_name is None:
            return unlocked_entities

        for entity in unlocked_entities:
            if entity.name == cursor_name:
                self._logger.info(
                    "Resuming entity cursor '%s' in phase '%s'",
                    cursor_name,
                    self.config.name,
                )
                return [entity] + [
                    candidate for candidate in unlocked_entities if candidate != entity
                ]

        for entity in entities:
            if entity.name == cursor_name and self._is_entity_locked(entity.name):
                self._logger.info(
                    "State cursor '%s' in phase '%s' is locked; clearing cursor",
                    cursor_name,
                    self.config.name,
                )
                self._clear_entity_cursor()
                return unlocked_entities

        self._logger.warning(
            "State cursor for phase '%s' entity '%s' is stale; clearing and continuing",
            self.config.name,
            cursor_name,
        )
        self._clear_entity_cursor()
        return unlocked_entities

    async def _flow_entity_to_rest(self, entity: Path) -> int:
        if not entity.exists():
            return 0

        self._save_entity_cursor(self.config.name, entity.name)

        moved = 0
        current = entity
        while True:
            state_name = current.parent.name
            transition = _find_transition_from_state(self.config, state_name)
            if transition is None:
                self._clear_entity_cursor()
                return moved

            entity_ids = (current.name,)
            self._execution_observer.transition_started(
                self.config.name,
                self.config.mode,
                transition.source,
                transition.destination,
                entity_ids,
            )
            try:
                result = await self._process_entity(transition, current)
            finally:
                self._execution_observer.transition_finished(
                    self.config.name,
                    transition.source,
                    transition.destination,
                    entity_ids,
                )
            if not result.moved:
                self._clear_entity_cursor()
                return moved

            moved += 1
            current = (
                self._entities.dir_for(self.config.name, transition.destination)
                / current.name
            )
            if result.jump is not None:
                await self._jump_handler(result.jump, self.config.name)


PHASE_PROCESSOR_FOR_MODE = {
    PHASE_MODE_TRANSITIONS: AllAtOncePhaseProcessor,
    PHASE_MODE_ENTITY: OneAtATimePhaseProcessor,
}


class WorkflowEngine:
    """Coordinates full workflow scheduling across phases and jumps."""

    @dataclass(frozen=True)
    class Deps:
        state: RuntimeStateStore
        entities: EntityStore
        hook_runner: HookRunner
        logger: logging.Logger
        execution_observer: NullExecutionObserver | None = None
        is_entity_locked: EntityLockChecker | None = None

    def __init__(
        self,
        config: WorkflowConfig,
        deps: Deps,
    ) -> None:
        self._config = config
        self._state = deps.state
        self._entities = deps.entities
        self._hook_runner = deps.hook_runner
        self._logger = deps.logger
        self._phases = {phase.name: phase for phase in config.phases}
        self._snapshot: RuntimeSnapshot | None = None
        self._did_run_init = False
        execution_observer = deps.execution_observer or NullExecutionObserver()
        is_entity_locked = deps.is_entity_locked or (lambda _entity_id: False)
        self._phase_processor_deps = PhaseProcessorDeps(
            hook_runner=deps.hook_runner,
            entities=deps.entities,
            logger=deps.logger,
            jump_handler=self._run_jump,
            load_entity_cursor=self._load_entity_cursor,
            save_entity_cursor=self._save_entity_cursor,
            clear_entity_cursor=self._clear_entity_cursor,
            execution_observer=execution_observer,
            is_entity_locked=is_entity_locked,
        )
        self._execution_observer = execution_observer

    def _processor_for_phase(self, phase: PhaseConfig) -> PhaseProcessor:
        return PHASE_PROCESSOR_FOR_MODE[phase.mode](self._phase_processor_deps, phase)

    async def run(self) -> None:
        self._entities.ensure_layout()
        if not self._did_run_init:
            await self._run_init()
            self._did_run_init = True

        phase_order = self._config.phase_order
        first_phase = phase_order[0]
        self._snapshot = self._load_or_init_snapshot(phase_order)

        wrapped_to_first = False
        while True:
            phase_name = self._snapshot.current_phase
            processor = self._processor_for_phase(self._phases[phase_name])
            moved = await processor.run_phase()

            if self._unwind_jump_if_target_phase(phase_name):
                continue

            if wrapped_to_first and phase_name == first_phase and moved == 0:
                self._logger.info(
                    "Reached stable fixpoint at first phase '%s'; exiting", first_phase
                )
                return

            current_index = phase_order.index(phase_name)
            next_phase = phase_order[(current_index + 1) % len(phase_order)]
            self._set_current_phase(next_phase)
            self._clear_entity_cursor()
            if next_phase == first_phase:
                wrapped_to_first = True

    async def _run_jump(self, target_phase: str, source_phase: str) -> None:
        if target_phase == source_phase:
            self._logger.warning("Ignoring self-jump from phase '%s'", source_phase)
            return

        source_entity_name = self._load_entity_cursor(source_phase)
        frame = JumpFrame(
            source_phase=source_phase,
            target_phase=target_phase,
            source_entity_name=source_entity_name,
        )

        self._logger.info(
            "Jumping from phase '%s' to phase '%s'", source_phase, target_phase
        )
        self._execution_observer.jump_started(source_phase, target_phase)

        snapshot = self._require_snapshot()
        self._persist_snapshot(
            replace(
                snapshot,
                jump_stack=(*snapshot.jump_stack, frame),
                current_phase=target_phase,
                entity_cursor=None,
            )
        )

        processor = self._processor_for_phase(self._phases[target_phase])
        try:
            await processor.run_phase()
        finally:
            self._return_from_jump_frame(frame)
            self._execution_observer.jump_finished(source_phase, target_phase)
            self._logger.info(
                "Returning to phase '%s' from jump phase '%s'", source_phase, target_phase
            )

    async def _run_init(self) -> None:
        hook = self._config.init
        if hook is None:
            return

        context = "init hook"
        self._logger.info("Running %s", context)
        self._execution_observer.init_started()
        try:
            success = await self._hook_runner.run(hook, {}, context)
            if not success:
                raise WorkflowError(f"{context} failed after retries")
        finally:
            self._execution_observer.init_finished()

    def _load_or_init_snapshot(self, phase_order: tuple[str, ...]) -> RuntimeSnapshot:
        snapshot = self._state.load_snapshot()
        if snapshot is None:
            snapshot = RuntimeSnapshot(
                schema_version=STATE_SCHEMA_VERSION,
                current_phase=phase_order[0],
                jump_stack=(),
                entity_cursor=None,
            )
            self._state.save_snapshot(snapshot)
            return snapshot

        self._validate_snapshot_phases(snapshot, phase_order)
        return snapshot

    def _validate_snapshot_phases(
        self,
        snapshot: RuntimeSnapshot,
        phase_order: tuple[str, ...],
    ) -> None:
        if snapshot.current_phase not in self._phases:
            raise WorkflowError(
                f"State file references unknown phase '{snapshot.current_phase}'. "
                f"Known phases: {', '.join(phase_order)}"
            )

        for frame in snapshot.jump_stack:
            if frame.source_phase not in self._phases or frame.target_phase not in self._phases:
                raise WorkflowError(
                    "State file contains jump frame with unknown phases "
                    f"'{frame.source_phase}->{frame.target_phase}'. "
                    f"Known phases: {', '.join(phase_order)}"
                )

        cursor = snapshot.entity_cursor
        if cursor is not None and cursor.phase not in self._phases:
            raise WorkflowError(
                f"State file references unknown cursor phase '{cursor.phase}'. "
                f"Known phases: {', '.join(phase_order)}"
            )

    def _unwind_jump_if_target_phase(self, phase_name: str) -> bool:
        snapshot = self._require_snapshot()
        if not snapshot.jump_stack:
            return False

        frame = snapshot.jump_stack[-1]
        if frame.target_phase != phase_name:
            return False

        self._logger.info(
            "Restoring source phase '%s' after resumed jump phase '%s'",
            frame.source_phase,
            frame.target_phase,
        )
        self._return_from_jump_frame(frame)
        return True

    def _return_from_jump_frame(self, frame: JumpFrame) -> None:
        snapshot = self._require_snapshot()
        if not snapshot.jump_stack or snapshot.jump_stack[-1] != frame:
            raise WorkflowError(
                "State file jump stack is inconsistent during jump return"
            )

        restored_cursor = (
            None
            if frame.source_entity_name is None
            else EntityCursor(phase=frame.source_phase, entity_name=frame.source_entity_name)
        )
        self._persist_snapshot(
            replace(
                snapshot,
                jump_stack=snapshot.jump_stack[:-1],
                current_phase=frame.source_phase,
                entity_cursor=restored_cursor,
            )
        )

    def _set_current_phase(self, phase_name: str) -> None:
        snapshot = self._require_snapshot()
        self._persist_snapshot(replace(snapshot, current_phase=phase_name))

    def _load_entity_cursor(self, phase_name: str) -> str | None:
        cursor = self._require_snapshot().entity_cursor
        if cursor is None:
            return None
        if cursor.phase != phase_name:
            return None
        return cursor.entity_name

    def _save_entity_cursor(self, phase_name: str, entity_name: str) -> None:
        snapshot = self._require_snapshot()
        cursor = snapshot.entity_cursor
        if cursor is not None and cursor.phase == phase_name and cursor.entity_name == entity_name:
            return
        self._persist_snapshot(
            replace(snapshot, entity_cursor=EntityCursor(phase=phase_name, entity_name=entity_name))
        )

    def _clear_entity_cursor(self) -> None:
        snapshot = self._require_snapshot()
        if snapshot.entity_cursor is None:
            return
        self._persist_snapshot(replace(snapshot, entity_cursor=None))

    def _persist_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        self._snapshot = snapshot
        self._state.save_snapshot(snapshot)

    def _require_snapshot(self) -> RuntimeSnapshot:
        if self._snapshot is None:
            raise WorkflowError("Runtime snapshot is unavailable")
        return self._snapshot


def _find_transition_from_state(
    phase: PhaseConfig,
    state_name: str,
) -> TransitionConfig | None:
    for transition in phase.transitions:
        if transition.source == state_name:
            return transition
    return None
