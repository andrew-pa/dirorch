from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from .constants import FAILED_STATE, PHASE_MODE_ENTITY
from .entities import EntityStore
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
from .state import RuntimeStateStore

JumpHandler = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True)
class PhaseProcessorDeps:
    hook_runner: HookRunner
    entities: EntityStore
    logger: logging.Logger
    jump_handler: JumpHandler


class PhaseProcessor:
    """Runs a single phase to fixpoint, independent of global phase scheduling."""

    def __init__(self, deps: PhaseProcessorDeps) -> None:
        self._hook_runner = deps.hook_runner
        self._entities = deps.entities
        self._logger = deps.logger
        self._jump_handler = deps.jump_handler

    async def run_phase(self, phase: PhaseConfig) -> int:
        self._logger.info("Processing phase '%s'", phase.name)
        if phase.mode == PHASE_MODE_ENTITY:
            moved_total = await self._run_phase_entity_mode(phase)
        else:
            moved_total = await self._run_phase_transition_mode(phase)
        await self._run_completions(phase)
        self._logger.info(
            "Phase '%s' reached fixpoint; transitions=%d", phase.name, moved_total
        )
        return moved_total

    async def _run_phase_transition_mode(self, phase: PhaseConfig) -> int:
        moved_total = 0
        while True:
            moved_this_pass = 0
            for transition in phase.transitions:
                moved, jumps = await self._apply_transition(phase, transition)
                moved_this_pass += moved
                moved_total += moved
                for jump_name in jumps:
                    await self._jump_handler(jump_name, phase.name)
            if moved_this_pass == 0:
                return moved_total

    async def _run_phase_entity_mode(self, phase: PhaseConfig) -> int:
        moved_total = 0
        while True:
            moved_this_pass = 0
            for entity in self._entities.list_phase_entities(phase):
                moved = await self._flow_entity_to_rest(phase, entity)
                moved_this_pass += moved
                moved_total += moved
            if moved_this_pass == 0:
                return moved_total

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
        entities = self._entities.list_transition_entities(
            phase.name, transition.source
        )
        if not entities:
            return 0, []

        moved = 0
        jumps: list[str] = []
        for group in self._entities.group_entities(entities):
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
        return [
            await self._process_entity(phase, transition, entity)
            for entity in group.entities
        ]

    async def _flow_entity_to_rest(self, phase: PhaseConfig, entity: Path) -> int:
        if not entity.exists():
            return 0

        moved = 0
        current = entity
        while True:
            state_name = current.parent.name
            transition = _find_transition_from_state(phase, state_name)
            if transition is None:
                return moved

            result = await self._process_entity(phase, transition, current)
            if not result.moved:
                return moved

            moved += 1
            current = (
                self._entities.dir_for(phase.name, transition.destination)
                / current.name
            )
            if result.jump is not None:
                await self._jump_handler(result.jump, phase.name)

    async def _process_entity(
        self,
        phase: PhaseConfig,
        transition: TransitionConfig,
        entity: Path,
    ) -> TransitionResult:
        if not entity.exists():
            return TransitionResult(moved=False, jump=None)

        context = (
            f"transition hook {phase.name}:{transition.source}->{transition.destination} "
            f"entity={entity.name}"
        )
        extra_env = {"INPUT_ENTITY": str(entity.resolve())}

        if transition.cmd is None:
            success = True
        else:
            success = await self._hook_runner.run(
                HookConfig(transition.cmd), extra_env, context
            )

        if success:
            await self._entities.move_to_state(
                phase.name, transition.destination, entity
            )
            self._logger.info(
                "Moved entity '%s' to %s/%s",
                entity.name,
                phase.name,
                transition.destination,
            )
            return TransitionResult(moved=True, jump=transition.jump)

        await self._entities.move_to_state(phase.name, FAILED_STATE, entity)
        self._logger.error(
            "Transition failed for '%s'; moved to %s/%s",
            entity.name,
            phase.name,
            FAILED_STATE,
        )
        return TransitionResult(moved=False, jump=None)


class WorkflowEngine:
    """Coordinates full workflow scheduling across phases and jumps."""

    @dataclass(frozen=True)
    class Deps:
        state: RuntimeStateStore
        entities: EntityStore
        hook_runner: HookRunner
        logger: logging.Logger

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
        self._phase_processor = PhaseProcessor(
            deps=PhaseProcessorDeps(
                hook_runner=deps.hook_runner,
                entities=deps.entities,
                logger=deps.logger,
                jump_handler=self._run_jump,
            )
        )

    async def run(self) -> None:
        self._entities.ensure_layout()
        await self._run_init()

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
            moved = await self._phase_processor.run_phase(self._phases[phase_name])
            if wrapped_to_first and phase_name == first_phase and moved == 0:
                self._logger.info(
                    "Reached stable fixpoint at first phase '%s'; exiting", first_phase
                )
                return
            current_index = (current_index + 1) % len(phase_order)
            if current_index == 0:
                wrapped_to_first = True

    async def _run_jump(self, target_phase: str, source_phase: str) -> None:
        if target_phase == source_phase:
            self._logger.warning("Ignoring self-jump from phase '%s'", source_phase)
            return

        self._logger.info(
            "Jumping from phase '%s' to phase '%s'", source_phase, target_phase
        )
        self._state.save_current_phase(target_phase)
        await self._phase_processor.run_phase(self._phases[target_phase])
        self._state.save_current_phase(source_phase)
        self._logger.info(
            "Returning to phase '%s' from jump phase '%s'", source_phase, target_phase
        )

    async def _run_init(self) -> None:
        hook = self._config.init
        if hook is None:
            return

        context = "init hook"
        self._logger.info("Running %s", context)
        success = await self._hook_runner.run(hook, {}, context)
        if not success:
            raise WorkflowError(f"{context} failed after retries")


def _find_transition_from_state(
    phase: PhaseConfig,
    state_name: str,
) -> TransitionConfig | None:
    for transition in phase.transitions:
        if transition.source == state_name:
            return transition
    return None
