from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from .cli import configure_logging
from .config_loader import load_workflow
from .constants import LOCKS_FILE_NAME
from .constants import PAUSED_FILE_NAME
from .entities import EntityStore
from .env import build_defined_hook_env, build_hook_env_from_defined
from .entity_logging import (
    EntityLogBroadcaster,
    EntityLogRouter,
    EntityTranscriptFormatter,
    EntityTranscriptSink,
    EntityTranscriptStore,
)
from .execution import ExecutionStatusTracker
from .files import FileStore
from .hooks import HookRunner, HookRunnerConfig
from .locks import EntityLockStore
from .models import CliOptions
from .pauses import ActiveShellCommandRegistry, EntityPauseStore
from .services import (
    EntityAdminService,
    FileAdminService,
    EntityLogService,
    MutationCoordinator,
    WorkflowDefinitionService,
    WorkflowStatusService,
)
from .state import RuntimeStateStore
from .workflow import WorkflowEngine
from .web import WebServer, build_web_app
from .web.app import WebServices

WATCH_POLL_INTERVAL_SECONDS = 0.25


def resolve_workflow_path(workflow: Path) -> Path:
    """Resolve workflow CLI input to a concrete YAML path."""
    if _is_explicit_path(workflow):
        return workflow.expanduser()
    return _config_dir() / "dirorch" / "workflows" / f"{workflow.name}.yml"


def _is_explicit_path(workflow: Path) -> bool:
    return workflow.is_absolute() or workflow.parent != Path(".") or workflow.suffix != ""


def _config_dir() -> Path:
    xdg_config_dir = os.environ.get("XDG_CONFIG_DIR")
    if xdg_config_dir:
        return Path(xdg_config_dir).expanduser()
    return Path.home() / ".config"


async def run(options: CliOptions) -> None:
    logger = configure_logging(options.log_level)
    runtime = _build_runtime(options, logger)
    if not options.web:
        await runtime.workflow_runner.run()
        return

    assert runtime.web_server is not None
    stop_event = asyncio.Event()
    installed_signals = _install_signal_handlers(stop_event)
    runner_task = asyncio.create_task(runtime.workflow_runner.run())
    runner_task.add_done_callback(
        lambda task: _log_runner_outcome(task, logger)
    )
    try:
        await runtime.web_server.start()
        logger.info(
            "HTTP API listening on http://%s:%d",
            options.web_host,
            options.web_port,
        )
        await stop_event.wait()
    finally:
        _remove_signal_handlers(installed_signals)
        await runtime.web_server.stop()
        if not runner_task.done():
            runner_task.cancel()
        with suppress(asyncio.CancelledError):
            await runner_task


@dataclass(frozen=True)
class RuntimeContext:
    workflow_runner: "WorkflowRunner"
    web_server: WebServer | None


class WorkflowRunner:
    def __init__(
        self,
        engine: WorkflowEngine,
        entities: EntityStore,
        logger: logging.Logger,
        tracker: ExecutionStatusTracker,
        watch: bool,
    ) -> None:
        self._engine = engine
        self._entities = entities
        self._logger = logger
        self._tracker = tracker
        self._watch = watch

    async def run(self) -> None:
        try:
            if self._watch:
                await self._run_watch_loop()
            else:
                self._tracker.runner_started()
                await self._engine.run()
                self._tracker.runner_stopped()
        except asyncio.CancelledError:
            self._tracker.runner_stopped()
            raise
        except Exception as exc:
            self._tracker.runner_failed(str(exc))
            raise

    async def _run_watch_loop(self) -> None:
        while True:
            self._tracker.runner_started()
            await self._engine.run()
            previous_layout = self._entities.entity_layout()
            self._logger.info("Watch mode idle; waiting for entity layout changes")
            self._tracker.runner_waiting()

            while True:
                await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
                current_layout = self._entities.entity_layout()
                if current_layout == previous_layout:
                    continue
                self._logger.info("Detected entity layout change; resuming workflow")
                break


def _build_runtime(options: CliOptions, logger: logging.Logger) -> RuntimeContext:
    config = load_workflow(resolve_workflow_path(options.workflow))
    retries = (
        options.retries_override
        if options.retries_override is not None
        else config.retries
    )
    template_env = build_defined_hook_env(config, options.root)
    base_env = build_hook_env_from_defined(template_env)

    tracker = ExecutionStatusTracker()
    coordinator = MutationCoordinator()
    state = RuntimeStateStore(options.root, options.state_file)
    locks = EntityLockStore(options.root, LOCKS_FILE_NAME)
    pauses = EntityPauseStore(options.root, PAUSED_FILE_NAME)
    command_registry = ActiveShellCommandRegistry(pauses.is_paused)
    entities = EntityStore(options.root, config)
    files = FileStore(
        options.root,
        config,
        options.state_file,
        LOCKS_FILE_NAME,
        PAUSED_FILE_NAME,
    )
    transcript_store = EntityTranscriptStore(options.root)
    broadcaster = EntityLogBroadcaster()
    entity_log_emitter = EntityLogRouter(
        (
            EntityTranscriptSink(
                EntityTranscriptFormatter(),
                transcript_store,
                broadcaster,
            ),
        )
    )
    hook_runner = HookRunner(
        HookRunnerConfig(
            root=options.root,
            base_env=base_env,
            template_env=template_env,
            retries=retries,
            logger=logger,
            cwd=config.cwd,
            entity_log_emitter=entity_log_emitter,
            is_entity_paused=pauses.is_paused,
            command_registry=command_registry,
        )
    )
    engine = WorkflowEngine(
        config,
        WorkflowEngine.Deps(
            state=state,
            entities=entities,
            hook_runner=hook_runner,
            logger=logger,
            execution_observer=tracker,
            is_entity_locked=locks.is_locked,
            is_entity_paused=pauses.is_paused,
            entity_log_emitter=entity_log_emitter,
        ),
    )
    workflow_runner = WorkflowRunner(
        engine=engine,
        entities=entities,
        logger=logger,
        tracker=tracker,
        watch=options.watch,
    )

    web_server: WebServer | None = None
    if options.web:
        entity_service = EntityAdminService(
            entities=entities,
            locks=locks,
            pauses=pauses,
            tracker=tracker,
            coordinator=coordinator,
            command_registry=command_registry,
            entity_log_emitter=entity_log_emitter,
        )
        log_service = EntityLogService(
            entities=entities,
            tracker=tracker,
            store=transcript_store,
            broadcaster=broadcaster,
        )
        services = WebServices(
            definition=WorkflowDefinitionService(config),
            status=WorkflowStatusService(
                state=state,
                tracker=tracker,
                entities=entity_service,
                locks=locks,
                pauses=pauses,
                config=config,
            ),
            entities=entity_service,
            logs=log_service,
            files=FileAdminService(files, coordinator),
        )
        web_server = WebServer(
            build_web_app(services),
            options.web_host,
            options.web_port,
            access_log_enabled=options.web_log,
        )

    return RuntimeContext(
        workflow_runner=workflow_runner,
        web_server=web_server,
    )


def _install_signal_handlers(stop_event: asyncio.Event) -> list[signal.Signals]:
    installed: list[signal.Signals] = []
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except NotImplementedError:
            continue
        installed.append(signum)
    return installed


def _remove_signal_handlers(signals_to_remove: list[signal.Signals]) -> None:
    loop = asyncio.get_running_loop()
    for signum in signals_to_remove:
        with suppress(NotImplementedError):
            loop.remove_signal_handler(signum)


def _log_runner_outcome(task: asyncio.Task[None], logger: logging.Logger) -> None:
    with suppress(asyncio.CancelledError):
        exc = task.exception()
        if exc is not None:
            logger.exception("Workflow runner failed", exc_info=exc)
