from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .cli import configure_logging
from .config_loader import load_workflow
from .entities import EntityStore
from .env import build_defined_hook_env, build_hook_env_from_defined
from .hooks import HookRunner, HookRunnerConfig
from .models import CliOptions
from .state import RuntimeStateStore
from .workflow import WorkflowEngine

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
    config = load_workflow(resolve_workflow_path(options.workflow))

    retries = (
        options.retries_override
        if options.retries_override is not None
        else config.retries
    )
    template_env = build_defined_hook_env(config, options.root)
    base_env = build_hook_env_from_defined(template_env)

    state = RuntimeStateStore(options.root, options.state_file)
    hook_runner = HookRunner(
        HookRunnerConfig(
            root=options.root,
            base_env=base_env,
            template_env=template_env,
            retries=retries,
            logger=logger,
        )
    )
    entities = EntityStore(options.root, config)
    engine = WorkflowEngine(
        config,
        WorkflowEngine.Deps(
            state=state,
            entities=entities,
            hook_runner=hook_runner,
            logger=logger,
        ),
    )
    if options.watch:
        await _run_watch_loop(engine, entities, logger)
        return
    await engine.run()


async def _run_watch_loop(
    engine: WorkflowEngine,
    entities: EntityStore,
    logger,
) -> None:
    while True:
        await engine.run()
        previous_layout = entities.entity_layout()
        logger.info("Watch mode idle; waiting for entity layout changes")

        while True:
            await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
            current_layout = entities.entity_layout()
            if current_layout == previous_layout:
                continue
            logger.info("Detected entity layout change; resuming workflow")
            break
