from __future__ import annotations

from .cli import configure_logging
from .config_loader import load_workflow
from .entities import EntityStore
from .env import build_hook_env
from .hooks import HookRunner, HookRunnerConfig
from .models import CliOptions
from .state import RuntimeStateStore
from .workflow import WorkflowEngine


async def run(options: CliOptions) -> None:
    logger = configure_logging(options.log_level)
    config = load_workflow(options.workflow)

    retries = (
        options.retries_override
        if options.retries_override is not None
        else config.retries
    )
    base_env = build_hook_env(config, options.root)

    state = RuntimeStateStore(options.root, options.state_file)
    hook_runner = HookRunner(
        HookRunnerConfig(
            root=options.root,
            base_env=base_env,
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
    await engine.run()
