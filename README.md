# Dirorch

Dirorch is a directory-based workflow orchestrator.

It executes workflow phases defined in YAML, where each entity is represented as a file in a phase/state directory and transitions are powered by shell hooks.

## Code Architecture

The implementation is organized into focused modules under `dirorch/`:

- `config_loader.py`: YAML parsing and validation
- `workflow.py`: phase processing and orchestration loop
- `entities.py`: filesystem-backed entity/state operations
- `hooks.py`: hook execution + retries
- `state.py`: runtime phase persistence
- `env.py`: hook environment composition
- `app.py`: top-level dependency wiring (`run`)
- `cli.py`: argument parsing and logging setup

`main.py` is intentionally thin and exists as a compatibility entrypoint for imports and CLI execution.

## What It Does

- Models workflow state directly on disk:
  - one directory per phase
  - one child directory per state
  - one file per entity
- Runs transitions until each phase reaches fixpoint (no more applicable moves)
- Supports transition hooks, completion hooks, retries, jump phases, resume-from-state, and grouped concurrency

## Requirements

- Python `>=3.11`
- `pyyaml>=6.0.2`

## Installation

With `uv`:

```bash
uv sync
```

Install as a global tool (exposes `dirorch` command):

```bash
uv tool install .
```

Or with `pip`:

```bash
pip install -e .
```

## Quick Start

1. Create a workflow file:

```yaml
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
```

2. Create initial entities:

```bash
mkdir -p ./work/tasks/new
echo "task1" > ./work/tasks/new/01-task.txt
echo "task2" > ./work/tasks/new/02-task.txt
```

3. Run Dirorch:

```bash
dirorch ./workflow.yaml --root ./work
```

Or run by workflow name from global config:

```bash
dirorch my-workflow --root ./work
```

Named workflows resolve in:

- `$XDG_CONFIG_DIR/dirorch/workflows/<name>.yml` when `XDG_CONFIG_DIR` is set
- `~/.config/dirorch/workflows/<name>.yml` otherwise

Alternative (without installing a tool):

```bash
python main.py ./workflow.yaml --root ./work
```

After completion, entities will be moved into `./work/tasks/done`.

## CLI Usage

```text
dirorch [-h] [--root ROOT] [--retries RETRIES]
        [--state-file STATE_FILE]
        [--log-level {DEBUG,INFO,WARNING,ERROR}]
        workflow
```

Arguments:

- `workflow`: either a path to a workflow YAML file, or a name that resolves to `<config>/dirorch/workflows/<name>.yml`
  - `<config>` is `$XDG_CONFIG_DIR` when set
  - fallback `<config>` is `~/.config`
- `--root`: workflow state root directory (default: current directory)
- `--retries`: override YAML retry count (`0` means one attempt total)
- `--state-file`: runtime state filename under `--root` (default: `.dirorch_runtime.json`)
- `--log-level`: `DEBUG|INFO|WARNING|ERROR` (default: `INFO`)

## Workflow YAML Reference

Top-level fields:

- `phases` (required): mapping of phase name -> phase definition
- `retries` (optional): non-negative integer, default `3`
- `env` or `environment` (optional): map of string env vars passed to hooks
- `init` (optional): one-time startup hook before any phase runs

Phase fields:

- `states` (required): non-empty list of state names
- `mode` (optional): phase execution strategy
  - `transitions` (default): process each transition rule across all applicable entities
  - `entity`: process one entity through transitions until no transition applies, then next entity
- `transitions` (optional): list of transition definitions
- `completions` (optional): list of completion hook definitions
  - `completion` is also accepted as alias

Transition fields:

- `from` (required): source state
- `to` (required): destination state
- `cmd` (optional): shell command to run before move
- `stdin` (optional): text rendered and piped to the hook process stdin (requires `cmd`)
- `jump` (optional): target phase name to run to fixpoint after successful transition

Completion hook fields:

- either a string command:
  - `- "echo done"`
- or object with `cmd`:
  - `- cmd: "echo done"`
  - optional `stdin`:
    - `- cmd: "cat > out.txt"`
    - `  stdin: "hello {{ MY_VAR }}"`

Init hook fields:

- either a string command:
  - `init: "echo setup"`
- or object with `cmd`:
  - `init: { cmd: "echo setup" }`
  - optional `stdin`:
    - `init: { cmd: "cat > setup.txt", stdin: "seed={{ APP_SEED }}" }`

Reserved state:

- `_failed` is reserved and may not appear in `states`

## Hook Environment

Transition hooks receive:

- `INPUT_ENTITY`: absolute path to the entity file currently being processed
- `DIR_<PHASE>_<STATE>` for every declared phase/state directory

Completion hooks receive:

- `DIR_<PHASE>_<STATE>` for every declared phase/state directory

Init hook receives:

- `DIR_<PHASE>_<STATE>` for every declared phase/state directory

All hooks also receive:

- current process environment
- values from YAML `env`/`environment`

## Stdin Templates

If a hook defines `stdin`, Dirorch renders it with Jinja2 before piping it into the command.

Template context includes only Dirorch-defined variables:

- YAML `env`/`environment` values
- generated `DIR_<PHASE>_<STATE>` variables
- hook runtime variables like `INPUT_ENTITY` (for transition hooks)

External inherited process variables are not available in templates.

Template helpers:

- `read_file(path)` (alias: `include_file(path)`): reads UTF-8 file content and inserts it into the rendered stdin text.
  - relative paths resolve from `--root`
  - you can pass path variables, e.g. `{{ read_file(FILE_TO_INCLUDE) }}`

Env var naming for `DIR_<PHASE>_<STATE>`:

- uppercased
- non-alphanumeric characters replaced with `_`
- example: phase `task-items`, state `in.progress` -> `DIR_TASK_ITEMS_IN_PROGRESS`

## Directory Layout

Given:

- root: `./work`
- phase: `tasks`
- states: `new`, `in_progress`, `done`

Dirorch uses:

```text
./work/
  tasks/
    new/
    in_progress/
    done/
    _failed/
```

Files in state directories are workflow entities.

## Execution Model

1. Create missing phase/state directories (including `_failed`).
2. Run `init` hook once (if configured).
3. Start from saved phase in runtime state file (or first phase if none).
4. For current phase:
   - apply transitions repeatedly until phase fixpoint
   - then run completion hooks
5. Move to next phase and repeat.
6. After last phase, return to first phase.
7. Exit successfully only when the first phase immediately reaches fixpoint with zero moves.

Transition processing details:

- Source entities are sorted alphabetically by filename.
- If transition `cmd` succeeds (or no `cmd`), entity moves `from -> to`.
- If `cmd` fails, it is retried `retries + 1` total attempts.
- If still failing, entity moves to `_failed` and no jump occurs for that entity.
- On successful transition with `jump`, target phase is run to fixpoint, then execution returns to the current phase.
- `init` and completion hooks use the same retry policy as transition hooks (`retries + 1` attempts total).

Phase `mode` behavior:

- `transitions` mode:
  - Dirorch applies each transition rule to all matching entities, looping until fixpoint.
  - Grouped concurrency (`NN-name.ext`) is enabled in this mode.
- `entity` mode:
  - Dirorch picks entities by filename, moves each entity through transitions until it comes to rest, then picks the next entity.
  - Jumps still run immediately after a successful transition with `jump`.
  - Processing is sequential per entity (no grouped concurrent transition execution).

Concurrency rule:

- Files named like `NN-name.ext` (numeric prefix + `-`) are grouped by `NN`.
- Entities in the same group may run concurrently.
- Other entities are processed sequentially.

## Runtime State and Resume

Dirorch stores runtime phase state in:

- `${root}/${state-file}`
- default: `.dirorch_runtime.json`

Example:

```json
{
  "current_phase": "tasks"
}
```

On restart, Dirorch resumes from that phase.

## Example Workflow (With Jump + Completion)

```yaml
retries: 2
env:
  PROJECT_ROOT: /workspace/project
init:
  cmd: >
    ./bootstrap-project "$DIR_TASKS_NEW" "$DIR_SUBTASKS_NEW"

phases:
  tasks:
    states:
      - new
      - in_progress
      - complete
    transitions:
      - from: new
        to: in_progress
        cmd: >
          ./plan-task "$INPUT_ENTITY" "$DIR_SUBTASKS_NEW"
        jump: subtasks
      - from: in_progress
        to: complete
        cmd: >
          ./complete-task "$INPUT_ENTITY"
    completions:
      - cmd: >
          ./generate-task-summary "$DIR_TASKS_COMPLETE"

  subtasks:
    states:
      - new
      - complete
    transitions:
      - from: new
        to: complete
        cmd: >
          ./exec-subtask "$INPUT_ENTITY"
```

## Logging

Logging includes:

- phase start/fixpoint events
- hook failures/retries
- entity moves
- jump execution
- hook command stdout/stderr streamed directly to the terminal

## Running Tests

```bash
uv run --with pytest pytest -q
```

Or, if installed with dev extras:

```bash
pip install -e ".[dev]"
pytest -q
```

## Troubleshooting

- `Invalid YAML ...`: check quoting, indentation, and command string syntax.
- `jump target ... is undefined`: `jump` must reference an existing phase name.
- `transition source/destination ... is not a phase state`: verify `from`/`to` are declared in phase `states`.
- `init hook failed after retries`: inspect startup command and any required files/directories.
- Completion hook failure aborts the run: inspect logs and hook exit status.
- Entities unexpectedly in `_failed`: transition hook exhausted retries and never succeeded.
