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
- Persists per-entity append-only transcripts under `${root}/entity_logs/`
- Runs transitions until each phase reaches fixpoint (no more applicable moves)
- Supports transition hooks, completion hooks, retries, jump phases, resume-from-state, and grouped concurrency
- Can stay in `--watch` mode and rerun when entities are added or externally moved

## Requirements

- Python `>=3.11`
- `aiohttp>=3.12.14`
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
        [--state-file STATE_FILE] [--watch] [--web]
        [--web-log] [--web-host WEB_HOST] [--web-port WEB_PORT]
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
- `--watch`: keep running, wait for new/moved entities, and rerun rules after each detected change
- `--web`: enable the HTTP API server
- `--web-log`: enable HTTP access logging for the API server (disabled by default)
- `--web-host`: bind host for the HTTP API server (default: `127.0.0.1`)
- `--web-port`: bind port for the HTTP API server (default: `8000`)
- `--log-level`: `DEBUG|INFO|WARNING|ERROR` (default: `INFO`)

When `--web` is enabled without `--watch`, Dirorch still runs the workflow immediately, but the process stays alive after that pass completes so the API remains available until shutdown.

## HTTP API

Enable the API server with:

```bash
dirorch ./workflow.yaml --root ./work --web
```

Or with an explicit bind address:

```bash
dirorch ./workflow.yaml --root ./work --web --web-log --web-host 0.0.0.0 --web-port 9000
```

Available endpoints:

- `GET /workflow`: workflow structure, phase order, states, transitions, and configured hooks
- `GET /status/workflow`: persisted runtime snapshot, entity counts, locks, and current execution activity
- `GET /status/entities`: entity list with phase/state, lock state, and processing flag
- `GET /entity/{id}`: entity metadata plus file contents
- `GET /entity/{id}/log`: current rendered entity transcript with `offset` and `next_offset`
- `GET /entity/{id}/log/events?from_offset=<n>`: SSE stream with `snapshot`, `append`, and `status` events
- `POST /entity`: create an entity
- `PUT /entity/{id}`: update entity contents and/or move it to another phase/state
- `PUT /entity/{id}/lock`: lock or unlock an entity
- `DELETE /entity/{id}`: delete an entity
- `GET|POST|PUT|DELETE /file/{path...}`: CRUD for other root-scoped UTF-8 text and JSON files

Locking behavior:

- Locked entities are excluded from workflow transitions until they are unlocked.
- Locks are persisted separately from runtime state in `${root}/.dirorch_locks.json`.
- The generic `/file` API cannot modify Dirorch-managed entity directories, `${root}/entity_logs/`, the runtime state file, or the lock file.

## Per-Entity Logs

Entity-scoped transition processing writes transcripts to `${root}/entity_logs/<entity>.log`.

The transcript includes:

- transition start, move, failure, and jump events
- command start, finish, and retry events
- raw `stdout` and `stderr` chunks exactly as observed, including ANSI/control sequences
- selector outcomes for dynamic `to` and `jump` hooks
- API-driven entity audit events such as create, update, manual move, lock/unlock, and delete

The transcript does not currently include global `init` hooks or completion hooks because those are not naturally scoped to one entity.

## Workflow YAML Reference

Top-level fields:

- `phases` (required): mapping of phase name -> phase definition
- `retries` (optional): non-negative integer, default `3`
- `env` or `environment` (optional): map of string env vars passed to hooks; values are Jinja2 templates rendered at startup
- `init` (optional): one-time startup hook before any phase runs

Phase fields:

- `states` (required): non-empty list of state names
- `mode` (optional): phase execution strategy
  - `transitions` (default): process each transition rule across all applicable entities
  - `parallel`: process each transition rule across all applicable entities, always running each transition batch concurrently
  - `entity`: process one entity through transitions until no transition applies, then next entity
- `transitions` (optional): list of transition definitions
- `completions` (optional): list of completion hook definitions
  - `completion` is also accepted as alias

Transition fields:

- `from` (required): source state
- `to` (required): either a destination state string or a hook object with `cmd` and optional `stdin`
- `cmd` (optional): shell command to run before move
- `stdin` (optional): text rendered and piped to the hook process stdin (requires `cmd`)
- `jump` (optional): either a target phase string or a hook object with `cmd` and optional `stdin`

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

Workflow `env` template context includes:

- other already-resolved workflow env vars
- generated `DIR_<PHASE>_<STATE>` variables

Workflow `env` templates do not include `INPUT_ENTITY`.

Workflow env templates and hook `cmd`/`stdin` templates can also use standard Jinja `{% include %}` and `{% import %}` statements for template files under `--root`.

- template names are resolved relative to `--root`
- Jinja auto-reload is enabled, so updated fragment files are picked up on later renders
- example: `{% include "templates/common-prompt.j2" %}`

## Hook Templates

If a hook defines `cmd` or `stdin`, Dirorch renders it with Jinja2 before running the command.
This applies to transition side-effect hooks, completion hooks, init hooks, and dynamic `to`/`jump` selector hooks.

Template context includes only Dirorch-defined variables:

- YAML `env`/`environment` values
- generated `DIR_<PHASE>_<STATE>` variables
- hook runtime variables like `INPUT_ENTITY` (for transition hooks)

External inherited process variables are not available in templates.

Template helpers:

- `read_file(path)` (alias: `include_file(path)`): reads UTF-8 file content and inserts it into the rendered stdin text.
  - relative paths resolve from `--root`
  - you can pass path variables, e.g. `{{ read_file(FILE_TO_INCLUDE) }}`
- `read_json(path)`: reads and parses a JSON file on demand, returning normal Jinja-accessible objects.
  - relative paths resolve from `--root`
  - this is lazy: Dirorch only reads/parses JSON if the template calls `read_json(...)`
  - useful for entity-backed JSON workflows, e.g. `{{ read_json(INPUT_ENTITY).task.name }}` or `{{ read_json(INPUT_ENTITY)["task"]["priority"] }}`

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
- Transition side-effect `cmd` runs first.
- Dirorch resolves `to` next:
  - string `to` uses the configured state directly
  - object `to` runs a selector command and reads the chosen state name from the temporary pipe named by `DIRORCH_SELECTOR_PIPE`
- If a destination was selected, Dirorch resolves `jump` after that:
  - string `jump` uses the configured phase directly
  - object `jump` runs a selector command and reads the chosen phase name from the temporary pipe named by `DIRORCH_SELECTOR_PIPE`
- Move happens only after destination and jump validation succeeds.
- If a selector command writes nothing to `DIRORCH_SELECTOR_PIPE`, the selection is treated as empty:
  - empty dynamic `to` means no move and no failure
  - empty dynamic `jump` means move normally and skip the jump
- Selector stdout/stderr do not affect target selection.
- If transition `cmd` or a dynamic selector fails after retries, entity moves to `_failed`.
- Unknown non-empty dynamic state or phase names also move the entity to `_failed`.
- On successful transition with `jump`, target phase is run to fixpoint, then execution returns to the current phase.
- `init`, completion hooks, transition side-effects, and dynamic selectors all use the same retry policy (`retries + 1` total attempts).

Phase `mode` behavior:

- `transitions` mode:
  - Dirorch applies each transition rule to all matching entities, looping until fixpoint.
  - Grouped concurrency (`NN-name.ext`) is enabled in this mode.
- `parallel` mode:
  - Dirorch applies each transition rule to all matching entities, looping until fixpoint.
  - Every eligible entity for the active transition runs in the same concurrent batch, regardless of filename.
- `entity` mode:
  - Dirorch picks entities by filename, moves each entity through transitions until it comes to rest, then picks the next entity.
  - Jumps still run immediately after a successful transition with `jump`.
  - Processing is sequential per entity (no grouped concurrent transition execution).

Concurrency rule:

- Files named like `NN-name.ext` (numeric prefix + `-`) are grouped by `NN`.
- Entities in the same group may run concurrently.
- Other entities are processed sequentially.

## Runtime State and Resume

Dirorch stores runtime execution state in:

- `${root}/${state-file}`
- default: `.dirorch_runtime.json`

Example:

```json
{
  "schema_version": 2,
  "current_phase": "tasks",
  "jump_stack": [],
  "entity_cursor": null
}
```

Fields:

- `schema_version`: currently must be `2`.
- `current_phase`: phase to run next.
- `jump_stack`: active jump return stack, with each frame containing:
  - `source_phase`
  - `target_phase`
  - `source_entity_name` (optional entity cursor to restore when returning)
- `entity_cursor`: active entity-mode cursor (`{phase, entity_name}`) or `null`.

On restart, Dirorch resumes using this full context so jump returns and `mode: entity`
processing continue safely.

State schema is strict: legacy state files are rejected and must be regenerated.

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

## Dynamic Targets

`to` and `jump` can also be computed at runtime with hook objects:

```yaml
phases:
  tasks:
    states: [new, review, done]
    transitions:
      - from: new
        cmd: >
          ./prepare-task "$INPUT_ENTITY"
        to:
          cmd: >
            if grep -q review "$INPUT_ENTITY"; then printf '%s\n' review > "$DIRORCH_SELECTOR_PIPE"; else printf '%s\n' done > "$DIRORCH_SELECTOR_PIPE"; fi
        jump:
          cmd: >
            if grep -q audit "$INPUT_ENTITY"; then printf '%s\n' audit > "$DIRORCH_SELECTOR_PIPE"; fi
  audit:
    states: [new, done]
```

Dynamic selector notes:

- Write the selected state or phase name to the path in `DIRORCH_SELECTOR_PIPE`, for example `printf '%s\n' done > "$DIRORCH_SELECTOR_PIPE"`.
- Selector `cmd` and `stdin` fields are rendered with the same Jinja2 template context as other hooks, including `DIRORCH_SELECTOR_PIPE` and `env.DIRORCH_SELECTOR_PIPE`.
- A fresh temporary named pipe is created for each selector attempt and cleaned up after the attempt finishes.
- Dirorch strips surrounding whitespace and uses the first output line from `DIRORCH_SELECTOR_PIPE`.
- Empty pipe output means "no selection".

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
