import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from main import CliOptions, FAILED_STATE, WorkflowError, load_workflow, run


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _run_workflow(
    workflow: Path, root: Path, retries: int | None = None, log_level: str = "ERROR"
) -> None:
    options = CliOptions(
        workflow=workflow,
        root=root,
        retries_override=retries,
        state_file=".dirorch_runtime.json",
        log_level=log_level,
    )
    asyncio.run(run(options))


def test_load_workflow_parses_env_retries_and_init(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
retries: 5
env:
  FOO: bar
init:
  cmd: "echo init"
phases:
  tasks:
    mode: entity
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )

    config = load_workflow(workflow)

    assert config.retries == 5
    assert config.environment == {"FOO": "bar"}
    assert config.phase_order == ("tasks",)
    assert config.phases[0].states == ("new", "done")
    assert config.phases[0].mode == "entity"
    assert config.init is not None
    assert config.init.cmd == "echo init"


@pytest.mark.parametrize(
    "yaml_text, expected",
    [
        ("phases: {}\n", "non-empty 'phases'"),
        (
            """
phases:
  p:
    states: [new]
    transitions:
      - from: missing
        to: new
""",
            "source 'missing'",
        ),
        (
            """
phases:
  p:
    states: [new]
    transitions:
      - from: new
        to: new
        jump: nowhere
""",
            "jump target 'nowhere'",
        ),
        (
            """
init: []
phases:
  p:
    states: [new]
""",
            "'init' must be a string or a mapping with 'cmd'",
        ),
        (
            """
phases:
  p:
    mode: per_entity
    states: [new]
""",
            "invalid mode 'per_entity'",
        ),
        (
            """
init:
  cmd: "cat"
  stdin: 1
phases:
  p:
    states: [new]
""",
            "hook has invalid 'stdin'",
        ),
        (
            """
phases:
  p:
    states: [new, done]
    transitions:
      - from: new
        to: done
        stdin: "x"
""",
            "requires 'cmd' when 'stdin' is set",
        ),
    ],
)
def test_load_workflow_rejects_invalid_definitions(
    tmp_path: Path, yaml_text: str, expected: str
) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(workflow, yaml_text)

    with pytest.raises(WorkflowError, match=expected):
        load_workflow(workflow)


def test_run_simple_transition_moves_entities_and_stops(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")
    _write(new_dir / "b.txt", "b")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    assert (tmp_path / "tasks" / "done" / "b.txt").exists()
    assert not (tmp_path / "tasks" / "new" / "a.txt").exists()

    state = json.loads((tmp_path / ".dirorch_runtime.json").read_text(encoding="utf-8"))
    assert state["current_phase"] == "tasks"


def test_workflow_env_templates_can_reference_dir_variables(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "observed.txt"
    _write(
        workflow,
        f"""
env:
  TARGET_PATH: "{{{{ DIR_TASKS_DONE }}}}/target.txt"
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          printf '%s' "$TARGET_PATH" > "{observed}"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "x.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert observed.read_text(encoding="utf-8") == str(
        (tmp_path / "tasks" / "done" / "target.txt").resolve()
    )


def test_workflow_env_templates_can_include_files(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "observed.txt"
    payload = tmp_path / "payload.txt"
    _write(payload, "file-payload\n")
    _write(
        workflow,
        f"""
env:
  PAYLOAD_PATH: "{payload}"
  PAYLOAD: "{{{{ include_file(PAYLOAD_PATH).strip() }}}}"
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          printf '%s' "$PAYLOAD" > "{observed}"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "x.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert observed.read_text(encoding="utf-8") == "file-payload"


def test_workflow_env_templates_cannot_reference_input_entity(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
env:
  BAD: "{{ INPUT_ENTITY }}"
phases:
  tasks:
    states: [new]
""",
    )

    with pytest.raises(WorkflowError, match="INPUT_ENTITY"):
        _run_workflow(workflow, tmp_path)


def test_init_hook_runs_once_before_any_phase(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
    _write(
        workflow,
        f"""
init: >
  echo init >> {trace_file};
  echo seeded > "$DIR_TASKS_NEW/from-init.txt"
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "from-init.txt").exists()
    trace_lines = trace_file.read_text(encoding="utf-8").splitlines()
    assert trace_lines == ["init"]


def test_transition_mode_processes_transitions_in_batches(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
    _write(
        workflow,
        f"""
phases:
  tasks:
    states: [new, mid, done]
    transitions:
      - from: new
        to: mid
        cmd: >
          echo "first-$(basename "$INPUT_ENTITY")" >> {trace_file}
      - from: mid
        to: done
        cmd: >
          echo "second-$(basename "$INPUT_ENTITY")" >> {trace_file}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")
    _write(new_dir / "b.txt", "b")

    _run_workflow(workflow, tmp_path)

    assert trace_file.read_text(encoding="utf-8").splitlines() == [
        "first-a.txt",
        "first-b.txt",
        "second-a.txt",
        "second-b.txt",
    ]


def test_entity_mode_processes_each_entity_until_rest(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
    _write(
        workflow,
        f"""
phases:
  tasks:
    mode: entity
    states: [new, mid, done]
    transitions:
      - from: new
        to: mid
        cmd: >
          echo "first-$(basename "$INPUT_ENTITY")" >> {trace_file}
      - from: mid
        to: done
        cmd: >
          echo "second-$(basename "$INPUT_ENTITY")" >> {trace_file}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")
    _write(new_dir / "b.txt", "b")

    _run_workflow(workflow, tmp_path)

    assert trace_file.read_text(encoding="utf-8").splitlines() == [
        "first-a.txt",
        "second-a.txt",
        "first-b.txt",
        "second-b.txt",
    ]


def test_entity_mode_jump_runs_target_phase_to_fixpoint(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    mode: entity
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          cp "$INPUT_ENTITY" "$DIR_SUBTASKS_NEW/sub-$(basename "$INPUT_ENTITY")"
        jump: subtasks
  subtasks:
    states: [new, complete]
    transitions:
      - from: new
        to: complete
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    assert (tmp_path / "subtasks" / "complete" / "sub-a.txt").exists()


def test_init_hook_retries_then_succeeds(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    marker = tmp_path / "init.marker"
    success_out = tmp_path / "init.success"
    _write(
        workflow,
        f"""
retries: 1
init: >
  if [ -f {marker} ]; then echo ok > {success_out}; else touch {marker}; exit 1; fi
phases:
  tasks:
    states: [new]
""",
    )

    _run_workflow(workflow, tmp_path)

    assert success_out.exists()


def test_init_hook_failure_aborts_run(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
retries: 0
init: "exit 9"
phases:
  tasks:
    states: [new]
""",
    )

    with pytest.raises(WorkflowError, match="init hook failed after retries"):
        _run_workflow(workflow, tmp_path)


def test_transition_hook_gets_input_and_dir_env(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "observed.txt"
    _write(
        workflow,
        f"""
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          printf '%s|%s' "$INPUT_ENTITY" "$DIR_TASKS_DONE" > {observed}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "x.txt"
    _write(entity, "x")

    _run_workflow(workflow, tmp_path)

    content = observed.read_text(encoding="utf-8")
    input_entity, done_dir = content.split("|", maxsplit=1)
    assert Path(input_entity) == entity.resolve()
    assert Path(done_dir) == (tmp_path / "tasks" / "done").resolve()


def test_transition_hook_stdin_template_renders_vars_and_file(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    include_path = tmp_path / "payload.txt"
    observed = tmp_path / "rendered.txt"
    _write(include_path, "from-file\n")
    _write(
        workflow,
        f"""
env:
  GREETING: hello
  FILE_TO_INCLUDE: "{include_path}"
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          cat > "{observed}"
        stdin: |
          greeting={{{{ GREETING }}}}
          input={{{{ INPUT_ENTITY }}}}
          payload={{{{ include_file(FILE_TO_INCLUDE).strip() }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "x.txt"
    _write(entity, "x")

    _run_workflow(workflow, tmp_path)

    rendered = observed.read_text(encoding="utf-8").splitlines()
    assert rendered[0] == "greeting=hello"
    assert rendered[1] == f"input={entity.resolve()}"
    assert rendered[2] == "payload=from-file"


def test_stdin_template_cannot_access_external_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "rendered.txt"
    monkeypatch.setenv("EXTERNAL_ONLY", "secret")
    _write(
        workflow,
        f"""
retries: 0
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          cat > "{observed}"
        stdin: |
          {{{{ EXTERNAL_ONLY }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "x.txt"
    _write(entity, "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / FAILED_STATE / "x.txt").exists()
    assert not observed.exists()


def test_failure_retries_then_moves_to_failed(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
retries: 1
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: "exit 2"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "bad.txt", "bad")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / FAILED_STATE / "bad.txt").exists()
    assert not (tmp_path / "tasks" / "done" / "bad.txt").exists()


def test_completion_hook_retries_then_succeeds(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    attempt_marker = tmp_path / "completion_attempt"
    completion_out = tmp_path / "completion_ok"
    _write(
        workflow,
        f"""
retries: 1
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
    completions:
      - cmd: "if [ -f {attempt_marker} ]; then echo ok > {completion_out}; else touch {attempt_marker}; exit 1; fi"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "t.txt", "task")

    _run_workflow(workflow, tmp_path)

    assert completion_out.exists()


def test_completion_hook_supports_templated_stdin(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    completion_out = tmp_path / "completion.txt"
    _write(
        workflow,
        f"""
env:
  MESSAGE: hello-completion
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
    completions:
      - cmd: >
          cat > "{completion_out}"
        stdin: |
          {{{{ MESSAGE }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "t.txt", "task")

    _run_workflow(workflow, tmp_path)

    assert completion_out.read_text(encoding="utf-8").strip() == "hello-completion"


def test_jump_runs_target_phase_to_fixpoint(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          cp "$INPUT_ENTITY" "$DIR_SUBTASKS_NEW/sub-$(basename "$INPUT_ENTITY")"
        jump: subtasks
  subtasks:
    states: [new, complete]
    transitions:
      - from: new
        to: complete
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    assert (tmp_path / "subtasks" / "complete" / "sub-a.txt").exists()


def test_resume_from_state_file_starts_at_saved_phase(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
  subtasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )
    subtasks_new = tmp_path / "subtasks" / "new"
    subtasks_new.mkdir(parents=True)
    _write(subtasks_new / "s.txt", "s")

    _write(tmp_path / ".dirorch_runtime.json", '{"current_phase":"subtasks"}')

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "subtasks" / "done" / "s.txt").exists()


def test_grouped_numeric_prefix_entities_run_concurrently(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        f"""
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
        cmd: >
          sleep 0.2; echo $(basename "$INPUT_ENTITY") >> {tmp_path / 'seen.txt'}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    for name in ["01-a.txt", "01-b.txt", "02-c.txt"]:
        _write(new_dir / name, name)

    started = time.monotonic()
    _run_workflow(workflow, tmp_path)
    elapsed = time.monotonic() - started

    # Sequential would be around 0.6s; grouped concurrency should stay under this bound.
    assert elapsed < 0.55


def test_cli_invocation_works_end_to_end(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "cli.txt", "x")

    result = subprocess.run(
        [
            sys.executable,
            "main.py",
            str(workflow),
            "--root",
            str(tmp_path),
            "--log-level",
            "ERROR",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "tasks" / "done" / "cli.txt").exists()


def test_run_supports_named_global_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    workflow_dir = home / ".config" / "dirorch" / "workflows"
    workflow_dir.mkdir(parents=True)
    _write(
        workflow_dir / "global.yml",
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )
    root = tmp_path / "root"
    new_dir = root / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "named.txt", "x")
    monkeypatch.setenv("HOME", str(home))

    options = CliOptions(
        workflow=Path("global"),
        root=root,
        retries_override=None,
        state_file=".dirorch_runtime.json",
        log_level="ERROR",
    )

    asyncio.run(run(options))

    assert (root / "tasks" / "done" / "named.txt").exists()


def test_run_named_global_workflow_prefers_xdg_config_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    xdg = tmp_path / "xdg"
    xdg_workflow_dir = xdg / "dirorch" / "workflows"
    xdg_workflow_dir.mkdir(parents=True)
    _write(
        xdg_workflow_dir / "global.yml",
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )
    root = tmp_path / "root-xdg"
    new_dir = root / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "named.txt", "x")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_CONFIG_DIR", str(xdg))

    options = CliOptions(
        workflow=Path("global"),
        root=root,
        retries_override=None,
        state_file=".dirorch_runtime.json",
        log_level="ERROR",
    )

    asyncio.run(run(options))

    assert (root / "tasks" / "done" / "named.txt").exists()
