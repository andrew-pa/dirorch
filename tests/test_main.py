import asyncio
import json
import os
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

import pytest

from dirorch.template_engine import TemplateRenderer
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


async def _wait_for_path(path: Path, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {path}")


def _state_snapshot(
    current_phase: str,
    jump_stack: list[dict[str, object]] | None = None,
    entity_cursor: dict[str, str] | None = None,
) -> str:
    return json.dumps(
        {
            "schema_version": 2,
            "current_phase": current_phase,
            "jump_stack": jump_stack or [],
            "entity_cursor": entity_cursor,
        }
    )


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


def test_load_workflow_accepts_dynamic_transition_targets(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, review, done]
    transitions:
      - from: new
        to:
          cmd: "printf '%s\\n' review > \\\"$DIRORCH_SELECTOR_PIPE\\\""
          stdin: "{{ INPUT_ENTITY }}"
        jump:
          cmd: "printf '%s\\n' audit > \\\"$DIRORCH_SELECTOR_PIPE\\\""
  audit:
    states: [new, done]
""",
    )

    config = load_workflow(workflow)
    transition = config.phases[0].transitions[0]

    assert transition.destination.constant is None
    assert transition.destination.hook is not None
    assert transition.destination.hook.cmd == "printf '%s\n' review > \"$DIRORCH_SELECTOR_PIPE\""
    assert transition.destination.hook.stdin == "{{ INPUT_ENTITY }}"
    assert transition.jump_target is not None
    assert transition.jump_target.hook is not None
    assert transition.jump_target.hook.cmd == "printf '%s\n' audit > \"$DIRORCH_SELECTOR_PIPE\""


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
        (
            """
phases:
  p:
    states: [new, done]
    transitions:
      - from: new
""",
            "missing valid 'to'",
        ),
        (
            """
phases:
  p:
    states: [new, done]
    transitions:
      - from: new
        to:
          stdin: hello
""",
            "to selector has invalid 'cmd'",
        ),
        (
            """
phases:
  p:
    states: [new, done]
    transitions:
      - from: new
        to: done
        jump:
          stdin: hello
""",
            "jump selector has invalid 'cmd'",
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


def test_load_workflow_accepts_parallel_mode(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    mode: parallel
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )

    config = load_workflow(workflow)

    assert config.phases[0].mode == "parallel"


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
    assert state["schema_version"] == 2
    assert state["current_phase"] == "tasks"
    assert state["jump_stack"] == []
    assert state["entity_cursor"] is None


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


def test_workflow_env_templates_support_jinja_include_from_root(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "observed.txt"
    fragment = tmp_path / "templates" / "target_path.j2"
    fragment.parent.mkdir(parents=True)
    _write(fragment, "{{ DIR_TASKS_DONE }}/target.txt")
    _write(
        workflow,
        f"""
env:
  TARGET_PATH: "{{% include 'templates/target_path.j2' %}}"
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


def test_transition_hook_cmd_template_renders_vars_and_file(tmp_path: Path) -> None:
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
          printf '%s\\n%s\\n%s\\n' "{{{{ GREETING }}}}" "{{{{ INPUT_ENTITY }}}}" "{{{{ include_file(FILE_TO_INCLUDE).strip() }}}}" > "{observed}"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "x.txt"
    _write(entity, "x")

    _run_workflow(workflow, tmp_path)

    rendered = observed.read_text(encoding="utf-8").splitlines()
    assert rendered[0] == "hello"
    assert rendered[1] == str(entity.resolve())
    assert rendered[2] == "from-file"


def test_transition_hook_stdin_template_can_read_input_entity_json(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "rendered.txt"
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
          cat > "{observed}"
        stdin: |
          name={{{{ read_json(INPUT_ENTITY).task.name }}}}
          priority={{{{ read_json(INPUT_ENTITY)["task"]["priority"] }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "task.json"
    _write(entity, '{"task":{"name":"ship","priority":3}}')

    _run_workflow(workflow, tmp_path)

    assert observed.read_text(encoding="utf-8").splitlines() == [
        "name=ship",
        "priority=3",
    ]


def test_transition_hook_stdin_template_supports_jinja_import_from_root(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "rendered.txt"
    macro_file = tmp_path / "templates" / "formatters.j2"
    macro_file.parent.mkdir(parents=True)
    _write(
        macro_file,
        """
{% macro render_entity(name) -%}
entity={{ name.upper() }}
{%- endmacro %}
""",
    )
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
          cat > "{observed}"
        stdin: |
          {{% from 'templates/formatters.j2' import render_entity %}}
          {{{{ render_entity(read_json(INPUT_ENTITY).task.name) }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "task.json"
    _write(entity, '{"task":{"name":"ship"}}')

    _run_workflow(workflow, tmp_path)

    assert observed.read_text(encoding="utf-8").strip() == "entity=SHIP"


def test_transition_hook_stdin_template_reads_json_lazily(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "rendered.txt"
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
          cat > "{observed}"
        stdin: |
          entity={{{{ INPUT_ENTITY }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "task.txt"
    _write(entity, "not-json")

    _run_workflow(workflow, tmp_path)

    assert observed.read_text(encoding="utf-8").strip() == f"entity={entity.resolve()}"
    assert (tmp_path / "tasks" / "done" / "task.txt").exists()


def test_dynamic_destination_runs_after_side_effect(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    marker = tmp_path / "marker.txt"
    _write(
        workflow,
        f"""
phases:
  tasks:
    states: [new, review, done]
    transitions:
      - from: new
        cmd: >
          printf '%s' ready > "{marker}"
        to:
          cmd: >
            if [ "$(cat "{marker}")" = "ready" ]; then printf '%s\\n' review > "$DIRORCH_SELECTOR_PIPE"; else exit 7; fi
      - from: review
        to: done
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "item.txt").exists()


def test_dynamic_destination_cmd_template_can_read_input_entity_json(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, review, done]
    transitions:
      - from: new
        to:
          cmd: >
            printf '%s\\n' "{{ read_json(INPUT_ENTITY).next_state }}" > "$DIRORCH_SELECTOR_PIPE"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.json", '{"next_state":"review"}')

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "review" / "item.json").exists()


def test_dynamic_destination_uses_selector_pipe_not_stdout(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to:
          cmd: >
            printf '%s\\n' ignored;
            printf '%s\\n' done > "$DIRORCH_SELECTOR_PIPE";
            printf '%s\\n' ignored >&2
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "item.txt").exists()


def test_dynamic_destination_selector_pipe_is_in_env_and_templates_and_cleaned_up(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "selector-pipe.txt"
    _write(
        workflow,
        f"""
phases:
  tasks:
    states: [new, review]
    transitions:
      - from: new
        to:
          cmd: >
            printf '%s\\n' "{{{{ DIRORCH_SELECTOR_PIPE }}}}" > "{observed}";
            printf '%s\\n' review > "$DIRORCH_SELECTOR_PIPE"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    selector_pipe = Path(observed.read_text(encoding="utf-8").strip())

    assert (tmp_path / "tasks" / "review" / "item.txt").exists()
    assert not selector_pipe.exists()
    assert not selector_pipe.parent.exists()


def test_dynamic_destination_empty_output_leaves_entity_in_place(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to:
          cmd: "true"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "new" / "item.txt").exists()
    assert not (tmp_path / "tasks" / "done" / "item.txt").exists()
    assert not (tmp_path / "tasks" / FAILED_STATE / "item.txt").exists()


def test_dynamic_destination_invalid_output_moves_to_failed(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to:
          cmd: 'printf ''%s\n'' nowhere > "$DIRORCH_SELECTOR_PIPE"'
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / FAILED_STATE / "item.txt").exists()


def test_dynamic_destination_retries_then_succeeds(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    marker = tmp_path / "selector.marker"
    _write(
        workflow,
        f"""
retries: 1
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to:
          cmd: >
            if [ -f "{marker}" ]; then printf '%s\\n' done > "$DIRORCH_SELECTOR_PIPE"; else touch "{marker}"; exit 9; fi
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "item.txt", "x")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "item.txt").exists()


def test_template_renderer_auto_reloads_included_fragments(tmp_path: Path) -> None:
    fragment = tmp_path / "templates" / "snippet.j2"
    fragment.parent.mkdir(parents=True)
    _write(fragment, "version-one")
    renderer = TemplateRenderer(tmp_path)
    template = "{% include 'templates/snippet.j2' %}"

    assert renderer.render(template, {}) == "version-one"

    previous_mtime = fragment.stat().st_mtime_ns
    while True:
        _write(fragment, "version-two")
        if fragment.stat().st_mtime_ns != previous_mtime:
            break
        time.sleep(0.01)

    assert renderer.render(template, {}) == "version-two"


def test_transition_hook_stdin_template_invalid_input_entity_json_fails(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    observed = tmp_path / "rendered.txt"
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
          name={{{{ read_json(INPUT_ENTITY).task.name }}}}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    entity = new_dir / "task.txt"
    _write(entity, "not-json")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / FAILED_STATE / "task.txt").exists()
    assert not observed.exists()


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


def test_dynamic_jump_runs_target_phase_to_fixpoint(tmp_path: Path) -> None:
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
        jump:
          cmd: 'printf ''%s\n'' subtasks > "$DIRORCH_SELECTOR_PIPE"'
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


def test_dynamic_jump_empty_output_skips_jump(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
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
          cp "$INPUT_ENTITY" "$DIR_SUBTASKS_NEW/sub-$(basename "$INPUT_ENTITY")"
        jump:
          cmd: "true"
    completions:
      - cmd: >
          echo tasks-complete >> "{trace_file}"
  subtasks:
    states: [new, complete]
    transitions:
      - from: new
        to: complete
        cmd: >
          echo subtasks-ran >> "{trace_file}"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    assert trace_file.read_text(encoding="utf-8").splitlines()[:2] == [
        "tasks-complete",
        "subtasks-ran",
    ]


def test_dynamic_jump_invalid_output_moves_to_failed(tmp_path: Path) -> None:
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
        jump:
          cmd: 'printf ''%s\n'' nowhere > "$DIRORCH_SELECTOR_PIPE"'
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / FAILED_STATE / "a.txt").exists()
    assert not (tmp_path / "tasks" / "done" / "a.txt").exists()


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

    _write(
        tmp_path / ".dirorch_runtime.json",
        _state_snapshot(current_phase="subtasks"),
    )

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "subtasks" / "done" / "s.txt").exists()


def test_runtime_state_rejects_legacy_schema(tmp_path: Path) -> None:
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
    _write(tmp_path / ".dirorch_runtime.json", '{"current_phase":"tasks"}')

    with pytest.raises(WorkflowError, match="expected fields"):
        _run_workflow(workflow, tmp_path)


def test_resume_unwinds_saved_jump_stack(tmp_path: Path) -> None:
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
    tasks_done = tmp_path / "tasks" / "done"
    subtasks_new = tmp_path / "subtasks" / "new"
    tasks_done.mkdir(parents=True)
    subtasks_new.mkdir(parents=True)
    _write(tasks_done / "a.txt", "a")
    _write(subtasks_new / "sub-a.txt", "a")

    _write(
        tmp_path / ".dirorch_runtime.json",
        _state_snapshot(
            current_phase="subtasks",
            jump_stack=[
                {
                    "source_phase": "tasks",
                    "target_phase": "subtasks",
                    "source_entity_name": None,
                }
            ],
        ),
    )

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "subtasks" / "complete" / "sub-a.txt").exists()


def test_entity_mode_resume_prioritizes_saved_cursor(tmp_path: Path) -> None:
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

    _write(
        tmp_path / ".dirorch_runtime.json",
        _state_snapshot(
            current_phase="tasks",
            entity_cursor={"phase": "tasks", "entity_name": "b.txt"},
        ),
    )

    _run_workflow(workflow, tmp_path)

    assert trace_file.read_text(encoding="utf-8").splitlines()[:2] == [
        "first-b.txt",
        "second-b.txt",
    ]


def test_entity_mode_resume_stale_cursor_auto_heals(tmp_path: Path) -> None:
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
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _write(
        tmp_path / ".dirorch_runtime.json",
        _state_snapshot(
            current_phase="tasks",
            entity_cursor={"phase": "tasks", "entity_name": "missing.txt"},
        ),
    )

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    state = json.loads((tmp_path / ".dirorch_runtime.json").read_text(encoding="utf-8"))
    assert state["entity_cursor"] is None


def test_entity_mode_dynamic_destination_continues_from_resolved_state(tmp_path: Path) -> None:
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
        to:
          cmd: 'printf ''%s\n'' mid > "$DIRORCH_SELECTOR_PIPE"'
      - from: mid
        to: done
        cmd: >
          echo "second-$(basename "$INPUT_ENTITY")" >> {trace_file}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "a.txt", "a")

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "done" / "a.txt").exists()
    assert trace_file.read_text(encoding="utf-8").splitlines() == ["second-a.txt"]


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


def test_parallel_mode_runs_each_transition_for_all_entities_concurrently(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
    _write(
        workflow,
        f"""
phases:
  tasks:
    mode: parallel
    states: [new, mid, done]
    transitions:
      - from: new
        to: mid
        cmd: >
          sleep 0.2; echo "first-$(basename "$INPUT_ENTITY")" >> {trace_file}
      - from: mid
        to: done
        cmd: >
          sleep 0.2; echo "second-$(basename "$INPUT_ENTITY")" >> {trace_file}
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    for name in ["a.txt", "b.txt", "c.txt"]:
        _write(new_dir / name, name)

    started = time.monotonic()
    _run_workflow(workflow, tmp_path)
    elapsed = time.monotonic() - started

    assert sorted(trace_file.read_text(encoding="utf-8").splitlines()) == [
        "first-a.txt",
        "first-b.txt",
        "first-c.txt",
        "second-a.txt",
        "second-b.txt",
        "second-c.txt",
    ]
    # Two transition barriers at ~0.2s each, plus process overhead.
    assert elapsed < 0.8


def test_parallel_dynamic_selectors_do_not_cross_talk(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    mode: parallel
    states: [new, left, right]
    transitions:
      - from: new
        to:
          cmd: >
            case "$(basename "$INPUT_ENTITY")" in
              left-*) printf '%s\\n' left > "$DIRORCH_SELECTOR_PIPE" ;;
              *) printf '%s\\n' right > "$DIRORCH_SELECTOR_PIPE" ;;
            esac
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    for name in ["left-a.txt", "right-b.txt", "right-c.txt"]:
        _write(new_dir / name, name)

    _run_workflow(workflow, tmp_path)

    assert (tmp_path / "tasks" / "left" / "left-a.txt").exists()
    assert (tmp_path / "tasks" / "right" / "right-b.txt").exists()
    assert (tmp_path / "tasks" / "right" / "right-c.txt").exists()


def test_watch_mode_reacts_to_new_entities_and_external_moves(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    trace_file = tmp_path / "trace.log"
    _write(
        workflow,
        f"""
init: >
  echo init >> "{trace_file}"
phases:
  tasks:
    states: [new, done]
    transitions:
      - from: new
        to: done
  review:
    states: [new, done]
    transitions:
      - from: new
        to: done
""",
    )

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            watch=True,
        )
        watch_task = asyncio.create_task(run(options))
        try:
            await asyncio.sleep(0.4)

            first_entity = tmp_path / "tasks" / "new" / "watch.txt"
            _write(first_entity, "watch")
            await _wait_for_path(tmp_path / "tasks" / "done" / "watch.txt")

            moved_entity = tmp_path / "tasks" / "done" / "watch.txt"
            target = tmp_path / "review" / "new" / "watch.txt"
            target.parent.mkdir(parents=True, exist_ok=True)
            moved_entity.rename(target)
            await _wait_for_path(tmp_path / "review" / "done" / "watch.txt")
        finally:
            watch_task.cancel()
            with suppress(asyncio.CancelledError):
                await watch_task

    asyncio.run(scenario())

    assert trace_file.read_text(encoding="utf-8").splitlines() == ["init"]


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


def test_entity_logs_capture_command_output_and_move_events(tmp_path: Path) -> None:
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
          python -c "import sys; sys.stdout.write('stdout-line\\n'); sys.stdout.flush();
          sys.stderr.write('\\x1b[31mstderr-line\\x1b[0m\\n'); sys.stderr.flush()"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "logged.txt", "payload")

    _run_workflow(workflow, tmp_path)

    log_path = tmp_path / "entity_logs" / "logged.txt.log"
    log_text = log_path.read_text(encoding="utf-8")

    assert "transition started tasks:new -> done" in log_text
    assert 'command started attempt=1 cmd="' in log_text
    assert "stdout-line\n" in log_text
    assert "\x1b[31mstderr-line\x1b[0m\n" in log_text
    assert "command finished exit=0" in log_text
    assert "moved to tasks/done" in log_text


def test_entity_logs_show_retries_and_final_success(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    retry_flag = tmp_path / "retry.flag"
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
        cmd: >
          if [ ! -f "{retry_flag}" ]; then
            printf 'first-fail\\n' >&2;
            touch "{retry_flag}";
            exit 1;
          fi;
          printf 'second-pass\\n'
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "retry.txt", "payload")

    _run_workflow(workflow, tmp_path)

    log_text = (tmp_path / "entity_logs" / "retry.txt.log").read_text(encoding="utf-8")

    assert "first-fail\n" in log_text
    assert 'retrying command next_attempt=2 reason="exit=1"' in log_text
    assert "second-pass\n" in log_text
    assert "command finished exit=0" in log_text
    assert "moved to tasks/done" in log_text


def test_entity_logs_capture_implicit_transition_and_selector_resolution(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, review, done]
    transitions:
      - from: new
        to:
          cmd: >
            printf 'selector-output\\n';
            printf 'review\\n' > "$DIRORCH_SELECTOR_PIPE"
""",
    )
    new_dir = tmp_path / "tasks" / "new"
    new_dir.mkdir(parents=True)
    _write(new_dir / "selector.txt", "payload")

    _run_workflow(workflow, tmp_path)

    log_text = (tmp_path / "entity_logs" / "selector.txt.log").read_text(encoding="utf-8")

    assert "implicit transition; no command configured" in log_text
    assert 'selector started kind=destination attempt=1 cmd="' in log_text
    assert "selector-output\n" in log_text
    assert "selector resolved destination=review" in log_text
    assert "moved to tasks/review" in log_text
