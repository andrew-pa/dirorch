import asyncio
import json
import socket
import sys
from contextlib import suppress
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web
from yarl import URL

from dirorch.cli import parse_args
from dirorch.constants import LOCKS_FILE_NAME, PAUSED_FILE_NAME
from dirorch.web.app import WebServer
from main import CliOptions, run


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_for_server(base_url: str, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{base_url}/workflow") as response:
                    if response.status == 200:
                        return
            except aiohttp.ClientError:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"Timed out waiting for {base_url}")
            await asyncio.sleep(0.05)


async def _wait_for_processing(
    session: aiohttp.ClientSession,
    base_url: str,
    entity_id: str,
    timeout: float = 5.0,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        async with session.get(f"{base_url}/status/entities") as response:
            payload = await response.json()
        for entity in payload["entities"]:
            if entity["id"] == entity_id and entity["processing"]:
                return entity
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Timed out waiting for active entity {entity_id}")
        await asyncio.sleep(0.05)


async def _wait_for_path(path: Path, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if path.exists():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Timed out waiting for {path}")
        await asyncio.sleep(0.05)


async def _wait_for_entity_state(
    session: aiohttp.ClientSession,
    base_url: str,
    entity_id: str,
    state: str,
    timeout: float = 5.0,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        async with session.get(f"{base_url}/status/entities") as response:
            payload = await response.json()
        for entity in payload["entities"]:
            if entity["id"] == entity_id and entity["state"] == state:
                return entity
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Timed out waiting for entity {entity_id} in state {state}")
        await asyncio.sleep(0.05)


async def _wait_for_entity(
    session: aiohttp.ClientSession,
    base_url: str,
    entity_id: str,
    timeout: float = 5.0,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        async with session.get(f"{base_url}/status/entities") as response:
            payload = await response.json()
        for entity in payload["entities"]:
            if entity["id"] == entity_id:
                return entity
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Timed out waiting for entity {entity_id}")
        await asyncio.sleep(0.05)


async def _read_sse_event(
    response: aiohttp.ClientResponse,
    timeout: float = 5.0,
) -> tuple[str | None, dict[str, object] | None]:
    lines: list[str] = []
    while True:
        line = await asyncio.wait_for(response.content.readline(), timeout=timeout)
        if line in {b"", b"\n", b"\r\n"}:
            break
        lines.append(line.decode("utf-8").rstrip("\r\n"))

    event_name: str | None = None
    data: str | None = None
    for line in lines:
        if line.startswith("event: "):
            event_name = line.removeprefix("event: ")
        elif line.startswith("data: "):
            data = line.removeprefix("data: ")
    payload = None if data is None else json.loads(data)
    return event_name, payload


def test_parse_args_supports_web_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dirorch",
            "workflow.yml",
            "--web",
            "--web-log",
            "--web-host",
            "0.0.0.0",
            "--web-port",
            "9001",
        ],
    )

    options = parse_args()

    assert options.web is True
    assert options.web_log is True
    assert options.web_host == "0.0.0.0"
    assert options.web_port == 9001


def test_web_server_disables_access_logging_by_default() -> None:
    async def scenario() -> None:
        runner = AsyncMock()
        site = AsyncMock()
        with (
            patch("dirorch.web.app.web.AppRunner", return_value=runner) as app_runner,
            patch("dirorch.web.app.web.TCPSite", return_value=site),
        ):
            server = WebServer(web.Application(), "127.0.0.1", 8000)
            await server.start()

        assert app_runner.call_args.kwargs["access_log"] is None

    asyncio.run(scenario())


def test_web_server_enables_access_logging_with_flag() -> None:
    async def scenario() -> None:
        runner = AsyncMock()
        site = AsyncMock()
        with (
            patch("dirorch.web.app.web.AppRunner", return_value=runner) as app_runner,
            patch("dirorch.web.app.web.TCPSite", return_value=site),
        ):
            server = WebServer(
                web.Application(),
                "127.0.0.1",
                8000,
                access_log_enabled=True,
            )
            await server.start()

        assert app_runner.call_args.kwargs["access_log"] is not None

    asyncio.run(scenario())


def test_web_api_supports_entity_and_file_crud(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
""",
    )
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                cors_origin = "http://localhost:5173"
                async with session.options(
                    f"{base_url}/entity",
                    headers={
                        "Origin": cors_origin,
                        "Access-Control-Request-Method": "POST",
                        "Access-Control-Request-Headers": "content-type",
                    },
                ) as response:
                    assert response.status == 204
                    assert response.headers["Access-Control-Allow-Origin"] == cors_origin
                    assert "POST" in response.headers["Access-Control-Allow-Methods"]
                    assert response.headers["Access-Control-Allow-Headers"] == "content-type"

                async with session.get(f"{base_url}/workflow") as response:
                    workflow_payload = await response.json()
                assert workflow_payload["phase_order"] == ["tasks"]
                assert workflow_payload["workflow_file"] == f"{tmp_path.name}/workflow.yaml"

                async with session.get(
                    f"{base_url}/workflow",
                    headers={"Origin": cors_origin},
                ) as response:
                    assert response.headers["Access-Control-Allow-Origin"] == cors_origin

                async with session.post(
                    f"{base_url}/entity",
                    json={
                        "id": "task.txt",
                        "phase": "tasks",
                        "state": "new",
                        "format": "text",
                        "content": "hello",
                    },
                ) as response:
                    entity_payload = await response.json()
                    assert response.status == 201
                assert entity_payload["id"] == "task.txt"
                assert entity_payload["state"] == "new"

                async with session.put(
                    f"{base_url}/entity/task.txt/lock",
                    json={"locked": True},
                ) as response:
                    locked_payload = await response.json()
                assert locked_payload["locked"] is True

                async with session.put(
                    f"{base_url}/entity/task.txt",
                    json={
                        "phase": "tasks",
                        "state": "done",
                        "format": "text",
                        "content": "updated",
                    },
                ) as response:
                    updated_payload = await response.json()
                assert updated_payload["state"] == "done"

                async with session.get(f"{base_url}/status/entities") as response:
                    status_payload = await response.json()
                assert status_payload["entities"] == [
                    {
                        "id": "task.txt",
                        "phase": "tasks",
                        "state": "done",
                        "locked": True,
                        "paused": False,
                        "processing": False,
                        "active_command": None,
                        "format": "text",
                    }
                ]

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["counts"]["tasks"]["done"] == 1
                assert workflow_status["paused_entities"] == 0
                assert workflow_status["execution"]["runner_state"] == "stopped"

                async with session.get(f"{base_url}/entity/task.txt/log") as response:
                    log_payload = await response.json()
                assert log_payload["exists"] is True
                assert "entity created phase=tasks state=new" in log_payload["text"]
                assert "entity locked" in log_payload["text"]
                assert "entity updated fields=content,format" in log_payload["text"]
                assert "manually moved tasks/new -> tasks/done" in log_payload["text"]

                async with session.post(
                    f"{base_url}/file/docs/info.json",
                    json={"format": "json", "content": json.dumps({"ok": True})},
                ) as response:
                    file_payload = await response.json()
                    assert response.status == 201
                assert file_payload["format"] == "json"
                assert file_payload["json"] == {"ok": True}

                async with session.get(f"{base_url}/file/docs/info.json") as response:
                    fetched_file_payload = await response.json()
                assert fetched_file_payload["content"] == json.dumps({"ok": True})

                async with session.post(
                    f"{base_url}/file/.dirorch_runtime.json",
                    json={"format": "text", "content": "nope"},
                ) as response:
                    reserved_payload = await response.json()
                assert response.status == 403
                assert reserved_payload["code"] == "forbidden"

                async with session.get(f"{base_url}/file/entity_logs/task.txt.log") as response:
                    reserved_logs_payload = await response.json()
                assert response.status == 403
                assert reserved_logs_payload["code"] == "forbidden"

                async with session.delete(f"{base_url}/entity/task.txt") as response:
                    assert response.status == 204
                async with session.delete(f"{base_url}/file/docs/info.json") as response:
                    assert response.status == 204
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_status_reports_active_processing_and_server_stays_up(tmp_path: Path) -> None:
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
          sleep 0.6
""",
    )
    _write(tmp_path / "tasks" / "new" / "active.txt", "payload")
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                entity_status = await _wait_for_processing(session, base_url, "active.txt")
                assert entity_status["processing"] is True
                active_command = entity_status["active_command"]
                assert active_command is not None
                assert active_command["attempt"] == 1
                assert "sleep 0.6" in active_command["command"]
                assert active_command["started_at"].endswith("Z")

                async with session.get(f"{base_url}/entity/active.txt") as response:
                    entity_payload = await response.json()
                assert entity_payload["active_command"] == active_command

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["execution"]["runner_state"] == "running"
                assert workflow_status["execution"]["activity"]["entity_ids"] == ["active.txt"]
                assert workflow_status["execution"]["activity"]["source_state"] == "new"
                assert workflow_status["execution"]["activity"]["destination_state"] == "done"

                await _wait_for_path(tmp_path / "tasks" / "done" / "active.txt")

                async with session.get(f"{base_url}/workflow") as response:
                    workflow_payload = await response.json()
                assert workflow_payload["phase_order"] == ["tasks"]

                async with session.get(f"{base_url}/status/workflow") as response:
                    final_status = await response.json()
                assert final_status["execution"]["runner_state"] == "stopped"
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_api_returns_expected_errors_and_json_shapes(tmp_path: Path) -> None:
    workflow = tmp_path / "workflow.yaml"
    _write(
        workflow,
        """
phases:
  tasks:
    states: [new, done]
""",
    )
    _write(tmp_path / "tasks" / "new" / "doc.json", json.dumps({"name": "example"}))
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base_url}/entity/doc.json") as response:
                    payload = await response.json()
                    assert response.status == 200
                assert payload["format"] == "json"
                assert payload["json"] == {"name": "example"}
                assert payload["content"] == json.dumps({"name": "example"})

                async with session.get(f"{base_url}/entity/missing.txt") as response:
                    missing_entity = await response.json()
                assert response.status == 404
                assert missing_entity["code"] == "not_found"

                async with session.post(
                    f"{base_url}/entity",
                    data="not-json",
                    headers={"Content-Type": "application/json"},
                ) as response:
                    invalid_body = await response.json()
                assert response.status == 400
                assert invalid_body["code"] == "validation_error"

                async with session.post(
                    f"{base_url}/entity",
                    json={
                        "id": "doc.json",
                        "phase": "tasks",
                        "state": "new",
                        "format": "json",
                        "content": json.dumps({"name": "duplicate"}),
                    },
                ) as response:
                    duplicate_entity = await response.json()
                assert response.status == 409
                assert duplicate_entity["code"] == "conflict"

                async with session.put(
                    f"{base_url}/entity/doc.json",
                    json={"format": "json", "content": "{bad json"},
                ) as response:
                    invalid_json = await response.json()
                assert response.status == 422
                assert invalid_json["code"] == "validation_error"

                async with session.put(
                    f"{base_url}/entity/doc.json/lock",
                    json={"locked": "yes"},
                ) as response:
                    invalid_lock = await response.json()
                assert response.status == 400
                assert invalid_lock["code"] == "validation_error"

                async with session.put(
                    f"{base_url}/entity/doc.json/pause",
                    json={"paused": "yes"},
                ) as response:
                    invalid_pause = await response.json()
                assert response.status == 400
                assert invalid_pause["code"] == "validation_error"

                async with session.get(f"{base_url}/file/missing.txt") as response:
                    missing_file = await response.json()
                assert response.status == 404
                assert missing_file["code"] == "not_found"

                async with session.post(
                    URL(f"{base_url}/file/%2E%2E/escape.txt", encoded=True),
                    json={"format": "text", "content": "bad"},
                ) as response:
                    forbidden_path = await response.json()
                assert response.status == 403
                assert forbidden_path["code"] == "forbidden"

                async with session.post(
                    f"{base_url}/file/tasks/outside.txt",
                    json={"format": "text", "content": "bad"},
                ) as response:
                    reserved_phase_dir = await response.json()
                assert response.status == 403
                assert reserved_phase_dir["code"] == "forbidden"

                async with session.post(
                    f"{base_url}/file/docs/bad.json",
                    json={"format": "json", "content": "{oops"},
                ) as response:
                    invalid_file_json = await response.json()
                assert response.status == 422
                assert invalid_file_json["code"] == "validation_error"
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_api_exposes_locking_that_blocks_workflow_processing(tmp_path: Path) -> None:
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
    _write(tmp_path / "tasks" / "new" / "locked.txt", "payload")
    _write(tmp_path / LOCKS_FILE_NAME, json.dumps(["locked.txt"]))
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
            watch=True,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                entity = await _wait_for_entity_state(session, base_url, "locked.txt", "new")
                assert entity["locked"] is True

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["counts"]["tasks"]["new"] == 1
                assert workflow_status["counts"]["tasks"]["done"] == 0
                assert workflow_status["locked_entities"] == 1
                assert workflow_status["paused_entities"] == 0
                assert workflow_status["execution"]["runner_state"] == "idle"

                await asyncio.sleep(0.3)
                assert (tmp_path / "tasks" / "new" / "locked.txt").exists()
                assert not (tmp_path / "tasks" / "done" / "locked.txt").exists()

                async with session.put(
                    f"{base_url}/entity/locked.txt/lock",
                    json={"locked": False},
                ) as response:
                    unlocked = await response.json()
                assert response.status == 200
                assert unlocked["locked"] is False

                _write(tmp_path / "tasks" / "new" / "trigger.txt", "go")
                await _wait_for_path(tmp_path / "tasks" / "done" / "locked.txt")
                await _wait_for_path(tmp_path / "tasks" / "done" / "trigger.txt")

                async with session.get(f"{base_url}/status/workflow") as response:
                    final_status = await response.json()
                assert final_status["counts"]["tasks"]["new"] == 0
                assert final_status["counts"]["tasks"]["done"] == 2
                assert final_status["locked_entities"] == 0
                assert final_status["paused_entities"] == 0
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_api_exposes_pause_that_blocks_workflow_processing(tmp_path: Path) -> None:
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
    _write(tmp_path / "tasks" / "new" / "paused.txt", "payload")
    _write(tmp_path / PAUSED_FILE_NAME, json.dumps(["paused.txt"]))
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
            watch=True,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                entity = await _wait_for_entity(session, base_url, "paused.txt")
                assert entity["paused"] is True
                assert entity["state"] == "new"

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["counts"]["tasks"]["new"] == 1
                assert workflow_status["counts"]["tasks"]["done"] == 0
                assert workflow_status["locked_entities"] == 0
                assert workflow_status["paused_entities"] == 1
                assert workflow_status["execution"]["runner_state"] == "idle"

                await asyncio.sleep(0.3)
                assert (tmp_path / "tasks" / "new" / "paused.txt").exists()
                assert not (tmp_path / "tasks" / "done" / "paused.txt").exists()

                async with session.put(
                    f"{base_url}/entity/paused.txt/pause",
                    json={"paused": False},
                ) as response:
                    resumed = await response.json()
                assert response.status == 200
                assert resumed["paused"] is False

                _write(tmp_path / "tasks" / "new" / "trigger.txt", "go")
                await _wait_for_path(tmp_path / "tasks" / "done" / "paused.txt")
                await _wait_for_path(tmp_path / "tasks" / "done" / "trigger.txt")

                async with session.get(f"{base_url}/status/workflow") as response:
                    final_status = await response.json()
                assert final_status["counts"]["tasks"]["new"] == 0
                assert final_status["counts"]["tasks"]["done"] == 2
                assert final_status["paused_entities"] == 0
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_api_pausing_running_entity_sigterms_shell_command(tmp_path: Path) -> None:
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
          python -c "import pathlib, signal, sys, time;
          marker = pathlib.Path('pause-term.txt');
          signal.signal(signal.SIGTERM, lambda *_args: (marker.write_text('terminated', encoding='utf-8'), sys.exit(0)));
          time.sleep(30)"
""",
    )
    _write(tmp_path / "tasks" / "new" / "running.txt", "payload")
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
            watch=True,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                await _wait_for_processing(session, base_url, "running.txt")

                async with session.put(
                    f"{base_url}/entity/running.txt/pause",
                    json={"paused": True},
                ) as response:
                    paused = await response.json()
                assert response.status == 200
                assert paused["paused"] is True

                await _wait_for_path(tmp_path / "pause-term.txt")

                deadline = asyncio.get_running_loop().time() + 5.0
                while True:
                    entity = await _wait_for_entity(session, base_url, "running.txt")
                    if not entity["processing"]:
                        break
                    if asyncio.get_running_loop().time() >= deadline:
                        raise AssertionError("Timed out waiting for running.txt to stop processing")
                    await asyncio.sleep(0.05)

                assert entity["paused"] is True
                assert entity["state"] == "new"
                assert (tmp_path / "tasks" / "new" / "running.txt").exists()
                assert not (tmp_path / "tasks" / "done" / "running.txt").exists()
                assert not (tmp_path / "tasks" / "_failed" / "running.txt").exists()

                async with session.get(f"{base_url}/entity/running.txt/log") as response:
                    log_payload = await response.json()
                assert "entity paused" in log_payload["text"]
                assert 'command terminated reason="entity paused"' in log_payload["text"]
                assert "transition paused" in log_payload["text"]
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_api_workflow_pause_pauses_running_entities_and_sigterms_hooks(tmp_path: Path) -> None:
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
          python -c "import pathlib, signal, sys, time;
          entity = pathlib.Path(sys.argv[1]).name;
          sys.exit(0) if entity == 'waiting.txt' else None;
          marker = pathlib.Path('global-pause-term.txt');
          signal.signal(signal.SIGTERM, lambda *_args: (marker.write_text('terminated', encoding='utf-8'), sys.exit(0)));
          time.sleep(30)" "$INPUT_ENTITY"
""",
    )
    _write(tmp_path / "tasks" / "new" / "running.txt", "payload")
    _write(tmp_path / "tasks" / "new" / "waiting.txt", "payload")
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
            watch=True,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                await _wait_for_processing(session, base_url, "running.txt")

                async with session.post(f"{base_url}/workflow/pause") as response:
                    paused = await response.json()
                assert response.status == 200
                assert paused["workflow_pause_state"] == "paused"
                assert paused["paused_entities"] == 1

                await _wait_for_path(tmp_path / "global-pause-term.txt")

                deadline = asyncio.get_running_loop().time() + 5.0
                while True:
                    running = await _wait_for_entity(session, base_url, "running.txt")
                    if not running["processing"]:
                        break
                    if asyncio.get_running_loop().time() >= deadline:
                        raise AssertionError("Timed out waiting for running.txt to stop")
                    await asyncio.sleep(0.05)

                waiting = await _wait_for_entity(session, base_url, "waiting.txt")
                assert running["paused"] is True
                assert waiting["paused"] is False
                assert running["state"] == "new"
                assert waiting["state"] == "new"
                assert not (tmp_path / "tasks" / "done" / "running.txt").exists()
                assert not (tmp_path / "tasks" / "done" / "waiting.txt").exists()

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["workflow_pause_state"] == "paused"
                assert workflow_status["paused_entities"] == 1

                async with session.post(f"{base_url}/workflow/resume") as response:
                    resumed = await response.json()
                assert response.status == 200
                assert resumed["workflow_pause_state"] == "running"
                assert resumed["paused_entities"] == 1

                await _wait_for_path(tmp_path / "tasks" / "done" / "waiting.txt")
                running = await _wait_for_entity(session, base_url, "running.txt")
                assert running["paused"] is True
                assert (tmp_path / "tasks" / "new" / "running.txt").exists()
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())


def test_web_log_endpoints_stream_live_entity_output(tmp_path: Path) -> None:
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
          python -c "import sys,time; sys.stdout.write('start\\n'); sys.stdout.flush();
          time.sleep(0.8); sys.stdout.write('end\\n'); sys.stdout.flush()"
""",
    )
    _write(tmp_path / "tasks" / "new" / "stream.txt", "payload")
    port = _free_port()

    async def scenario() -> None:
        options = CliOptions(
            workflow=workflow,
            root=tmp_path,
            retries_override=None,
            state_file=".dirorch_runtime.json",
            log_level="ERROR",
            web=True,
            web_host="127.0.0.1",
            web_port=port,
        )
        base_url = f"http://127.0.0.1:{port}"
        server_task = asyncio.create_task(run(options))
        try:
            await _wait_for_server(base_url)
            async with aiohttp.ClientSession() as session:
                await _wait_for_processing(session, base_url, "stream.txt")

                async with session.get(f"{base_url}/entity/stream.txt/log") as response:
                    initial_log = await response.json()
                assert initial_log["exists"] is True
                assert "transition started tasks:new -> done" in initial_log["text"]

                async with session.get(
                    f"{base_url}/entity/stream.txt/log/events?from_offset={initial_log['next_offset']}",
                    headers={"Origin": "http://localhost:5173"},
                ) as response:
                    assert (
                        response.headers["Access-Control-Allow-Origin"]
                        == "http://localhost:5173"
                    )
                    snapshot_event, snapshot_payload = await _read_sse_event(response)
                    assert snapshot_event == "snapshot"
                    assert snapshot_payload is not None

                    while True:
                        event_name, payload = await _read_sse_event(response)
                        if event_name != "append":
                            continue
                        assert payload is not None
                        text = payload["text"]
                        assert isinstance(text, str)
                        if "end\n" in text:
                            break

                await _wait_for_path(tmp_path / "tasks" / "done" / "stream.txt")
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())
