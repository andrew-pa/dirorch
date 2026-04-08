import asyncio
import json
import socket
import sys
from contextlib import suppress
from pathlib import Path

import aiohttp
from yarl import URL

from dirorch.cli import parse_args
from dirorch.constants import LOCKS_FILE_NAME
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


def test_parse_args_supports_web_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dirorch",
            "workflow.yml",
            "--web",
            "--web-host",
            "0.0.0.0",
            "--web-port",
            "9001",
        ],
    )

    options = parse_args()

    assert options.web is True
    assert options.web_host == "0.0.0.0"
    assert options.web_port == 9001


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
                async with session.get(f"{base_url}/workflow") as response:
                    workflow_payload = await response.json()
                assert workflow_payload["phase_order"] == ["tasks"]

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
                        "processing": False,
                        "format": "text",
                    }
                ]

                async with session.get(f"{base_url}/status/workflow") as response:
                    workflow_status = await response.json()
                assert workflow_status["counts"]["tasks"]["done"] == 1
                assert workflow_status["execution"]["runner_state"] == "stopped"

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
        finally:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task

    asyncio.run(scenario())
