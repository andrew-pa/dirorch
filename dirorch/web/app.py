from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from ..errors import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
    WorkflowError,
)
from ..services import (
    EntityAdminService,
    FileAdminService,
    WorkflowDefinitionService,
    WorkflowStatusService,
)


@dataclass(frozen=True)
class WebServices:
    definition: WorkflowDefinitionService
    status: WorkflowStatusService
    entities: EntityAdminService
    files: FileAdminService


SERVICES_KEY: web.AppKey[WebServices] = web.AppKey("services", WebServices)


def build_web_app(services: WebServices) -> web.Application:
    app = web.Application(middlewares=[_error_middleware])
    app[SERVICES_KEY] = services
    app.add_routes(
        [
            web.get("/workflow", _get_workflow),
            web.get("/status/workflow", _get_workflow_status),
            web.get("/status/entities", _get_entity_status),
            web.get("/entity/{id}", _get_entity),
            web.post("/entity", _post_entity),
            web.put("/entity/{id}", _put_entity),
            web.put("/entity/{id}/lock", _put_entity_lock),
            web.delete("/entity/{id}", _delete_entity),
            web.get(r"/file/{path:.*}", _get_file),
            web.post(r"/file/{path:.*}", _post_file),
            web.put(r"/file/{path:.*}", _put_file),
            web.delete(r"/file/{path:.*}", _delete_file),
        ]
    )
    return app


class WebServer:
    def __init__(
        self,
        app: web.Application,
        host: str,
        port: int,
        access_log_enabled: bool = False,
    ) -> None:
        self._app = app
        self._host = host
        self._port = port
        self._access_log_enabled = access_log_enabled
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None

    async def start(self) -> None:
        access_log = logging.getLogger("aiohttp.access")
        if not self._access_log_enabled:
            access_log = None
        self._runner = web.AppRunner(self._app, access_log=access_log)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None


@web.middleware
async def _error_middleware(
    request: web.Request,
    handler,
) -> web.StreamResponse:
    try:
        return await handler(request)
    except NotFoundError as exc:
        return web.json_response(_error_payload("not_found", str(exc)), status=404)
    except ConflictError as exc:
        return web.json_response(_error_payload("conflict", str(exc)), status=409)
    except ForbiddenError as exc:
        return web.json_response(_error_payload("forbidden", str(exc)), status=403)
    except ValidationError as exc:
        status = 422 if "Invalid JSON" in str(exc) else 400
        return web.json_response(
            _error_payload("validation_error", str(exc)),
            status=status,
        )
    except WorkflowError as exc:
        return web.json_response(
            _error_payload("workflow_error", str(exc)),
            status=500,
        )
    except web.HTTPException:
        raise
    except Exception as exc:
        return web.json_response(
            _error_payload("internal_error", str(exc)),
            status=500,
        )


async def _get_workflow(request: web.Request) -> web.Response:
    services = _services(request)
    return web.json_response(services.definition.describe())


async def _get_workflow_status(request: web.Request) -> web.Response:
    services = _services(request)
    return web.json_response(services.status.workflow_status())


async def _get_entity_status(request: web.Request) -> web.Response:
    services = _services(request)
    return web.json_response(services.status.entity_status())


async def _get_entity(request: web.Request) -> web.Response:
    services = _services(request)
    return web.json_response(services.entities.get_entity(request.match_info["id"]))


async def _post_entity(request: web.Request) -> web.Response:
    payload = await _json_body(request)
    services = _services(request)
    entity = await services.entities.create_entity(
        entity_id=_require_string(payload, "id"),
        phase_name=_require_string(payload, "phase"),
        state_name=_require_string(payload, "state"),
        content=_require_string(payload, "content"),
        format_name=_require_string(payload, "format"),
    )
    return web.json_response(entity, status=201)


async def _put_entity(request: web.Request) -> web.Response:
    payload = await _json_body(request)
    services = _services(request)
    entity = await services.entities.update_entity(
        entity_id=request.match_info["id"],
        phase_name=_optional_string(payload, "phase"),
        state_name=_optional_string(payload, "state"),
        content=_optional_string(payload, "content"),
        format_name=_optional_string(payload, "format"),
    )
    return web.json_response(entity)


async def _put_entity_lock(request: web.Request) -> web.Response:
    payload = await _json_body(request)
    locked = payload.get("locked")
    if not isinstance(locked, bool):
        raise ValidationError("'locked' must be a boolean")
    services = _services(request)
    entity = await services.entities.set_locked(
        entity_id=request.match_info["id"],
        locked=locked,
    )
    return web.json_response(entity)


async def _delete_entity(request: web.Request) -> web.Response:
    services = _services(request)
    await services.entities.delete_entity(request.match_info["id"])
    return web.Response(status=204)


async def _get_file(request: web.Request) -> web.Response:
    services = _services(request)
    return web.json_response(services.files.get_file(request.match_info["path"]))


async def _post_file(request: web.Request) -> web.Response:
    payload = await _json_body(request)
    services = _services(request)
    file_payload = await services.files.create_file(
        relative_path=request.match_info["path"],
        content=_require_string(payload, "content"),
        format_name=_require_string(payload, "format"),
    )
    return web.json_response(file_payload, status=201)


async def _put_file(request: web.Request) -> web.Response:
    payload = await _json_body(request)
    services = _services(request)
    file_payload = await services.files.update_file(
        relative_path=request.match_info["path"],
        content=_require_string(payload, "content"),
        format_name=_require_string(payload, "format"),
    )
    return web.json_response(file_payload)


async def _delete_file(request: web.Request) -> web.Response:
    services = _services(request)
    await services.files.delete_file(request.match_info["path"])
    return web.Response(status=204)


def _services(request: web.Request) -> WebServices:
    return request.app[SERVICES_KEY]


async def _json_body(request: web.Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise ValidationError("Request body must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValidationError("Request body must be a JSON object")
    return payload


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"'{key}' must be a non-empty string")
    return value


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValidationError(f"'{key}' must be a non-empty string when provided")
    return value


def _error_payload(code: str, message: str) -> dict[str, Any]:
    return {
        "error": message,
        "code": code,
    }
