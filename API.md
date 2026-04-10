# Dirorch HTTP API

This document describes the HTTP API exposed when Dirorch is started with `--web`.

Example startup:

```bash
dirorch ./workflow.yaml --root ./work --web
```

Enable HTTP access logging explicitly with:

```bash
dirorch ./workflow.yaml --root ./work --web --web-log
```

Default bind address:

- host: `127.0.0.1`
- port: `8000`

Base URL example:

```text
http://127.0.0.1:8000
```

## General Conventions

### Content Type

- Request bodies for mutating endpoints must be JSON objects.
- Responses are JSON except for `204 No Content` responses.

### Data Types

- `string`: JSON string
- `boolean`: JSON boolean
- `integer`: JSON integer
- `object`: JSON object
- `array<T>`: JSON array of `T`
- `null`: JSON null

### Entity Identity

- Entity `id` is the filename, including extension.
- Entity ids are global across the workflow.
- If multiple files with the same filename exist in different phase/state directories, entity lookups become ambiguous and the API returns `409 Conflict`.

### Formats

The API supports two logical content formats:

- `text`: arbitrary UTF-8 text
- `json`: UTF-8 text that must parse as JSON

For `json`, the API stores the original JSON text and may also return a parsed `json` field in responses.

### Error Envelope

Error responses use this schema:

```json
{
  "error": "Human-readable message",
  "code": "machine_code"
}
```

Current `code` values:

- `validation_error`
- `forbidden`
- `not_found`
- `conflict`
- `workflow_error`
- `internal_error`

### Standard Status Codes

- `200 OK`: successful read or update
- `201 Created`: successful create
- `204 No Content`: successful delete
- `400 Bad Request`: invalid request body or non-JSON validation failure
- `403 Forbidden`: reserved or disallowed path
- `404 Not Found`: missing entity or file
- `409 Conflict`: duplicate resource, ambiguous entity id, or in-use mutation conflict
- `422 Unprocessable Entity`: invalid JSON content for `format=json`
- `500 Internal Server Error`: unexpected internal failure

## Shared Schemas

### EntitySummary

```json
{
  "id": "task.txt",
  "phase": "tasks",
  "state": "new",
  "locked": false,
  "processing": false,
  "format": "text"
}
```

Fields:

- `id: string`
- `phase: string`
- `state: string`
- `locked: boolean`
- `processing: boolean`
- `format: "text" | "json"`

### EntityDetail

`EntityDetail` extends `EntitySummary`.

Text example:

```json
{
  "id": "task.txt",
  "phase": "tasks",
  "state": "new",
  "locked": false,
  "processing": false,
  "format": "text",
  "content": "plain text"
}
```

JSON example:

```json
{
  "id": "task.json",
  "phase": "tasks",
  "state": "new",
  "locked": false,
  "processing": false,
  "format": "json",
  "content": "{\"name\":\"ship\"}",
  "json": {
    "name": "ship"
  }
}
```

Additional fields:

- `content: string`
- `json: object | array | string | number | boolean | null`
  - present only when `content` parses as JSON

### FileDetail

Text example:

```json
{
  "path": "docs/readme.txt",
  "format": "text",
  "content": "hello"
}
```

JSON example:

```json
{
  "path": "docs/info.json",
  "format": "json",
  "content": "{\"ok\":true}",
  "json": {
    "ok": true
  }
}
```

Fields:

- `path: string`
- `format: "text" | "json"`
- `content: string`
- `json: any`
  - present only when content parses as JSON

### RuntimeSnapshot

```json
{
  "schema_version": 2,
  "current_phase": "tasks",
  "jump_stack": [],
  "entity_cursor": null
}
```

Fields:

- `schema_version: integer`
- `current_phase: string`
- `jump_stack: array<object>`
- `entity_cursor: object | null`

`jump_stack` item schema:

- `source_phase: string`
- `target_phase: string`
- `source_entity_name: string | null`

`entity_cursor` schema:

- `phase: string`
- `entity_name: string`

### ExecutionStatus

```json
{
  "runner_state": "running",
  "current_phase": "tasks",
  "current_phase_mode": "transitions",
  "activity": {
    "kind": "transition",
    "phase": "tasks",
    "phase_mode": "transitions",
    "source_state": "new",
    "destination_state": "done",
    "entity_ids": ["task.txt"],
    "details": null
  },
  "jump_stack": [],
  "last_error": null
}
```

Fields:

- `runner_state: "idle" | "running" | "stopped" | "failed"`
- `current_phase: string | null`
- `current_phase_mode: string | null`
- `activity: object`
- `jump_stack: array<object>`
- `last_error: string | null`

`activity` fields:

- `kind: "init" | "completion" | "transition" | null`
- `phase: string | null`
- `phase_mode: string | null`
- `source_state: string | null`
- `destination_state: string | null`
- `entity_ids: array<string>`
- `details: string | null`

## Routes

### GET `/workflow`

Return the loaded workflow definition as currently understood by the running process.

Response `200`:

```json
{
  "phase_order": ["tasks"],
  "environment": {},
  "retries": 3,
  "init": null,
  "phases": [
    {
      "name": "tasks",
      "mode": "transitions",
      "states": ["new", "done"],
      "reserved_states": ["_failed"],
      "transitions": [
        {
          "from": "new",
          "to": "done",
          "cmd": null,
          "jump": null
        }
      ],
      "completions": []
    }
  ]
}
```

Fields:

- `phase_order: array<string>`
- `environment: object`
- `retries: integer`
- `init: object | null`
- `phases: array<object>`

Per-phase fields:

- `name: string`
- `mode: string`
  Supported values currently include `transitions`, `parallel`, and `entity`.
- `states: array<string>`
- `reserved_states: array<string>`
- `transitions: array<object>`
- `completions: array<object>`

Transition fields:

- `from: string`
- `to: string | { cmd: string, stdin: string | null }`
- `cmd: string | null`
- `jump: string | { cmd: string, stdin: string | null } | null`

Completion/init hook fields:

- `cmd: string`
- `stdin: string | null`

Dynamic target semantics:

- Transition side-effect `cmd` runs before dynamic `to` or dynamic `jump` selectors.
- Selector commands must write the chosen state or phase name to fd `3`.
- Empty dynamic `to` output means no move; empty dynamic `jump` output means no jump.

Errors:

- `500` if internal workflow state cannot be read

### GET `/status/workflow`

Return workflow runtime state, counts, locks, and current execution activity.

Response `200`:

```json
{
  "runtime_snapshot": {
    "schema_version": 2,
    "current_phase": "tasks",
    "jump_stack": [],
    "entity_cursor": null
  },
  "counts": {
    "tasks": {
      "new": 1,
      "done": 2,
      "_failed": 0
    }
  },
  "locked_entities": 1,
  "execution": {
    "runner_state": "idle",
    "current_phase": "tasks",
    "current_phase_mode": "transitions",
    "activity": {
      "kind": null,
      "phase": "tasks",
      "phase_mode": "transitions",
      "source_state": null,
      "destination_state": null,
      "entity_ids": [],
      "details": null
    },
    "jump_stack": [],
    "last_error": null
  }
}
```

Fields:

- `runtime_snapshot: RuntimeSnapshot | null`
- `counts: object`
  - top-level keys are phase names
  - each phase value is an object mapping state name to integer count
  - `_failed` is always included
- `locked_entities: integer`
- `execution: ExecutionStatus`

Operational semantics:

- `runtime_snapshot` may be `null` before any snapshot has been persisted.
- `execution.runner_state` reflects workflow runner state, not server state.
- `execution.activity.entity_ids` may contain multiple ids when grouped concurrency is active.

Errors:

- `500` if runtime snapshot or lock state cannot be read

### GET `/status/entities`

Return all known entities.

Response `200`:

```json
{
  "entities": [
    {
      "id": "task.txt",
      "phase": "tasks",
      "state": "new",
      "locked": false,
      "processing": false,
      "format": "text"
    }
  ]
}
```

Fields:

- `entities: array<EntitySummary>`

Operational semantics:

- Results are derived from the current on-disk phase/state layout.
- `processing=true` is transient and only indicates active in-process execution for the current runner.

Errors:

- `500` if entity state cannot be enumerated

### GET `/entity/{id}`

Return one entity by filename.

Path params:

- `id: string`

Response `200`:

- body schema: `EntityDetail`

Operational semantics:

- Lookup is by exact filename, including extension.
- If the stored content parses as JSON, the response includes `format: "json"` and a parsed `json` field.

Errors:

- `404` if the entity does not exist
- `409` if the entity id is ambiguous across multiple phase/state directories

### POST `/entity`

Create a new entity file.

Request body:

```json
{
  "id": "task.txt",
  "phase": "tasks",
  "state": "new",
  "format": "text",
  "content": "hello"
}
```

Fields:

- `id: string` required
- `phase: string` required
- `state: string` required
- `format: "text" | "json"` required
- `content: string` required

Operational semantics:

- The target phase/state must already exist in the workflow definition.
- The entity id must not already exist anywhere in the workflow.
- For `format=json`, `content` must parse as JSON before write.
- The response body is the created `EntityDetail`.

Success:

- `201 Created`

Errors:

- `400` invalid request shape or unknown phase/state
- `409` duplicate entity id
- `422` invalid JSON content

### PUT `/entity/{id}`

Update entity content and/or move the entity to another phase/state.

Path params:

- `id: string`

Request body:

```json
{
  "phase": "tasks",
  "state": "done",
  "format": "json",
  "content": "{\"done\":true}"
}
```

All fields are optional, but at least one meaningful change should be supplied:

- `phase: string` optional
- `state: string` optional
- `format: "text" | "json"` optional
- `content: string` optional

Operational semantics:

- If `phase` or `state` is omitted, the current value is retained.
- If `format` is provided, `content` must also be provided.
- If `content` is provided and `format` is omitted, the content is validated as `text`.
- For `format=json`, `content` must parse as JSON.
- The entity may be moved and updated in a single request.
- Mutations are rejected while the entity is actively being processed.
- Lock state does not block update; only active processing does.

Success:

- `200 OK`
- response body: updated `EntityDetail`

Errors:

- `400` invalid request shape or unknown target phase/state
- `404` entity not found
- `409` entity is ambiguous or currently processing
- `422` invalid JSON content

### PUT `/entity/{id}/lock`

Lock or unlock an entity.

Path params:

- `id: string`

Request body:

```json
{
  "locked": true
}
```

Fields:

- `locked: boolean` required

Operational semantics:

- A locked entity is skipped by workflow transition selection.
- Lock state is persisted in `${root}/.dirorch_locks.json`.
- The operation is idempotent.
- Lock changes are rejected while the entity is actively being processed.
- Unlocking an entity does not itself trigger execution; it only changes eligibility for the next workflow pass.

Success:

- `200 OK`
- response body: updated `EntityDetail`

Errors:

- `400` invalid request shape
- `404` entity not found
- `409` entity is ambiguous or currently processing

### DELETE `/entity/{id}`

Delete an entity file.

Path params:

- `id: string`

Operational semantics:

- Also clears any persisted lock record for that entity id.
- Deletion is rejected while the entity is actively being processed.

Success:

- `204 No Content`

Errors:

- `404` entity not found
- `409` entity is ambiguous or currently processing

### GET `/file/{path...}`

Read a non-entity file under the workflow root.

Path params:

- `path: string`
  - relative path under `--root`

Response `200`:

- body schema: `FileDetail`

Operational semantics:

- Reads UTF-8 text only.
- If content parses as JSON, `format` is reported as `json` and the parsed `json` field is included.
- Reserved internal workflow locations are blocked.

Forbidden path categories:

- absolute paths
- paths containing `..`
- phase top-level directories such as `tasks/...`
- the runtime state file, typically `.dirorch_runtime.json`
- the lock file `.dirorch_locks.json`

Errors:

- `403` forbidden or reserved path
- `404` file not found

### POST `/file/{path...}`

Create a non-entity file.

Request body:

```json
{
  "format": "json",
  "content": "{\"ok\":true}"
}
```

Fields:

- `format: "text" | "json"` required
- `content: string` required

Operational semantics:

- Parent directories are created automatically.
- The file must not already exist.
- For `format=json`, `content` must parse as JSON before write.
- Response body is `FileDetail`.

Success:

- `201 Created`

Errors:

- `400` invalid request shape
- `403` forbidden or reserved path
- `409` file already exists
- `422` invalid JSON content

### PUT `/file/{path...}`

Update an existing non-entity file.

Request body:

```json
{
  "format": "text",
  "content": "updated"
}
```

Fields:

- `format: "text" | "json"` required
- `content: string` required

Operational semantics:

- The file must already exist.
- For `format=json`, `content` must parse as JSON before write.
- Response body is `FileDetail`.

Success:

- `200 OK`

Errors:

- `400` invalid request shape
- `403` forbidden or reserved path
- `404` file not found
- `422` invalid JSON content

### DELETE `/file/{path...}`

Delete an existing non-entity file.

Path params:

- `path: string`

Operational semantics:

- Reserved and forbidden paths are blocked before deletion.

Success:

- `204 No Content`

Errors:

- `403` forbidden or reserved path
- `404` file not found

## Concurrency and Semantics Notes

- The HTTP server and workflow engine run in the same process.
- API write operations are serialized through an internal mutation coordinator.
- Read endpoints reflect the current in-process view of the filesystem and runner state.
- `processing=true` is only true while the current process is actively executing that entity.
- When `--web` is enabled without `--watch`, the workflow performs its normal initial pass and then stops, but the API server remains available until process shutdown.
