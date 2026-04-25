import type {
  CreateEntityPayload,
  EntityDetail,
  EntityLogPayload,
  EntityStatusPayload,
  FileDetail,
  UpdateEntityPayload,
  WorkflowDefinition,
  WorkflowPausePayload,
  WorkflowStatusPayload,
  WriteFilePayload,
} from './types'

const API_BASE = (import.meta.env.VITE_DIRORCH_API_BASE ?? '').replace(/\/$/, '')

interface ErrorEnvelope {
  error?: string
  code?: string
}

export class ApiError extends Error {
  readonly status: number
  readonly code: string | null

  constructor(message: string, status: number, code: string | null = null) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

export const queryKeys = {
  workflow: ['workflow'] as const,
  workflowStatus: ['workflow-status'] as const,
  entities: ['entities'] as const,
  entity: (entityId: string) => ['entity', entityId] as const,
  entityLog: (entityId: string) => ['entity-log', entityId] as const,
  file: (path: string) => ['file', path] as const,
}

export async function getWorkflow() {
  return requestJson<WorkflowDefinition>('/workflow')
}

export async function getWorkflowStatus() {
  return requestJson<WorkflowStatusPayload>('/status/workflow')
}

export async function getEntities() {
  return requestJson<EntityStatusPayload>('/status/entities')
}

export async function pauseWorkflow() {
  return requestJson<WorkflowPausePayload>('/workflow/pause', {
    method: 'POST',
  })
}

export async function getEntity(entityId: string) {
  return requestJson<EntityDetail>(`/entity/${encodeURIComponent(entityId)}`)
}

export async function createEntity(payload: CreateEntityPayload) {
  return requestJson<EntityDetail>('/entity', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function updateEntity(entityId: string, payload: UpdateEntityPayload) {
  return requestJson<EntityDetail>(`/entity/${encodeURIComponent(entityId)}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export async function setEntityLocked(entityId: string, locked: boolean) {
  return requestJson<EntityDetail>(`/entity/${encodeURIComponent(entityId)}/lock`, {
    method: 'PUT',
    body: JSON.stringify({ locked }),
  })
}

export async function setEntityPaused(entityId: string, paused: boolean) {
  return requestJson<EntityDetail>(`/entity/${encodeURIComponent(entityId)}/pause`, {
    method: 'PUT',
    body: JSON.stringify({ paused }),
  })
}

export async function getEntityLog(
  entityId: string,
  offset = 0,
  limitBytes?: number,
) {
  const params = new URLSearchParams({ offset: String(offset) })
  if (limitBytes !== undefined) {
    params.set('limit_bytes', String(limitBytes))
  }
  return requestJson<EntityLogPayload>(
    `/entity/${encodeURIComponent(entityId)}/log?${params.toString()}`,
  )
}

export function openEntityLogEvents(entityId: string, fromOffset: number) {
  const params = new URLSearchParams({ from_offset: String(fromOffset) })
  return new EventSource(
    `${API_BASE}/entity/${encodeURIComponent(entityId)}/log/events?${params.toString()}`,
  )
}

export async function getFile(path: string) {
  return requestJson<FileDetail>(`/file/${encodePath(path)}`)
}

export async function createFile(path: string, payload: WriteFilePayload) {
  return requestJson<FileDetail>(`/file/${encodePath(path)}`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function updateFile(path: string, payload: WriteFilePayload) {
  return requestJson<FileDetail>(`/file/${encodePath(path)}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

async function requestJson<T>(path: string, init?: RequestInit) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init?.headers,
    },
  })

  if (!response.ok) {
    throw await toApiError(response)
  }

  return (await response.json()) as T
}

async function toApiError(response: Response) {
  try {
    const payload = (await response.json()) as ErrorEnvelope
    return new ApiError(
      payload.error ?? `Request failed with status ${response.status}`,
      response.status,
      payload.code ?? null,
    )
  } catch {
    return new ApiError(`Request failed with status ${response.status}`, response.status)
  }
}

function encodePath(path: string) {
  return path
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/')
}
