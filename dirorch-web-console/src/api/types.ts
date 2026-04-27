export type ContentFormat = 'text' | 'json'

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }

export interface TransitionDefinition {
  from: string
  to: NamedTargetDefinition
  cmd: string | null
  cwd: string | null
  jump: NamedTargetDefinition | null
}

export interface CompletionHook {
  cmd: string
  stdin: string | null
  cwd: string | null
}

export type NamedTargetDefinition = string | CompletionHook

export interface PhaseDefinition {
  name: string
  mode: string
  cwd: string | null
  states: string[]
  reserved_states: string[]
  transitions: TransitionDefinition[]
  completions: CompletionHook[]
}

export interface WorkflowDefinition {
  workflow_file: string
  phase_order: string[]
  environment: Record<string, string>
  retries: number
  cwd: string | null
  init: CompletionHook | null
  phases: PhaseDefinition[]
}

export interface EntitySummary {
  id: string
  phase: string
  state: string
  locked: boolean
  paused: boolean
  processing: boolean
  format: ContentFormat
}

export interface EntityDetail extends EntitySummary {
  content: string
  json?: JsonValue
}

export interface EntityLogPayload {
  entity_id: string
  text: string
  offset: number
  next_offset: number
  exists: boolean
  processing: boolean
}

export interface EntityLogAppendEvent {
  entity_id: string
  text: string
  next_offset: number
  processing: boolean
}

export interface EntityLogStatusEvent {
  entity_id: string
  processing: boolean
}

export interface FileDetail {
  path: string
  format: ContentFormat
  content: string
  json?: JsonValue
}

export interface RuntimeJumpFrame {
  source_phase: string
  target_phase: string
  source_entity_name: string | null
}

export interface RuntimeEntityCursor {
  phase: string
  entity_name: string
}

export interface RuntimeSnapshot {
  schema_version: number
  current_phase: string
  jump_stack: RuntimeJumpFrame[]
  entity_cursor: RuntimeEntityCursor | null
}

export interface ExecutionActivity {
  kind: 'init' | 'completion' | 'transition' | null
  phase: string | null
  phase_mode: string | null
  source_state: string | null
  destination_state: string | null
  entity_ids: string[]
  details: string | null
}

export interface ExecutionStatus {
  runner_state: 'idle' | 'running' | 'paused' | 'stopped' | 'failed'
  current_phase: string | null
  current_phase_mode: string | null
  activity: ExecutionActivity
  jump_stack: RuntimeJumpFrame[]
  last_error: string | null
}

export interface WorkflowStatusPayload {
  runtime_snapshot: RuntimeSnapshot | null
  counts: Record<string, Record<string, number>>
  locked_entities: number
  paused_entities: number
  workflow_pause_state: 'running' | 'pausing' | 'paused'
  execution: ExecutionStatus
}

export interface EntityStatusPayload {
  entities: EntitySummary[]
}

export interface WorkflowPausePayload {
  workflow_pause_state: 'running' | 'pausing' | 'paused'
  paused_entities: number
  entities: EntitySummary[]
}

export interface CreateEntityPayload {
  id: string
  phase: string
  state: string
  format: ContentFormat
  content: string
}

export interface UpdateEntityPayload {
  phase?: string
  state?: string
  format?: ContentFormat
  content?: string
}

export interface WriteFilePayload {
  format: ContentFormat
  content: string
}
