import type {
  EntitySummary,
  ExecutionStatus,
  JsonValue,
  PhaseDefinition,
  RuntimeSnapshot,
} from '../api/types'

export function entitiesForState(
  entities: EntitySummary[],
  phaseName: string,
  stateName: string,
) {
  return sortEntities(
    entities.filter((entity) => entity.phase === phaseName && entity.state === stateName),
  )
}

export function sortEntities(entities: EntitySummary[]) {
  return [...entities].sort((left, right) => {
    if (left.processing !== right.processing) {
      return left.processing ? -1 : 1
    }
    if (left.locked !== right.locked) {
      return left.locked ? -1 : 1
    }
    return left.id.localeCompare(right.id)
  })
}

export function stateNamesForPhase(phase: PhaseDefinition) {
  return [...phase.states, ...phase.reserved_states]
}

export function stateCount(
  counts: Record<string, Record<string, number>>,
  phaseName: string,
  stateName: string,
) {
  return counts[phaseName]?.[stateName] ?? 0
}

export function findCursorEntity(
  snapshot: RuntimeSnapshot | null,
  entities: EntitySummary[],
) {
  if (!snapshot?.entity_cursor) {
    return null
  }

  const cursor = snapshot.entity_cursor
  return (
    entities.find(
      (entity) => entity.phase === cursor.phase && entity.id === cursor.entity_name,
    ) ?? null
  )
}

export function isActiveTransitionState(
  execution: ExecutionStatus,
  phaseName: string,
  stateName: string,
  role: 'source' | 'destination',
) {
  if (execution.activity.kind !== 'transition') {
    return false
  }

  if (execution.activity.phase !== phaseName) {
    return false
  }

  if (role === 'source') {
    return execution.activity.source_state === stateName
  }

  return execution.activity.destination_state === stateName
}

export function runnerEntityIds(execution: ExecutionStatus) {
  return execution.activity.entity_ids
}

export function activityLabel(execution: ExecutionStatus) {
  const activity = execution.activity

  if (activity.kind === 'transition') {
    const source = activity.source_state ?? 'unknown'
    const destination = activity.destination_state ?? 'unknown'
    return `${source} -> ${destination}`
  }

  if (activity.kind === 'completion') {
    return 'completion'
  }

  if (activity.kind === 'init') {
    return 'init'
  }

  return 'idle'
}

export function formatJsonValue(value: JsonValue | undefined) {
  return value === undefined ? '' : JSON.stringify(value, null, 2)
}

