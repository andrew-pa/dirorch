import clsx from 'clsx'
import type { CSSProperties, Dispatch, RefObject, SetStateAction } from 'react'
import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import {
  Activity,
  ArrowRight,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  CircleDot,
  FilePlus2,
  Lock,
  Pause,
  Play,
  RefreshCw,
  Route,
  Waypoints,
} from 'lucide-react'

import type {
  EntitySummary,
  PhaseDefinition,
  TransitionDefinition,
  WorkflowDefinition,
  WorkflowStatusPayload,
} from '../api/types'
import {
  activityLabel,
  entitiesForState,
  findCursorEntity,
  isActiveTransitionState,
  runnerEntityIds,
  stateCount,
  stateNamesForPhase,
} from '../lib/entities'
import { Tooltip } from './Tooltip'
import { InfoCard } from './ui/InfoCard'
import { SectionHeader } from './ui/SectionHeader'
import { Surface } from './ui/Surface'

interface WorkflowOverviewProps {
  entities: EntitySummary[]
  isRefreshing: boolean
  movingEntityId: string | null
  onCreateEntity: (phase: string, state: string) => void
  onMoveEntity: (entity: EntitySummary, phase: string, state: string) => void
  onRefresh: () => void
  onSelectEntity: (entity: EntitySummary) => void
  status: WorkflowStatusPayload
  workflow: WorkflowDefinition
}

export function WorkflowOverview({
  entities,
  isRefreshing,
  movingEntityId,
  onCreateEntity,
  onMoveEntity,
  onRefresh,
  onSelectEntity,
  status,
  workflow,
}: WorkflowOverviewProps) {
  const [expandedStates, setExpandedStates] = useState<Record<string, boolean>>({})
  const [draggedEntityId, setDraggedEntityId] = useState<string | null>(null)
  const [dropTargetKey, setDropTargetKey] = useState<string | null>(null)
  const [runtimeCollapsed, setRuntimeCollapsed] = useState(false)
  const cursorEntity = findCursorEntity(status.runtime_snapshot, entities)
  const activeIds = runnerEntityIds(status.execution)
  const runtimeSummary = [
    status.execution.runner_state,
    activityLabel(status.execution),
    status.execution.current_phase
      ? `${status.execution.current_phase}${status.execution.current_phase_mode ? `/${status.execution.current_phase_mode}` : ''}`
      : 'no phase',
    cursorEntity ? `cursor ${cursorEntity.id}` : 'no cursor',
    `${status.locked_entities} locks`,
    `${status.paused_entities} paused`,
  ].join(' · ')

  return (
    <div className="console-shell">
      <header className="console-header">
        <SectionHeader
          contentClassName="console-header__brand"
          eyebrow="Dirorch Console"
          title={<h1 className="console-header__title">Workflow</h1>}
          actions={
            <button className="button button--ghost" type="button" onClick={onRefresh}>
              <RefreshCw className={isRefreshing ? 'spin' : undefined} size={16} />
              Refresh
            </button>
          }
        />
      </header>

      <section className={clsx('runtime-panel', runtimeCollapsed && 'runtime-panel--collapsed')}>
        <button
          className="runtime-panel__toggle"
          type="button"
          aria-expanded={!runtimeCollapsed}
          onClick={() => setRuntimeCollapsed((current) => !current)}
        >
          <div className="runtime-panel__summary">
            <span className="eyebrow">System status</span>
            <span className="runtime-panel__headline">
              {runtimeCollapsed ? runtimeSummary : 'Status overview'}
            </span>
          </div>
          <span className="runtime-panel__toggle-icon" aria-hidden="true">
            {runtimeCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
          </span>
        </button>

        {!runtimeCollapsed ? (
          <div className="runtime-panel__cards">
            <InfoCard
              label="Runtime"
              value={
                <>
                  {status.execution.runner_state} · {activityLabel(status.execution)}
                </>
              }
              meta={
                <>
                  {status.execution.current_phase ?? 'No active phase'}
                  {status.execution.current_phase_mode
                    ? ` · ${status.execution.current_phase_mode}`
                    : ''}
                </>
              }
            />

            <InfoCard
              label="Cursor"
              value={cursorEntity ? cursorEntity.id : 'None'}
              meta={cursorEntity ? `${cursorEntity.phase}/${cursorEntity.state}` : 'No entity cursor'}
            />

            <InfoCard
              label="Locks"
              value={status.locked_entities}
              meta={`${status.paused_entities} paused${activeIds.length > 0 ? ` · ${activeIds.length} running` : ''}`}
            />

            <InfoCard label="Jump stack">
              {status.execution.jump_stack.length > 0 ? (
                <div className="jump-stack">
                  {status.execution.jump_stack.map((frame, index) => (
                    <span className="jump-stack__item" key={`${frame.source_phase}:${index}`}>
                      <Route size={14} />
                      {frame.source_phase} {'->'} {frame.target_phase}
                    </span>
                  ))}
                </div>
              ) : (
                <div className="info-card__meta">Empty</div>
              )}
            </InfoCard>
          </div>
        ) : null}
      </section>

      {status.execution.last_error ? (
        <section className="error-banner">
          <CircleAlert size={16} />
          <span>{status.execution.last_error}</span>
        </section>
      ) : null}

      <section className="phase-list">
        {workflow.phases.map((phase) => (
          <PhasePanel
            cursorEntity={cursorEntity}
            entities={entities}
            expandedStates={expandedStates}
            key={phase.name}
            movingEntityId={movingEntityId}
            onCreateEntity={onCreateEntity}
            onMoveEntity={onMoveEntity}
            onSelectEntity={onSelectEntity}
            phase={phase}
            setExpandedStates={setExpandedStates}
            status={status}
            draggedEntityId={draggedEntityId}
            dropTargetKey={dropTargetKey}
            setDraggedEntityId={setDraggedEntityId}
            setDropTargetKey={setDropTargetKey}
          />
        ))}
      </section>
    </div>
  )
}

interface PhasePanelProps {
  cursorEntity: EntitySummary | null
  draggedEntityId: string | null
  dropTargetKey: string | null
  entities: EntitySummary[]
  expandedStates: Record<string, boolean>
  movingEntityId: string | null
  onCreateEntity: (phase: string, state: string) => void
  onMoveEntity: (entity: EntitySummary, phase: string, state: string) => void
  onSelectEntity: (entity: EntitySummary) => void
  phase: PhaseDefinition
  setDraggedEntityId: Dispatch<SetStateAction<string | null>>
  setDropTargetKey: Dispatch<SetStateAction<string | null>>
  setExpandedStates: Dispatch<SetStateAction<Record<string, boolean>>>
  status: WorkflowStatusPayload
}

function PhasePanel({
  cursorEntity,
  draggedEntityId,
  dropTargetKey,
  entities,
  expandedStates,
  movingEntityId,
  onCreateEntity,
  onMoveEntity,
  onSelectEntity,
  phase,
  setDraggedEntityId,
  setDropTargetKey,
  setExpandedStates,
  status,
}: PhasePanelProps) {
  const stateNames = stateNamesForPhase(phase)
  const statesContainerRef = useRef<HTMLDivElement | null>(null)
  const staticTransitions = phase.transitions.filter(isStaticTransition)
  const connectorHeight = staticTransitions.length > 0 ? 18 : 0

  return (
    <Surface
      as="article"
      className={clsx(
        'phase-row',
        status.execution.current_phase === phase.name && 'phase-row--current',
      )}
    >
      <SectionHeader
        className="phase-row__header"
        title={
          <div className="phase-row__title">
            <h2>{phase.name}</h2>
            <span className="status-pill status-pill--neutral">
              <Waypoints size={14} />
              {phase.mode}
            </span>
          </div>
        }
        actions={
          <div className="phase-row__meta">
            <span className="status-pill status-pill--neutral">
              <Activity size={14} />
              {stateNames.reduce(
                (total, stateName) => total + stateCount(status.counts, phase.name, stateName),
                0,
              )}{' '}
              entities
            </span>
            <span className="status-pill status-pill--neutral">
              <ArrowRight size={14} />
              {phase.transitions.length} transitions
            </span>
            {status.execution.current_phase === phase.name ? (
              <span className="status-pill status-pill--success">
                <Play size={14} />
                Active phase
              </span>
            ) : null}
          </div>
        }
      />

      <div
        className="phase-row__flow"
        style={
          {
            '--connector-height': `${connectorHeight}px`,
            '--state-count': stateNames.length,
          } as CSSProperties
        }
      >
        {connectorHeight > 0 ? (
          <TransitionGraph
            phase={phase}
            stateContainerRef={statesContainerRef}
            stateNames={stateNames}
            status={status}
          />
        ) : null}

        <div className="phase-row__states" ref={statesContainerRef}>
          {stateNames.map((stateName) => {
            const items = entitiesForState(entities, phase.name, stateName)
            const stateKey = `${phase.name}:${stateName}`
            const expanded = expandedStates[stateKey] ?? false
            const processing = items.filter((entity) => entity.processing)
            const entityCount = stateCount(status.counts, phase.name, stateName)
            const runningSummary =
              processing.length > 0
                ? `Running: ${processing.map((entity) => entity.id).join(', ')}`
                : null
            const activeSource = isActiveTransitionState(
              status.execution,
              phase.name,
              stateName,
              'source',
            )
            const activeDestination = isActiveTransitionState(
              status.execution,
              phase.name,
              stateName,
              'destination',
            )
            const dynamicOutgoingTransitions = phase.transitions.filter(
              (transition) =>
                transition.from === stateName &&
                (typeof transition.to !== 'string' ||
                  (transition.jump !== null && typeof transition.jump !== 'string')),
            )
            const dropTargetActive = dropTargetKey === stateKey
            const draggedEntity = draggedEntityId
              ? entities.find((entity) => entity.id === draggedEntityId) ?? null
              : null
            const canDropDraggedEntity = Boolean(
              draggedEntity &&
                !draggedEntity.processing &&
                !movingEntityId &&
                (draggedEntity.phase !== phase.name || draggedEntity.state !== stateName),
            )

            return (
              <section
                data-state-drop-target={stateKey}
                className={clsx(
                  'state-card',
                  stateName.startsWith('_') && 'state-card--reserved',
                  activeSource && 'state-card--source',
                  activeDestination && 'state-card--destination',
                  dropTargetActive && 'state-card--drop-target',
                )}
                key={stateKey}
                onDragEnter={(event) => {
                  if (!canDropDraggedEntity) {
                    return
                  }

                  event.preventDefault()
                  setDropTargetKey(stateKey)
                }}
                onDragOver={(event) => {
                  if (!canDropDraggedEntity) {
                    return
                  }

                  event.preventDefault()
                  event.dataTransfer.dropEffect = 'move'
                  setDropTargetKey(stateKey)
                }}
                onDragLeave={(event) => {
                  if (event.currentTarget.contains(event.relatedTarget as Node | null)) {
                    return
                  }

                  if (dropTargetKey === stateKey) {
                    setDropTargetKey(null)
                  }
                }}
                onDrop={(event) => {
                  if (!draggedEntity || !canDropDraggedEntity) {
                    return
                  }

                  event.preventDefault()
                  setDropTargetKey(null)
                  setDraggedEntityId(null)
                  onMoveEntity(draggedEntity, phase.name, stateName)
                }}
              >
                <header className="state-card__header">
                  <div>
                    <div className="state-card__name">{stateName}</div>
                    {dynamicOutgoingTransitions.length > 0 ? (
                      <div className="state-card__transition-tags">
                        {dynamicOutgoingTransitions.map((transition, index) => (
                          <Tooltip
                            key={`${phase.name}:${stateName}:${index}`}
                            content={<TransitionTooltipContent transition={transition} />}
                          >
                            <button className="state-card__transition-tag" type="button">
                              <ArrowRight size={12} />
                              <span>dynamic</span>
                            </button>
                          </Tooltip>
                        ))}
                      </div>
                    ) : null}
                  </div>

                  <div className="state-card__actions">
                    {items.some((entity) => entity.locked) ? (
                      <span className="status-pill status-pill--warning">
                        <Lock size={14} />
                        {items.filter((entity) => entity.locked).length}
                      </span>
                    ) : null}
                    {items.some((entity) => entity.paused) ? (
                      <span className="status-pill status-pill--warning">
                        <Pause size={14} />
                        {items.filter((entity) => entity.paused).length}
                      </span>
                    ) : null}
                    <button
                      className="icon-button"
                      type="button"
                      aria-label={`Create entity in ${phase.name}/${stateName}`}
                      onClick={() => onCreateEntity(phase.name, stateName)}
                    >
                      <FilePlus2 size={16} />
                    </button>
                  </div>
                </header>

                <button
                  className="state-card__toggle"
                  type="button"
                  aria-expanded={expanded}
                  aria-label={`${expanded ? 'Collapse' : 'Expand'} entities in ${phase.name}/${stateName}`}
                  onClick={() =>
                    setExpandedStates((current) => ({
                      ...current,
                      [stateKey]: !expanded,
                    }))
                  }
                >
                  <span className="state-card__toggle-icon" aria-hidden="true">
                    {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                  </span>
                  <span className="state-card__toggle-meta">
                    {expanded
                      ? items.length > 0
                        ? 'Hide entities'
                        : 'No entities'
                      : items.length > 0
                        ? `View ${entityCount} ${entityCount === 1 ? 'entity' : 'entities'}${runningSummary ? ` · ${runningSummary}` : ''}`
                        : 'No entities'}
                  </span>
                </button>

                {expanded ? (
                  <div className="entity-list">
                    {items.length > 0 ? (
                      items.map((entity) => {
                        const canMoveEntity = !entity.processing && !entity.locked

                        return (
                        <button
                          data-entity-drag-source={entity.id}
                          className={clsx(
                            'entity-list__item',
                            canMoveEntity && 'entity-list__item--draggable',
                            movingEntityId === entity.id && 'entity-list__item--moving',
                          )}
                          draggable={canMoveEntity && movingEntityId !== entity.id}
                          key={entity.id}
                          type="button"
                          onDragEnd={() => {
                            setDraggedEntityId(null)
                            setDropTargetKey(null)
                          }}
                          onDragStart={(event) => {
                            if (!canMoveEntity || movingEntityId === entity.id) {
                              event.preventDefault()
                              return
                            }

                            event.dataTransfer.effectAllowed = 'move'
                            event.dataTransfer.setData('text/plain', entity.id)
                            setDraggedEntityId(entity.id)
                          }}
                          onClick={() => onSelectEntity(entity)}
                        >
                          <div className="entity-list__title">
                            <span className="entity-list__id">{entity.id}</span>
                            <div className="entity-list__badges">
                              {entity.processing ? (
                                <span className="status-pill status-pill--success">
                                  <Play size={14} />
                                  Running
                                </span>
                              ) : canMoveEntity ? (
                                <span className="status-pill status-pill--neutral">
                                  Move
                                </span>
                              ) : null}
                              {entity.locked ? (
                                <span className="status-pill status-pill--warning">
                                  <Lock size={14} />
                                  Locked
                                </span>
                              ) : null}
                              {entity.paused ? (
                                <span className="status-pill status-pill--warning">
                                  <Pause size={14} />
                                  Paused
                                </span>
                              ) : null}
                              {cursorEntity?.id === entity.id &&
                              cursorEntity.phase === entity.phase ? (
                                <span className="status-pill status-pill--neutral">
                                  <CircleDot size={14} />
                                  Cursor
                                </span>
                              ) : null}
                            </div>
                          </div>
                        </button>
                        )
                      })
                    ) : (
                      <div className="entity-list__empty">No entities</div>
                    )}
                  </div>
                ) : null}
              </section>
            )
          })}
        </div>
      </div>
    </Surface>
  )
}

interface TransitionGraphProps {
  phase: PhaseDefinition
  stateContainerRef: RefObject<HTMLDivElement | null>
  stateNames: string[]
  status: WorkflowStatusPayload
}

type StaticTransitionDefinition = TransitionDefinition & { to: string }

interface RoutedTransition {
  index: number
  sourceOrder: number
  sourceTotal: number
  targetOrder: number
  targetTotal: number
  transition: StaticTransitionDefinition
}

interface TransitionShape {
  triggerX: number
  triggerY: number
}

interface MeasuredStateRect {
  left: number
  right: number
  top: number
  height: number
  width: number
}

interface GraphLayoutSnapshot {
  width: number
  stateRects: Array<MeasuredStateRect | null>
}

function TransitionGraph({
  phase,
  stateContainerRef,
  stateNames,
  status,
}: TransitionGraphProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [layoutSnapshot, setLayoutSnapshot] = useState<GraphLayoutSnapshot | null>(null)
  const graphedTransitions = phase.transitions.filter(isStaticTransition)
  const orderedTransitions = [...graphedTransitions].sort((left, right) => {
    const leftDistance = Math.abs(
      stateNames.indexOf(left.to) - stateNames.indexOf(left.from),
    )
    const rightDistance = Math.abs(
      stateNames.indexOf(right.to) - stateNames.indexOf(right.from),
    )
    return rightDistance - leftDistance
  })
  const routedTransitions = useMemo<RoutedTransition[]>(() => {
    const outgoingTotals = new Map<string, number>()
    const incomingTotals = new Map<string, number>()
    const outgoingSlots = new Map<string, number>()
    const incomingSlots = new Map<string, number>()

    for (const transition of orderedTransitions) {
      outgoingTotals.set(transition.from, (outgoingTotals.get(transition.from) ?? 0) + 1)
      incomingTotals.set(transition.to, (incomingTotals.get(transition.to) ?? 0) + 1)
    }

    return orderedTransitions.map((transition, index) => {
      const sourceOrder = outgoingSlots.get(transition.from) ?? 0
      const targetOrder = incomingSlots.get(transition.to) ?? 0

      outgoingSlots.set(transition.from, sourceOrder + 1)
      incomingSlots.set(transition.to, targetOrder + 1)

      return {
        index,
        sourceOrder,
        sourceTotal: outgoingTotals.get(transition.from) ?? 1,
        targetOrder,
        targetTotal: incomingTotals.get(transition.to) ?? 1,
        transition,
      }
    })
  }, [orderedTransitions])

  useLayoutEffect(() => {
    const container = containerRef.current
    if (!container) {
      return
    }

    const measure = () => {
      const containerRect = container.getBoundingClientRect()
      const stateElements = Array.from(
        stateContainerRef.current?.querySelectorAll<HTMLElement>('.state-card') ?? [],
      )
      setLayoutSnapshot({
        width: containerRect.width,
        stateRects: stateElements.map((element) => {
          if (!element) {
            return null
          }

          const rect = element.getBoundingClientRect()
          return {
            left: rect.left - containerRect.left,
            right: rect.right - containerRect.left,
            top: rect.top - containerRect.top,
            height: rect.height,
            width: rect.width,
          }
        }),
      })
    }

    measure()

    const resizeObserver = new ResizeObserver(measure)
    resizeObserver.observe(container)
    const stateElements = Array.from(
      stateContainerRef.current?.querySelectorAll<HTMLElement>('.state-card') ?? [],
    )
    stateElements.forEach((element) => {
      if (element) {
        resizeObserver.observe(element)
      }
    })

    return () => resizeObserver.disconnect()
  }, [stateContainerRef, stateNames.length])

  const geometries = useMemo<
    Array<{
      isActive: boolean
      key: string
      route: RoutedTransition
      shape: TransitionShape
    }>
  >(
    () =>
      routedTransitions.flatMap((route) => {
        const shape = transitionGeometry(route, stateNames, layoutSnapshot)

        if (!shape) {
          return []
        }

        return [{
          isActive:
            status.execution.activity.kind === 'transition' &&
            status.execution.activity.phase === phase.name &&
            status.execution.activity.source_state === route.transition.from &&
            status.execution.activity.destination_state === route.transition.to,
          key: `${route.transition.from}:${route.transition.to}:${route.index}`,
          route,
          shape,
        }]
      }),
    [
      layoutSnapshot,
      phase.name,
      routedTransitions,
      stateNames,
      status.execution.activity.destination_state,
      status.execution.activity.kind,
      status.execution.activity.phase,
      status.execution.activity.source_state,
    ],
  )

  return (
    <div className="transition-graph" ref={containerRef}>
      {geometries.map(({ key, route, shape }) => (
        <Tooltip
          key={`tip:${key}`}
          content={<TransitionTooltipContent transition={route.transition} />}
        >
          <button
            className="transition-graph__trigger"
            aria-label={`Show transition ${route.transition.from} to ${targetLabel(route.transition.to)}`}
            style={{
              left: `${shape.triggerX}px`,
              top: `${shape.triggerY}px`,
            }}
            type="button"
          >
            <ArrowRight size={11} strokeWidth={2.3} />
          </button>
        </Tooltip>
      ))}
    </div>
  )
}

function TransitionTooltipContent({
  transition,
}: {
  transition: TransitionDefinition
}) {
  return (
    <div className="transition-tooltip">
      <div className="transition-tooltip__title">
        {transition.from} {'->'} {targetLabel(transition.to)}
      </div>
      <div>cmd: {transition.cmd ?? 'implicit move'}</div>
      {typeof transition.to === 'string' ? null : <div>to selector: {transition.to.cmd}</div>}
      <div>jump: {transition.jump === null ? 'none' : targetLabel(transition.jump)}</div>
      {transition.jump && typeof transition.jump !== 'string' ? (
        <div>jump selector: {transition.jump.cmd}</div>
      ) : null}
    </div>
  )
}

function isStaticTransition(
  transition: TransitionDefinition,
): transition is StaticTransitionDefinition {
  return typeof transition.to === 'string'
}

function targetLabel(target: TransitionDefinition['to'] | TransitionDefinition['jump']) {
  if (target === null) {
    return 'none'
  }
  if (typeof target === 'string') {
    return target
  }
  return `dynamic (${target.cmd})`
}

function transitionGeometry(
  route: RoutedTransition,
  stateNames: string[],
  layoutSnapshot: GraphLayoutSnapshot | null,
): TransitionShape | null {
  const { transition } = route
  const fromIndex = stateNames.indexOf(transition.from)
  const toIndex = stateNames.indexOf(transition.to)
  const fromState = layoutSnapshot?.stateRects[fromIndex] ?? null
  const toState = layoutSnapshot?.stateRects[toIndex] ?? null

  if (!layoutSnapshot || !fromState || !toState) {
    return null
  }

  const fromX = resolvePortX(fromState, route.sourceOrder, route.sourceTotal)
  const toX = resolvePortX(toState, route.targetOrder, route.targetTotal)

  if (fromIndex === toIndex) {
    return {
      triggerX: Math.min(layoutSnapshot.width - 10, fromState.right - 10),
      triggerY: fromState.top + fromState.height / 2,
    }
  }

  return {
    triggerX: fromX + (toX - fromX) / 2,
    triggerY: Math.min(fromState.top, toState.top) + Math.max(fromState.height, toState.height) / 2,
  }
}

function resolvePortX(
  stateRect: MeasuredStateRect,
  order: number,
  total: number,
) {
  const inset = Math.min(22, Math.max(12, stateRect.width * 0.1))
  const left = stateRect.left + inset
  const usableWidth = Math.max(stateRect.width - inset * 2, 12)

  if (total <= 1) {
    return left + usableWidth / 2
  }

  return left + (usableWidth / (total + 1)) * (order + 1)
}
