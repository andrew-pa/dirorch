import * as Dialog from '@radix-ui/react-dialog'
import clsx from 'clsx'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronRight,
  Clock3,
  FilePenLine,
  FileJson2,
  LoaderCircle,
  Lock,
  Pause,
  Plus,
  Play,
  Save,
  X,
} from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import {
  ApiError,
  createEntity,
  getEntity,
  queryKeys,
  setEntityLocked,
  setEntityPaused,
  updateEntity,
} from '../api/dirorch'
import type {
  ContentFormat,
  EntityDetail,
  EntitySummary,
  WorkflowDefinition,
} from '../api/types'
import { extractPathReferences, tryParseJson } from '../lib/json'
import { DocumentContentEditor } from './DocumentContentEditor'
import { EntityLogViewer } from './EntityLogViewer'
import { LinkedFileEditor } from './LinkedFileEditor'
import { EmptyState } from './ui/EmptyState'
import { SectionHeader } from './ui/SectionHeader'

export type EntityPanelTab = 'content' | 'logs'
export type EntityFullscreenPane = 'file' | 'logs' | null

interface EntityEditorModalProps {
  activeTab?: EntityPanelTab
  fullscreenPane?: EntityFullscreenPane
  initialPhase: string
  initialState: string
  mode: 'create' | 'edit'
  onActiveTabChange?: (activeTab: EntityPanelTab) => void
  onClose: () => void
  onFullscreenPaneChange?: (fullscreenPane: EntityFullscreenPane) => void
  onSelectedFilePathChange?: (selectedFilePath: string | null) => void
  presentation?: 'modal' | 'panel'
  selectedFilePath?: string | null
  summary?: EntitySummary
  workflow: WorkflowDefinition
}

interface EntityDraft {
  id: string
  phase: string
  state: string
  format: ContentFormat
  rawContent: string
  editorMode: 'raw' | 'structured'
}

const EMPTY_DRAFT: EntityDraft = {
  id: '',
  phase: '',
  state: '',
  format: 'text',
  rawContent: '',
  editorMode: 'raw',
}

export function EntityEditorModal({
  activeTab: controlledActiveTab,
  fullscreenPane: controlledFullscreenPane,
  initialPhase,
  initialState,
  mode,
  onActiveTabChange,
  onClose,
  onFullscreenPaneChange,
  onSelectedFilePathChange,
  presentation = 'modal',
  selectedFilePath: controlledSelectedFilePath,
  summary,
  workflow,
}: EntityEditorModalProps) {
  const queryClient = useQueryClient()
  const entityId = summary?.id ?? null
  const detailQuery = useQuery({
    queryKey: entityId ? queryKeys.entity(entityId) : [...queryKeys.entities(), 'draft'],
    queryFn: () => getEntity(entityId!),
    enabled: mode === 'edit' && Boolean(entityId),
    retry: false,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  })

  const [draft, setDraft] = useState<EntityDraft>({
    ...EMPTY_DRAFT,
    phase: initialPhase,
    state: initialState,
  })
  const [localSelectedFilePath, setLocalSelectedFilePath] = useState<string | null>(null)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [isPausePending, setIsPausePending] = useState(false)
  const [isEditing, setIsEditing] = useState(mode === 'create')
  const [lockState, setLockState] = useState<'idle' | 'pending' | 'ready' | 'error'>(
    mode === 'create' ? 'ready' : 'idle',
  )
  const [localActiveTab, setLocalActiveTab] = useState<EntityPanelTab>('content')
  const [localFullscreenPane, setLocalFullscreenPane] = useState<EntityFullscreenPane>(null)

  const loadedEntityRef = useRef<EntityDetail | null>(null)
  const ownedLockRef = useRef(false)
  const activeTab = controlledActiveTab ?? localActiveTab
  const selectedFilePath =
    controlledSelectedFilePath === undefined
      ? localSelectedFilePath
      : controlledSelectedFilePath
  const fullscreenPane =
    controlledFullscreenPane === undefined ? localFullscreenPane : controlledFullscreenPane

  const setActiveTab = useCallback(
    (nextActiveTab: EntityPanelTab) => {
      if (controlledActiveTab === undefined) {
        setLocalActiveTab(nextActiveTab)
      }
      onActiveTabChange?.(nextActiveTab)
    },
    [controlledActiveTab, onActiveTabChange],
  )

  const setSelectedFilePath = useCallback(
    (nextSelectedFilePath: string | null) => {
      if (controlledSelectedFilePath === undefined) {
        setLocalSelectedFilePath(nextSelectedFilePath)
      }
      onSelectedFilePathChange?.(nextSelectedFilePath)
    },
    [controlledSelectedFilePath, onSelectedFilePathChange],
  )

  const setFullscreenPane = useCallback(
    (nextFullscreenPane: EntityFullscreenPane) => {
      if (controlledFullscreenPane === undefined) {
        setLocalFullscreenPane(nextFullscreenPane)
      }
      onFullscreenPaneChange?.(nextFullscreenPane)
    },
    [controlledFullscreenPane, onFullscreenPaneChange],
  )

  useEffect(() => {
    if (mode !== 'create') {
      return
    }

    loadedEntityRef.current = null
    setSaveError(null)
    setLocalSelectedFilePath(null)
    setIsEditing(true)
    setLockState('ready')
    setLocalActiveTab('content')
    setLocalFullscreenPane(null)
    setDraft({
      ...EMPTY_DRAFT,
      phase: initialPhase,
      state: initialState,
    })
  }, [initialPhase, initialState, mode])

  useEffect(() => {
    if (controlledActiveTab === undefined) {
      setLocalActiveTab('content')
    }
    if (controlledSelectedFilePath === undefined) {
      setLocalSelectedFilePath(null)
    }
    if (controlledFullscreenPane === undefined) {
      setLocalFullscreenPane(null)
    }
  }, [controlledActiveTab, controlledFullscreenPane, controlledSelectedFilePath, entityId, mode])

  useEffect(() => {
    if (mode !== 'edit' || !detailQuery.data) {
      return
    }

    loadedEntityRef.current = detailQuery.data
    setSaveError(null)
    if (!isEditing) {
      setLockState(ownedLockRef.current ? 'ready' : 'idle')
      setDraft(createDraftFromEntity(detailQuery.data))
    }
  }, [detailQuery.data, isEditing, mode])

  useEffect(() => {
    if (mode !== 'edit' || !isEditing || !detailQuery.data || !entityId) {
      return
    }

    const entity = detailQuery.data
    const lockedEntityId = entityId
    let active = true

    async function acquireLock() {
      if (ownedLockRef.current) {
        setLockState('ready')
        return
      }

      if (entity.locked) {
        setLockState('error')
        setSaveError('Entity is already locked')
        setIsEditing(false)
        return
      }

      setLockState('pending')

      try {
        await setEntityLocked(lockedEntityId, true)
        if (!active) {
          await setEntityLocked(lockedEntityId, false).catch(() => undefined)
          return
        }

        ownedLockRef.current = true
        setLockState('ready')
        await invalidateConsoleQueries(queryClient, lockedEntityId)
      } catch (error) {
        if (!active) {
          return
        }

        setLockState('error')
        setSaveError(formatError(error))
        setIsEditing(false)
      }
    }

    void acquireLock()

    return () => {
      active = false
    }
  }, [detailQuery.data, entityId, isEditing, mode, queryClient])

  useEffect(() => {
    if (mode !== 'edit' || !entityId) {
      return
    }

    return () => {
      if (!ownedLockRef.current) {
        return
      }

      ownedLockRef.current = false
      void setEntityLocked(entityId, false)
        .catch(() => undefined)
        .then(() => invalidateConsoleQueries(queryClient, entityId))
    }
  }, [entityId, mode, queryClient])

  useEffect(() => {
    if (!selectedFilePath && fullscreenPane === 'file') {
      setFullscreenPane(null)
    }
  }, [fullscreenPane, selectedFilePath, setFullscreenPane])

  useEffect(() => {
    if (mode === 'edit' && !detailQuery.data) {
      return
    }

    if (activeTab !== 'content') {
      if (selectedFilePath) {
        setSelectedFilePath(null)
      }
      return
    }

    const parsedJson =
      draft.format === 'json' ? tryParseJson(draft.rawContent) : { ok: false as const }
    const pathReferences =
      draft.format === 'json' && draft.editorMode === 'structured' && parsedJson.ok
        ? extractPathReferences(parsedJson.value)
        : []

    if (pathReferences.length === 0) {
      setSelectedFilePath(null)
      return
    }

    if (
      selectedFilePath &&
      pathReferences.some((reference) => reference.value === selectedFilePath)
    ) {
      return
    }

    setSelectedFilePath(pathReferences[0].value)
  }, [
    activeTab,
    detailQuery.data,
    draft.editorMode,
    draft.format,
    draft.rawContent,
    mode,
    selectedFilePath,
    setSelectedFilePath,
  ])

  const parsedJson =
    draft.format === 'json' ? tryParseJson(draft.rawContent) : { ok: false as const }

  const pathReferences =
    draft.format === 'json' && draft.editorMode === 'structured' && parsedJson.ok
      ? extractPathReferences(parsedJson.value)
      : []

  const currentPhase = workflow.phases.find((phase) => phase.name === draft.phase)
  const stateOptions = currentPhase
    ? [...currentPhase.states, ...currentPhase.reserved_states]
    : []
  const originalEntity = loadedEntityRef.current
  const currentEntity = detailQuery.data
    ? {
        ...detailQuery.data,
        ...(summary ?? {}),
      }
    : (summary ?? null)
  const isPaused = currentEntity?.paused ?? false
  const isProcessing = currentEntity?.processing ?? false
  const activeCommand = currentEntity?.active_command ?? null
  const commandElapsed = useElapsedTime(activeCommand?.started_at ?? null, isProcessing)
  const readOnly = mode === 'edit' && (!isEditing || lockState !== 'ready')
  const hasChanges =
    mode === 'create'
      ? draft.id.trim().length > 0 ||
        draft.rawContent.length > 0 ||
        draft.format !== 'text' ||
        draft.phase !== initialPhase ||
        draft.state !== initialState
      : Boolean(
          originalEntity &&
            (draft.phase !== originalEntity.phase ||
              draft.state !== originalEntity.state ||
              draft.format !== originalEntity.format ||
              draft.rawContent !== originalEntity.content),
        )

  async function handleSave() {
    setSaveError(null)

    if (draft.id.trim().length === 0) {
      setSaveError('Entity id is required')
      return
    }

    if (draft.format === 'json') {
      const parsed = tryParseJson(draft.rawContent)
      if (!parsed.ok) {
        setSaveError(parsed.error)
        return
      }
    }

    if (lockState === 'pending') {
      setSaveError('Lock is still being acquired')
      return
    }

    setIsSaving(true)

    try {
      if (mode === 'create') {
        await createEntity({
          id: draft.id.trim(),
          phase: draft.phase,
          state: draft.state,
          format: draft.format,
          content: draft.rawContent,
        })
      } else if (entityId) {
        const payload: {
          phase?: string
          state?: string
          format?: ContentFormat
          content?: string
        } = {}

        if (originalEntity && draft.phase !== originalEntity.phase) {
          payload.phase = draft.phase
        }
        if (originalEntity && draft.state !== originalEntity.state) {
          payload.state = draft.state
        }
        if (
          originalEntity &&
          (draft.format !== originalEntity.format ||
            draft.rawContent !== originalEntity.content)
        ) {
          payload.format = draft.format
          payload.content = draft.rawContent
        }

        if (Object.keys(payload).length > 0) {
          await updateEntity(entityId, payload)
        }
      }

      await invalidateConsoleQueries(queryClient, entityId ?? draft.id.trim())
      onClose()
    } catch (error) {
      setSaveError(formatError(error))
    } finally {
      setIsSaving(false)
    }
  }

  async function handleTogglePaused() {
    if (mode !== 'edit' || !entityId || !currentEntity) {
      return
    }

    if (
      !currentEntity.paused &&
      currentEntity.processing &&
      !window.confirm(
        'This entity is currently running. Pausing it will send SIGTERM to the active shell command and leave the entity in place. Continue?',
      )
    ) {
      return
    }

    setSaveError(null)
    setIsPausePending(true)

    try {
      await setEntityPaused(entityId, !currentEntity.paused)
      await invalidateConsoleQueries(queryClient, entityId)
    } catch (error) {
      setSaveError(formatError(error))
    } finally {
      setIsPausePending(false)
    }
  }

  function handleEnterEditMode() {
    setSaveError(null)
    setLockState('pending')
    setIsEditing(true)
  }

  function handleActiveTabChange(nextActiveTab: EntityPanelTab) {
    setFullscreenPane(null)
    setActiveTab(nextActiveTab)
  }

  return (
    <Dialog.Root
      modal={presentation !== 'panel'}
      open
      onOpenChange={(open) => !open && onClose()}
    >
      <Dialog.Portal>
        <Dialog.Overlay
          className={clsx(
            'dialog-overlay',
            presentation === 'panel' && 'dialog-overlay--panel',
          )}
        />
        <Dialog.Content
          className={clsx(
            'surface surface--padding-none surface--radius-xl dialog-content entity-dialog',
            presentation === 'panel' && 'entity-dialog--panel',
            fullscreenPane && 'entity-dialog--fullscreen-child',
          )}
          onInteractOutside={(event) => {
            if (presentation === 'panel') {
              event.preventDefault()
            }
          }}
        >
          <SectionHeader
            className="dialog-header"
            eyebrow={mode === 'edit' ? 'Entity' : 'New entity'}
            title={
              <Dialog.Title className="dialog-title">
                {mode === 'edit' ? draft.id || entityId : 'Create entity'}
              </Dialog.Title>
            }
            actions={
              <>
                {mode === 'edit' && entityId ? (
                  <div className="segmented-control" role="tablist" aria-label="Entity panel">
                    <button
                      className={clsx(
                        'segmented-control__button',
                        activeTab === 'content' && 'is-active',
                      )}
                      role="tab"
                      type="button"
                      aria-selected={activeTab === 'content'}
                      onClick={() => handleActiveTabChange('content')}
                    >
                      Content
                    </button>
                    <button
                      className={clsx(
                        'segmented-control__button',
                        activeTab === 'logs' && 'is-active',
                      )}
                      role="tab"
                      type="button"
                      aria-selected={activeTab === 'logs'}
                      onClick={() => handleActiveTabChange('logs')}
                    >
                      Logs
                    </button>
                  </div>
                ) : null}

                {mode === 'edit' ? (
                  <>
                    <span
                      className={clsx(
                        'status-pill',
                        !isEditing
                          ? 'status-pill--neutral'
                          : lockState === 'ready'
                            ? 'status-pill--success'
                            : lockState === 'error'
                              ? 'status-pill--danger'
                              : 'status-pill--warning',
                      )}
                    >
                      <Lock size={14} />
                      {!isEditing
                        ? 'View only'
                        : lockState === 'ready'
                          ? 'Locked'
                          : lockState === 'error'
                            ? 'Lock failed'
                            : 'Locking'}
                    </span>
                    {isPaused ? (
                      <span className="status-pill status-pill--warning">
                        <Pause size={14} />
                        Paused
                      </span>
                    ) : null}
                    {isProcessing ? (
                      <span
                        className="status-pill status-pill--warning"
                        title={activeCommand?.command ?? 'Hook command is running'}
                      >
                        <Clock3 size={14} />
                        {commandElapsed ? `Running ${commandElapsed}` : 'Running'}
                      </span>
                    ) : null}
                  </>
                ) : null}

                <Dialog.Close className="icon-button" aria-label="Close">
                  <X size={16} />
                </Dialog.Close>
              </>
            }
          />

          <Dialog.Description className="visually-hidden">
            {mode === 'edit'
              ? 'Inspect and edit the entity content, phase, state, and linked files.'
              : 'Create a new entity with its initial content, phase, and state.'}
          </Dialog.Description>

          {mode === 'edit' && detailQuery.isLoading ? (
            <div className="dialog-body">
              <EmptyState
                className="dialog-loading"
                icon={<LoaderCircle className="spin" size={16} />}
              >
                Loading entity
              </EmptyState>
            </div>
          ) : mode === 'edit' && detailQuery.error ? (
            <div className="dialog-body">
              <div className="inline-error">{formatError(detailQuery.error)}</div>
            </div>
          ) : (
            <>
              <div className="dialog-body">
                {activeTab === 'logs' && mode === 'edit' && entityId ? (
                  <EntityLogViewer
                    entityId={entityId}
                    fullscreen={fullscreenPane === 'logs'}
                    onCloseFullscreen={() => setFullscreenPane(null)}
                    onOpenFullscreen={() => setFullscreenPane('logs')}
                  />
                ) : (
                  <div className="entity-dialog__layout">
                    <section className="entity-form">
                      <div className="entity-form__grid">
                        <label className="field">
                          <span className="field__label">Entity</span>
                          <input
                            className="field__input field__input--mono"
                            disabled={mode === 'edit'}
                            value={draft.id}
                            onChange={(event) => {
                              setDraft((current) => ({
                                ...current,
                                id: event.target.value,
                              }))
                            }}
                          />
                        </label>

                        <label className="field">
                          <span className="field__label">Phase</span>
                          <select
                            className="field__input"
                            disabled={readOnly}
                            value={draft.phase}
                            onChange={(event) => {
                              const nextPhase = event.target.value
                              const nextPhaseConfig = workflow.phases.find(
                                (phase) => phase.name === nextPhase,
                              )
                              const nextStates = nextPhaseConfig
                                ? [...nextPhaseConfig.states, ...nextPhaseConfig.reserved_states]
                                : []

                              setDraft((current) => ({
                                ...current,
                                phase: nextPhase,
                                state:
                                  nextStates.includes(current.state) && current.state
                                    ? current.state
                                    : (nextStates[0] ?? ''),
                              }))
                            }}
                          >
                            {workflow.phases.map((phase) => (
                              <option key={phase.name} value={phase.name}>
                                {phase.name}
                              </option>
                            ))}
                          </select>
                        </label>

                        <label className="field">
                          <span className="field__label">State</span>
                          <select
                            className="field__input"
                            disabled={readOnly}
                            value={draft.state}
                            onChange={(event) => {
                              const nextState = event.target.value
                              setDraft((current) => ({ ...current, state: nextState }))
                            }}
                          >
                            {stateOptions.map((stateName) => (
                              <option key={stateName} value={stateName}>
                                {stateName}
                              </option>
                            ))}
                          </select>
                        </label>
                      </div>

                      <DocumentContentEditor
                        format={draft.format}
                        height={
                          presentation === 'panel' ? 'clamp(24rem, 48vh, 40rem)' : undefined
                        }
                        editorMode={draft.editorMode}
                        readOnly={readOnly}
                        rawContent={draft.rawContent}
                        onFormatChange={(nextFormat) => {
                          setDraft((current) => ({
                            ...current,
                            format: nextFormat,
                            editorMode: nextFormat === 'json' ? current.editorMode : 'raw',
                            rawContent:
                              nextFormat === 'json' && current.rawContent.trim().length === 0
                                ? '{}'
                                : current.rawContent,
                          }))
                        }}
                        onEditorModeChange={(nextMode) => {
                          setDraft((current) => ({ ...current, editorMode: nextMode }))
                        }}
                        onRawContentChange={(rawContent) => {
                          setDraft((current) => ({ ...current, rawContent }))
                        }}
                      />

                      {pathReferences.length > 0 ? (
                        <div className="linked-file-list">
                          <SectionHeader
                            className="linked-file-list__header"
                            eyebrow="Referenced files"
                            actions={
                              <span className="status-pill status-pill--neutral">
                                <FileJson2 size={14} />
                                {pathReferences.length}
                              </span>
                            }
                          />
                          <div className="linked-file-list__items">
                            {pathReferences.map((reference) => (
                              <button
                                key={`${reference.location}:${reference.value}`}
                                className={clsx(
                                  'linked-file-list__item',
                                  selectedFilePath === reference.value && 'is-active',
                                )}
                                type="button"
                                onClick={() => setSelectedFilePath(reference.value)}
                              >
                                <div className="linked-file-list__item-content">
                                  <span className="linked-file-list__key">
                                    {reference.location}
                                  </span>
                                  <span className="linked-file-list__path">
                                    {reference.value}
                                  </span>
                                </div>
                                <ChevronRight size={16} />
                              </button>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </section>

                    {selectedFilePath || presentation !== 'panel' ? (
                      <section className="entity-dialog__side">
                        {selectedFilePath ? (
                          <LinkedFileEditor
                            path={selectedFilePath}
                            readOnly={readOnly}
                            fullscreen={fullscreenPane === 'file'}
                            onCloseFullscreen={() => setFullscreenPane(null)}
                            onOpenFullscreen={() => setFullscreenPane('file')}
                          />
                        ) : (
                          <EmptyState className="panel-placeholder" icon={<Plus size={16} />}>
                            No linked file selected
                          </EmptyState>
                        )}
                      </section>
                    ) : null}
                  </div>
                )}

                {saveError ? <div className="inline-error">{saveError}</div> : null}
              </div>

              <footer className="dialog-footer">
                <div className="dialog-footer__secondary">
                  <button className="button button--ghost" type="button" onClick={onClose}>
                    {mode === 'edit' && !isEditing ? 'Close' : 'Cancel'}
                  </button>
                  {mode === 'edit' && entityId ? (
                    <button
                      className={clsx(
                        'button button--ghost',
                        isProcessing && !isPaused && 'button--warning',
                      )}
                      type="button"
                      disabled={isPausePending || detailQuery.isLoading}
                      onClick={() => void handleTogglePaused()}
                    >
                      {isPausePending ? (
                        <LoaderCircle className="spin" size={16} />
                      ) : isPaused ? (
                        <Play size={16} />
                      ) : (
                        <Pause size={16} />
                      )}
                      {isPaused ? 'Resume' : 'Pause'}
                    </button>
                  ) : null}
                </div>
                {mode === 'edit' && !isEditing ? (
                  <button
                    className="button button--primary"
                    type="button"
                    disabled={detailQuery.isLoading || Boolean(detailQuery.error)}
                    onClick={handleEnterEditMode}
                  >
                    <FilePenLine size={16} />
                    Edit entity
                  </button>
                ) : (
                  <button
                    className="button button--primary"
                    type="button"
                    disabled={
                      isSaving ||
                      !hasChanges ||
                      (mode === 'edit' && lockState !== 'ready') ||
                      detailQuery.isLoading
                    }
                    onClick={() => void handleSave()}
                  >
                    {mode === 'create' ? <Plus size={16} /> : <Save size={16} />}
                    {isSaving
                      ? 'Saving'
                      : mode === 'create'
                        ? 'Create entity'
                        : 'Save entity'}
                  </button>
                )}
              </footer>
            </>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function createDraftFromEntity(entity: EntityDetail): EntityDraft {
  return {
    id: entity.id,
    phase: entity.phase,
    state: entity.state,
    format: entity.format,
    rawContent: entity.content,
    editorMode: entity.format === 'json' ? 'structured' : 'raw',
  }
}

function useElapsedTime(startedAt: string | null, running: boolean) {
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    if (!startedAt || !running) {
      return
    }

    const intervalId = window.setInterval(() => setNow(Date.now()), 1_000)
    return () => window.clearInterval(intervalId)
  }, [running, startedAt])

  if (!startedAt || !running) {
    return null
  }

  const startedAtMs = Date.parse(startedAt)
  if (Number.isNaN(startedAtMs)) {
    return null
  }

  return formatElapsedSeconds(Math.max(0, Math.floor((now - startedAtMs) / 1_000)))
}

function formatElapsedSeconds(totalSeconds: number) {
  const hours = Math.floor(totalSeconds / 3_600)
  const minutes = Math.floor((totalSeconds % 3_600) / 60)
  const seconds = totalSeconds % 60

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`
  }

  if (minutes > 0) {
    return `${minutes}m ${seconds}s`
  }

  return `${seconds}s`
}

function formatError(error: unknown) {
  if (error instanceof ApiError) {
    return error.message
  }

  if (error instanceof Error) {
    return error.message
  }

  return 'Request failed'
}

async function invalidateConsoleQueries(
  queryClient: ReturnType<typeof useQueryClient>,
  entityId: string,
) {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: queryKeys.entities() }),
    queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus() }),
    queryClient.invalidateQueries({ queryKey: queryKeys.entity(entityId) }),
  ])
}
