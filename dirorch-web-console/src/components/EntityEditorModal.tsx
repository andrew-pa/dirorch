import * as Dialog from '@radix-ui/react-dialog'
import clsx from 'clsx'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronRight,
  FilePenLine,
  FileJson2,
  LoaderCircle,
  Lock,
  Plus,
  Save,
  X,
} from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import {
  ApiError,
  createEntity,
  getEntity,
  queryKeys,
  setEntityLocked,
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
import { LinkedFileEditor } from './LinkedFileEditor'

interface EntityEditorModalProps {
  initialPhase: string
  initialState: string
  mode: 'create' | 'edit'
  onClose: () => void
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
  initialPhase,
  initialState,
  mode,
  onClose,
  summary,
  workflow,
}: EntityEditorModalProps) {
  const queryClient = useQueryClient()
  const entityId = summary?.id ?? null
  const detailQuery = useQuery({
    queryKey: entityId ? queryKeys.entity(entityId) : ['entity', 'draft'],
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
  const [selectedFilePath, setSelectedFilePath] = useState<string | null>(null)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [isEditing, setIsEditing] = useState(mode === 'create')
  const [lockState, setLockState] = useState<'idle' | 'pending' | 'ready' | 'error'>(
    mode === 'create' ? 'ready' : 'idle',
  )

  const loadedEntityRef = useRef<EntityDetail | null>(null)
  const ownedLockRef = useRef(false)

  useEffect(() => {
    if (mode !== 'create') {
      return
    }

    loadedEntityRef.current = null
    setSaveError(null)
    setSelectedFilePath(null)
    setIsEditing(true)
    setLockState('ready')
    setDraft({
      ...EMPTY_DRAFT,
      phase: initialPhase,
      state: initialState,
    })
  }, [initialPhase, initialState, mode])

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
  }, [draft.editorMode, draft.format, draft.rawContent, selectedFilePath])

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

  function handleEnterEditMode() {
    setSaveError(null)
    setLockState('pending')
    setIsEditing(true)
  }

  return (
    <Dialog.Root open onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content entity-dialog">
          <header className="dialog-header">
            <div>
              <div className="panel-eyebrow">{mode === 'edit' ? 'Entity' : 'New entity'}</div>
              <Dialog.Title className="dialog-title">
                {mode === 'edit' ? draft.id || entityId : 'Create entity'}
              </Dialog.Title>
            </div>

            <div className="dialog-header__actions">
              {mode === 'edit' ? (
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
              ) : null}

              <Dialog.Close className="icon-button" aria-label="Close">
                <X size={16} />
              </Dialog.Close>
            </div>
          </header>

          <Dialog.Description className="visually-hidden">
            {mode === 'edit'
              ? 'Inspect and edit the entity content, phase, state, and linked files.'
              : 'Create a new entity with its initial content, phase, and state.'}
          </Dialog.Description>

          {mode === 'edit' && detailQuery.isLoading ? (
            <div className="dialog-loading">
              <LoaderCircle className="spin" size={16} />
              Loading entity
            </div>
          ) : mode === 'edit' && detailQuery.error ? (
            <div className="inline-error">{formatError(detailQuery.error)}</div>
          ) : (
            <>
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
                      <div className="linked-file-list__header">
                        <span className="panel-eyebrow">Referenced files</span>
                        <span className="status-pill status-pill--neutral">
                          <FileJson2 size={14} />
                          {pathReferences.length}
                        </span>
                      </div>
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
                            <div>
                              <div className="linked-file-list__key">{reference.location}</div>
                              <div className="linked-file-list__path">{reference.value}</div>
                            </div>
                            <ChevronRight size={16} />
                          </button>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </section>

                <section className="entity-dialog__side">
                  {selectedFilePath ? (
                    <LinkedFileEditor path={selectedFilePath} readOnly={readOnly} />
                  ) : (
                    <div className="panel-placeholder">
                      <Plus size={16} />
                      No linked file selected
                    </div>
                  )}
                </section>
              </div>

              {saveError ? <div className="inline-error">{saveError}</div> : null}

              <footer className="dialog-footer">
                <button className="button button--ghost" type="button" onClick={onClose}>
                  {mode === 'edit' && !isEditing ? 'Close' : 'Cancel'}
                </button>
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
    queryClient.invalidateQueries({ queryKey: queryKeys.entities }),
    queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus }),
    queryClient.invalidateQueries({ queryKey: queryKeys.entity(entityId) }),
  ])
}
