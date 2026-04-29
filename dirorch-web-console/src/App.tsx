import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, LoaderCircle, Settings } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import {
  ApiError,
  getEntities,
  getWorkflow,
  getWorkflowStatus,
  pauseWorkflow,
  queryKeys,
  resumeWorkflow,
  updateEntity,
} from './api/dirorch'
import {
  getBackendEndpoint,
  getDefaultBackendEndpoint,
  resetBackendEndpoint,
  setBackendEndpoint,
} from './api/backendEndpoint'
import type { EntitySummary } from './api/types'
import {
  EntityEditorModal,
  type EntityFullscreenPane,
  type EntityPanelTab,
} from './components/EntityEditorModal'
import { SettingsModal } from './components/SettingsModal'
import { WorkflowOverview } from './components/WorkflowOverview'
import { EmptyState } from './components/ui/EmptyState'
import './App.css'

type ModalState =
  | {
      mode: 'create'
      phase: string
      state: string
    }
  | null

interface ConsoleNavigationState {
  entityId: string | null
  entityView: EntityPanelTab
  selectedFilePath: string | null
  fullscreenPane: EntityFullscreenPane
}

type NavigationHistoryMode = 'push' | 'replace'

const ENTITY_PARAM = 'entity'
const ENTITY_VIEW_PARAM = 'view'
const ENTITY_FILE_PARAM = 'file'
const FULLSCREEN_PARAM = 'fullscreen'

export default function App() {
  const queryClient = useQueryClient()
  const defaultBackendEndpoint = getDefaultBackendEndpoint()
  const [backendEndpoint, setBackendEndpointState] = useState(getBackendEndpoint)
  const [navigationState, setNavigationState] = useUrlNavigationState()
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [modalState, setModalState] = useState<ModalState>(null)
  const [moveError, setMoveError] = useState<string | null>(null)
  const [usePanelEditor, setUsePanelEditor] = useState(() =>
    typeof window === 'undefined' ? false : window.matchMedia('(min-width: 1280px)').matches,
  )

  useEffect(() => {
    const mediaQuery = window.matchMedia('(min-width: 1280px)')
    const handleChange = () => setUsePanelEditor(mediaQuery.matches)

    handleChange()
    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [])

  const workflowQuery = useQuery({
    queryKey: queryKeys.workflow(),
    queryFn: getWorkflow,
    staleTime: 60_000,
  })

  const statusQuery = useQuery({
    queryKey: queryKeys.workflowStatus(),
    queryFn: getWorkflowStatus,
    refetchInterval: 2_000,
  })

  const entitiesQuery = useQuery({
    queryKey: queryKeys.entities(),
    queryFn: getEntities,
    refetchInterval: 2_000,
  })

  const selectedEntitySummary =
    navigationState.entityId && entitiesQuery.data
      ? entitiesQuery.data.entities.find((entity) => entity.id === navigationState.entityId) ?? null
      : null

  useEffect(() => {
    if (!entitiesQuery.data || !navigationState.entityId || selectedEntitySummary) {
      return
    }

    setNavigationState(
      {
        entityId: null,
        entityView: 'content',
        selectedFilePath: null,
        fullscreenPane: null,
      },
      'replace',
    )
  }, [entitiesQuery.data, navigationState.entityId, selectedEntitySummary, setNavigationState])

  const moveEntityMutation = useMutation({
    mutationFn: async ({
      entity,
      phase,
      state,
    }: {
      entity: EntitySummary
      phase: string
      state: string
    }) => {
      await updateEntity(entity.id, { phase, state })
    },
    onSuccess: async (_, { entity }) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.entities() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.entity(entity.id) }),
      ])
    },
  })

  const pauseWorkflowMutation = useMutation({
    mutationFn: pauseWorkflow,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.entities() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus() }),
      ])
    },
  })

  const resumeWorkflowMutation = useMutation({
    mutationFn: resumeWorkflow,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.entities() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus() }),
      ])
    },
  })

  function handleBackendEndpointChange(nextEndpoint: string) {
    const normalizedEndpoint = setBackendEndpoint(nextEndpoint)
    setBackendEndpointState(normalizedEndpoint)
    clearViewerNavigation()
    setModalState(null)
    setMoveError(null)
    setSettingsOpen(false)
    queryClient.clear()
  }

  function handleBackendEndpointReset() {
    const normalizedEndpoint = resetBackendEndpoint()
    setBackendEndpointState(normalizedEndpoint)
    clearViewerNavigation()
    setModalState(null)
    setMoveError(null)
    setSettingsOpen(false)
    queryClient.clear()
  }

  async function handleRefresh() {
    await Promise.all([
      workflowQuery.refetch(),
      statusQuery.refetch(),
      entitiesQuery.refetch(),
    ])
  }

  async function handleMoveEntity(entity: EntitySummary, phase: string, state: string) {
    if (entity.processing || entity.locked || (entity.phase === phase && entity.state === state)) {
      return
    }

    setMoveError(null)

    try {
      await moveEntityMutation.mutateAsync({ entity, phase, state })
    } catch (error) {
      setMoveError(formatError(error))
    }
  }

  async function handlePauseWorkflow() {
    if (
      !window.confirm(
        'Emergency stop will pause the workflow engine and terminate running entity hook commands. Continue?',
      )
    ) {
      return
    }

    setMoveError(null)

    try {
      await pauseWorkflowMutation.mutateAsync()
    } catch (error) {
      setMoveError(formatError(error))
    }
  }

  async function handleResumeWorkflow() {
    setMoveError(null)

    try {
      await resumeWorkflowMutation.mutateAsync()
    } catch (error) {
      setMoveError(formatError(error))
    }
  }

  function clearViewerNavigation() {
    setNavigationState(
      {
        entityId: null,
        entityView: 'content',
        selectedFilePath: null,
        fullscreenPane: null,
      },
      'replace',
    )
  }

  function handleSelectEntity(summary: EntitySummary) {
    setModalState(null)
    setNavigationState(
      {
        entityId: summary.id,
        entityView: 'content',
        selectedFilePath: null,
        fullscreenPane: null,
      },
      'push',
    )
  }

  function handleCreateEntity(phase: string, state: string) {
    clearViewerNavigation()
    setModalState({ mode: 'create', phase, state })
  }

  function handleEntityViewChange(entityView: EntityPanelTab) {
    setNavigationState(
      (current) => ({
        ...current,
        entityView,
        selectedFilePath: entityView === 'content' ? current.selectedFilePath : null,
        fullscreenPane: null,
      }),
      'replace',
    )
  }

  function handleSelectedFilePathChange(selectedFilePath: string | null) {
    setNavigationState(
      (current) => ({
        ...current,
        selectedFilePath: current.entityView === 'content' ? selectedFilePath : null,
        fullscreenPane: selectedFilePath ? current.fullscreenPane : null,
      }),
      'replace',
    )
  }

  function handleFullscreenPaneChange(fullscreenPane: EntityFullscreenPane) {
    setNavigationState(
      (current) => ({
        ...current,
        fullscreenPane,
      }),
      'replace',
    )
  }

  const loading =
    !workflowQuery.data || !statusQuery.data || !entitiesQuery.data || workflowQuery.isLoading
  const error = workflowQuery.error ?? statusQuery.error ?? entitiesQuery.error
  const settingsModal = (
    <SettingsModal
      backendEndpoint={backendEndpoint}
      defaultBackendEndpoint={defaultBackendEndpoint}
      onBackendEndpointChange={handleBackendEndpointChange}
      onBackendEndpointReset={handleBackendEndpointReset}
      onOpenChange={setSettingsOpen}
      open={settingsOpen}
    />
  )

  if (loading) {
    return (
      <>
        {settingsModal}
        <main className="app-state">
          <EmptyState icon={<LoaderCircle className="spin" size={18} />}>
            Loading workflow console
          </EmptyState>
          <button className="button button--ghost" type="button" onClick={() => setSettingsOpen(true)}>
            <Settings size={16} />
            Settings
          </button>
        </main>
      </>
    )
  }

  if (error) {
    return (
      <>
        {settingsModal}
        <main className="app-state app-state--error">
          <EmptyState icon={<AlertTriangle size={18} />}>{formatError(error)}</EmptyState>
          <div className="app-state__actions">
            <button className="button button--ghost" type="button" onClick={() => void handleRefresh()}>
              Retry
            </button>
            <button className="button button--ghost" type="button" onClick={() => setSettingsOpen(true)}>
              <Settings size={16} />
              Settings
            </button>
          </div>
        </main>
      </>
    )
  }

  return (
    <>
      {settingsModal}

      {moveError ? (
        <main className="app-notice">
          <div className="inline-error">{moveError}</div>
        </main>
      ) : null}

      <div
        className={
          (modalState || selectedEntitySummary) && usePanelEditor
            ? 'app-workspace app-workspace--editor-open'
            : 'app-workspace'
        }
      >
        <div className="app-workspace__workflow">
          <WorkflowOverview
            entities={entitiesQuery.data.entities}
            isPausingWorkflow={pauseWorkflowMutation.isPending}
            isResumingWorkflow={resumeWorkflowMutation.isPending}
            isRefreshing={statusQuery.isRefetching || entitiesQuery.isRefetching}
            movingEntityId={moveEntityMutation.isPending ? moveEntityMutation.variables?.entity.id ?? null : null}
            onCreateEntity={handleCreateEntity}
            onMoveEntity={(entity, phase, state) => void handleMoveEntity(entity, phase, state)}
            onOpenSettings={() => setSettingsOpen(true)}
            onPauseWorkflow={() => void handlePauseWorkflow()}
            onRefresh={() => void handleRefresh()}
            onResumeWorkflow={() => void handleResumeWorkflow()}
            onSelectEntity={handleSelectEntity}
            status={statusQuery.data}
            workflow={workflowQuery.data}
          />
        </div>
        {(modalState || selectedEntitySummary) && usePanelEditor ? (
          <div className="app-workspace__editor-slot" aria-hidden="true" />
        ) : null}
      </div>

      {modalState?.mode === 'create' ? (
        <EntityEditorModal
          initialPhase={modalState.phase}
          initialState={modalState.state}
          mode="create"
          presentation={usePanelEditor ? 'panel' : 'modal'}
          workflow={workflowQuery.data}
          onClose={() => {
            setModalState(null)
            void handleRefresh()
          }}
        />
      ) : null}

      {selectedEntitySummary ? (
        <EntityEditorModal
          activeTab={navigationState.entityView}
          fullscreenPane={navigationState.fullscreenPane}
          initialPhase={selectedEntitySummary.phase}
          initialState={selectedEntitySummary.state}
          mode="edit"
          presentation={usePanelEditor ? 'panel' : 'modal'}
          selectedFilePath={navigationState.selectedFilePath}
          summary={selectedEntitySummary}
          workflow={workflowQuery.data}
          onActiveTabChange={handleEntityViewChange}
          onClose={() => {
            clearViewerNavigation()
            void handleRefresh()
          }}
          onFullscreenPaneChange={handleFullscreenPaneChange}
          onSelectedFilePathChange={handleSelectedFilePathChange}
        />
      ) : null}
    </>
  )
}

function useUrlNavigationState() {
  const [navigationState, setNavigationStateValue] = useState(readUrlNavigationState)
  const navigationStateRef = useRef(navigationState)

  useEffect(() => {
    navigationStateRef.current = navigationState
  }, [navigationState])

  useEffect(() => {
    function handlePopState() {
      const nextState = readUrlNavigationState()
      navigationStateRef.current = nextState
      setNavigationStateValue(nextState)
    }

    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  const setNavigationState = useCallback(
    (
      next:
        | ConsoleNavigationState
        | ((current: ConsoleNavigationState) => ConsoleNavigationState),
      historyMode: NavigationHistoryMode = 'replace',
    ) => {
      const currentState = navigationStateRef.current
      const nextState = typeof next === 'function' ? next(currentState) : next

      navigationStateRef.current = nextState
      writeUrlNavigationState(nextState, historyMode)
      setNavigationStateValue(nextState)
    },
    [],
  )

  return [navigationState, setNavigationState] as const
}

function readUrlNavigationState(): ConsoleNavigationState {
  if (typeof window === 'undefined') {
    return createDefaultNavigationState()
  }

  const params = new URLSearchParams(window.location.search)
  const entityId = params.get(ENTITY_PARAM)

  if (!entityId) {
    return createDefaultNavigationState()
  }

  const entityView: EntityPanelTab =
    params.get(ENTITY_VIEW_PARAM) === 'logs' ? 'logs' : 'content'
  const selectedFilePath =
    entityView === 'content' ? emptyToNull(params.get(ENTITY_FILE_PARAM)) : null
  const requestedFullscreen = params.get(FULLSCREEN_PARAM)
  const fullscreenPane: EntityFullscreenPane =
    requestedFullscreen === 'logs' && entityView === 'logs'
      ? 'logs'
      : requestedFullscreen === 'file' && entityView === 'content'
        ? 'file'
        : null

  return {
    entityId,
    entityView,
    selectedFilePath,
    fullscreenPane,
  }
}

function writeUrlNavigationState(
  navigationState: ConsoleNavigationState,
  historyMode: NavigationHistoryMode,
) {
  if (typeof window === 'undefined') {
    return
  }

  const url = new URL(window.location.href)
  url.searchParams.delete(ENTITY_PARAM)
  url.searchParams.delete(ENTITY_VIEW_PARAM)
  url.searchParams.delete(ENTITY_FILE_PARAM)
  url.searchParams.delete(FULLSCREEN_PARAM)

  if (navigationState.entityId) {
    url.searchParams.set(ENTITY_PARAM, navigationState.entityId)

    if (navigationState.entityView === 'logs') {
      url.searchParams.set(ENTITY_VIEW_PARAM, 'logs')
    }

    if (navigationState.entityView === 'content' && navigationState.selectedFilePath) {
      url.searchParams.set(ENTITY_FILE_PARAM, navigationState.selectedFilePath)
    }

    if (navigationState.fullscreenPane) {
      url.searchParams.set(FULLSCREEN_PARAM, navigationState.fullscreenPane)
    }
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`
  const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`

  if (nextUrl === currentUrl) {
    return
  }

  if (historyMode === 'push') {
    window.history.pushState(null, '', nextUrl)
    return
  }

  window.history.replaceState(null, '', nextUrl)
}

function createDefaultNavigationState(): ConsoleNavigationState {
  return {
    entityId: null,
    entityView: 'content',
    selectedFilePath: null,
    fullscreenPane: null,
  }
}

function emptyToNull(value: string | null) {
  return value && value.length > 0 ? value : null
}

function formatError(error: unknown) {
  if (error instanceof ApiError) {
    return error.message
  }

  if (error instanceof Error) {
    return error.message
  }

  return 'Failed to load data'
}
