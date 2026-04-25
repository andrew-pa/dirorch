import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, LoaderCircle } from 'lucide-react'
import { useEffect, useState } from 'react'

import {
  ApiError,
  getEntities,
  getWorkflow,
  getWorkflowStatus,
  pauseWorkflow,
  queryKeys,
  updateEntity,
} from './api/dirorch'
import type { EntitySummary } from './api/types'
import { EntityEditorModal } from './components/EntityEditorModal'
import { WorkflowOverview } from './components/WorkflowOverview'
import { EmptyState } from './components/ui/EmptyState'
import './App.css'

type ModalState =
  | {
      mode: 'create'
      phase: string
      state: string
    }
  | {
      mode: 'edit'
      summary: EntitySummary
    }
  | null

export default function App() {
  const queryClient = useQueryClient()
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
    queryKey: queryKeys.workflow,
    queryFn: getWorkflow,
    staleTime: 60_000,
  })

  const statusQuery = useQuery({
    queryKey: queryKeys.workflowStatus,
    queryFn: getWorkflowStatus,
    refetchInterval: 2_000,
  })

  const entitiesQuery = useQuery({
    queryKey: queryKeys.entities,
    queryFn: getEntities,
    refetchInterval: 2_000,
  })

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
        queryClient.invalidateQueries({ queryKey: queryKeys.entities }),
        queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus }),
        queryClient.invalidateQueries({ queryKey: queryKeys.entity(entity.id) }),
      ])
    },
  })

  const pauseWorkflowMutation = useMutation({
    mutationFn: pauseWorkflow,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.entities }),
        queryClient.invalidateQueries({ queryKey: queryKeys.workflowStatus }),
      ])
    },
  })

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
        'Emergency stop will pause every entity and terminate any running entity hook commands. Continue?',
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

  const loading =
    !workflowQuery.data || !statusQuery.data || !entitiesQuery.data || workflowQuery.isLoading
  const error = workflowQuery.error ?? statusQuery.error ?? entitiesQuery.error

  if (loading) {
    return (
      <main className="app-state">
        <EmptyState icon={<LoaderCircle className="spin" size={18} />}>
          Loading workflow console
        </EmptyState>
      </main>
    )
  }

  if (error) {
    return (
      <main className="app-state app-state--error">
        <EmptyState icon={<AlertTriangle size={18} />}>{formatError(error)}</EmptyState>
        <button className="button button--ghost" type="button" onClick={() => void handleRefresh()}>
          Retry
        </button>
      </main>
    )
  }

  return (
    <>
      {moveError ? (
        <main className="app-notice">
          <div className="inline-error">{moveError}</div>
        </main>
      ) : null}

      <div
        className={
          modalState && usePanelEditor
            ? 'app-workspace app-workspace--editor-open'
            : 'app-workspace'
        }
      >
        <div className="app-workspace__workflow">
          <WorkflowOverview
            entities={entitiesQuery.data.entities}
            isPausingWorkflow={pauseWorkflowMutation.isPending}
            isRefreshing={statusQuery.isRefetching || entitiesQuery.isRefetching}
            movingEntityId={moveEntityMutation.isPending ? moveEntityMutation.variables?.entity.id ?? null : null}
            onCreateEntity={(phase, state) => setModalState({ mode: 'create', phase, state })}
            onMoveEntity={(entity, phase, state) => void handleMoveEntity(entity, phase, state)}
            onPauseWorkflow={() => void handlePauseWorkflow()}
            onRefresh={() => void handleRefresh()}
            onSelectEntity={(summary) => setModalState({ mode: 'edit', summary })}
            status={statusQuery.data}
            workflow={workflowQuery.data}
          />
        </div>
        {modalState && usePanelEditor ? (
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

      {modalState?.mode === 'edit' ? (
        <EntityEditorModal
          initialPhase={modalState.summary.phase}
          initialState={modalState.summary.state}
          mode="edit"
          presentation={usePanelEditor ? 'panel' : 'modal'}
          summary={modalState.summary}
          workflow={workflowQuery.data}
          onClose={() => {
            setModalState(null)
            void handleRefresh()
          }}
        />
      ) : null}
    </>
  )
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
