import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, LoaderCircle } from 'lucide-react'
import { useState } from 'react'

import { ApiError, getEntities, getWorkflow, getWorkflowStatus, queryKeys } from './api/dirorch'
import type { EntitySummary } from './api/types'
import { EntityEditorModal } from './components/EntityEditorModal'
import { WorkflowOverview } from './components/WorkflowOverview'
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
  const [modalState, setModalState] = useState<ModalState>(null)

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

  async function handleRefresh() {
    await Promise.all([
      workflowQuery.refetch(),
      statusQuery.refetch(),
      entitiesQuery.refetch(),
    ])
  }

  const loading =
    !workflowQuery.data || !statusQuery.data || !entitiesQuery.data || workflowQuery.isLoading
  const error = workflowQuery.error ?? statusQuery.error ?? entitiesQuery.error

  if (loading) {
    return (
      <main className="app-state">
        <LoaderCircle className="spin" size={18} />
        <span>Loading workflow console</span>
      </main>
    )
  }

  if (error) {
    return (
      <main className="app-state app-state--error">
        <AlertTriangle size={18} />
        <span>{formatError(error)}</span>
        <button className="button button--ghost" type="button" onClick={() => void handleRefresh()}>
          Retry
        </button>
      </main>
    )
  }

  return (
    <>
      <WorkflowOverview
        entities={entitiesQuery.data.entities}
        isRefreshing={statusQuery.isRefetching || entitiesQuery.isRefetching}
        onCreateEntity={(phase, state) => setModalState({ mode: 'create', phase, state })}
        onRefresh={() => void handleRefresh()}
        onSelectEntity={(summary) => setModalState({ mode: 'edit', summary })}
        status={statusQuery.data}
        workflow={workflowQuery.data}
      />

      {modalState?.mode === 'create' ? (
        <EntityEditorModal
          initialPhase={modalState.phase}
          initialState={modalState.state}
          mode="create"
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
