import { AlertTriangle, RotateCcw, Save, Server } from 'lucide-react'
import type { FormEvent } from 'react'
import { useState } from 'react'

import { normalizeBackendEndpoint } from '../api/backendEndpoint'

interface BackendEndpointControlProps {
  defaultEndpoint: string
  endpoint: string
  onChange: (endpoint: string) => void
  onReset: () => void
}

export function BackendEndpointControl({
  defaultEndpoint,
  endpoint,
  onChange,
  onReset,
}: BackendEndpointControlProps) {
  const [draftEndpoint, setDraftEndpoint] = useState(endpoint)
  const [error, setError] = useState<string | null>(null)

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()

    try {
      onChange(normalizeBackendEndpoint(draftEndpoint))
      setError(null)
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : 'Invalid backend endpoint')
    }
  }

  function handleReset() {
    onReset()
    setDraftEndpoint(defaultEndpoint)
    setError(null)
  }

  return (
    <section className="backend-endpoint">
      <form className="backend-endpoint__form" onSubmit={handleSubmit}>
        <label className="backend-endpoint__field">
          <span className="backend-endpoint__label">
            <Server size={15} />
            Backend API
          </span>
          <input
            className="backend-endpoint__input"
            type="url"
            value={draftEndpoint}
            placeholder={defaultEndpoint}
            spellCheck={false}
            onChange={(event) => setDraftEndpoint(event.target.value)}
          />
        </label>
        <div className="backend-endpoint__actions">
          <button className="button button--primary" type="submit">
            <Save size={15} />
            Apply
          </button>
          <button className="button button--ghost" type="button" onClick={handleReset}>
            <RotateCcw size={15} />
            Default
          </button>
        </div>
      </form>
      {error ? (
        <div className="backend-endpoint__error">
          <AlertTriangle size={15} />
          {error}
        </div>
      ) : null}
    </section>
  )
}
