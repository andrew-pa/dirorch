import { useEffect, useEffectEvent, useRef, useState } from 'react'
import { LoaderCircle, Radio, WifiOff } from 'lucide-react'
import { FitAddon } from '@xterm/addon-fit'
import { Terminal } from '@xterm/xterm'
import '@xterm/xterm/css/xterm.css'

import { getEntityLog, openEntityLogEvents } from '../api/dirorch'
import type {
  EntityLogAppendEvent,
  EntityLogPayload,
  EntityLogStatusEvent,
} from '../api/types'
import { ApiError } from '../api/dirorch'
import { EmptyState } from './ui/EmptyState'

interface EntityLogViewerProps {
  entityId: string
}

type SnapshotState = 'loading' | 'ready' | 'error'
type ConnectionState = 'connecting' | 'live' | 'disconnected'

export function EntityLogViewer({ entityId }: EntityLogViewerProps) {
  const hostRef = useRef<HTMLDivElement | null>(null)
  const terminalRef = useRef<Terminal | null>(null)
  const autoFollowRef = useRef(true)

  const [snapshotState, setSnapshotState] = useState<SnapshotState>('loading')
  const [connectionState, setConnectionState] = useState<ConnectionState>('connecting')
  const [processing, setProcessing] = useState(false)
  const [hasContent, setHasContent] = useState(false)
  const [autoFollow, setAutoFollow] = useState(true)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)

  useEffect(() => {
    autoFollowRef.current = autoFollow
  }, [autoFollow])

  const appendText = useEffectEvent((text: string) => {
    const terminal = terminalRef.current
    if (!terminal || text.length === 0) {
      return
    }

    terminal.write(text, () => {
      if (autoFollowRef.current) {
        terminal.scrollToBottom()
      }
    })
    setHasContent(true)
  })

  const resetTerminal = useEffectEvent(() => {
    const terminal = terminalRef.current
    if (!terminal) {
      return
    }

    terminal.reset()
    terminal.clear()
  })

  useEffect(() => {
    const host = hostRef.current
    if (!host) {
      return
    }

    const terminal = new Terminal({
      convertEol: true,
      disableStdin: true,
      allowTransparency: true,
      fontFamily: '"JetBrains Mono", monospace',
      fontSize: 13,
      lineHeight: 1.2,
      theme: {
        background: '#050a11',
        foreground: '#d8ecf6',
        cursor: '#45e8ff',
        black: '#07111b',
        red: '#ff7b88',
        green: '#8df2a8',
        yellow: '#ffd166',
        blue: '#7cb9ff',
        magenta: '#ff8fd8',
        cyan: '#45e8ff',
        white: '#e6fbff',
        brightBlack: '#4f6d80',
        brightRed: '#ff9fa9',
        brightGreen: '#b2ffd0',
        brightYellow: '#ffe29a',
        brightBlue: '#a9d5ff',
        brightMagenta: '#ffb6ea',
        brightCyan: '#9ff6ff',
        brightWhite: '#ffffff',
      },
    })
    const fitAddon = new FitAddon()
    terminal.loadAddon(fitAddon)
    terminal.open(host)
    fitAddon.fit()

    const scrollDisposable = terminal.onScroll(() => {
      setAutoFollow(isNearBottom(terminal))
    })
    const resizeObserver = new ResizeObserver(() => {
      fitAddon.fit()
      if (autoFollowRef.current) {
        terminal.scrollToBottom()
      }
    })
    resizeObserver.observe(host)

    terminalRef.current = terminal
    return () => {
      resizeObserver.disconnect()
      scrollDisposable.dispose()
      fitAddon.dispose()
      terminal.dispose()
      terminalRef.current = null
    }
  }, [])

  useEffect(() => {
    const terminal = terminalRef.current
    if (!terminal) {
      return
    }

    let active = true
    let eventSource: EventSource | null = null

    async function load() {
      setSnapshotState('loading')
      setConnectionState('connecting')
      setProcessing(false)
      setHasContent(false)
      setErrorMessage(null)
      setAutoFollow(true)
      resetTerminal()

      try {
        const snapshot = await getEntityLog(entityId)
        if (!active) {
          return
        }

        applySnapshot(snapshot)
        eventSource = openEntityLogEvents(entityId, snapshot.next_offset)
        bindEventSource(eventSource)
      } catch (error) {
        if (!active) {
          return
        }

        setSnapshotState('error')
        setConnectionState('disconnected')
        setErrorMessage(formatError(error))
      }
    }

    function bindEventSource(source: EventSource) {
      source.onopen = () => {
        if (!active) {
          return
        }
        setConnectionState('live')
      }

      source.onerror = () => {
        if (!active) {
          return
        }
        setConnectionState('disconnected')
      }

      source.addEventListener('snapshot', (event) => {
        if (!active) {
          return
        }
        applySnapshot(parseEventPayload<EntityLogPayload>(event))
      })

      source.addEventListener('append', (event) => {
        if (!active) {
          return
        }
        const payload = parseEventPayload<EntityLogAppendEvent>(event)
        if (payload.text.length > 0) {
          appendText(payload.text)
        }
        setProcessing(payload.processing)
      })

      source.addEventListener('status', (event) => {
        if (!active) {
          return
        }
        const payload = parseEventPayload<EntityLogStatusEvent>(event)
        setProcessing(payload.processing)
      })
    }

    function applySnapshot(payload: EntityLogPayload) {
      if (payload.offset === 0) {
        resetTerminal()
      }
      if (payload.text.length > 0) {
        appendText(payload.text)
      }
      setProcessing(payload.processing)
      setHasContent(payload.exists || payload.text.length > 0)
      setSnapshotState('ready')
    }

    void load()

    return () => {
      active = false
      eventSource?.close()
    }
  }, [entityId])

  return (
    <section className="entity-log-viewer">
      <header className="entity-log-viewer__header">
        <div className="entity-log-viewer__status">
          <span
            className={`status-pill ${
              connectionState === 'live'
                ? 'status-pill--success'
                : connectionState === 'connecting'
                  ? 'status-pill--warning'
                  : 'status-pill--danger'
            }`}
          >
            {connectionState === 'live' ? <Radio size={14} /> : <WifiOff size={14} />}
            {connectionState === 'live'
              ? 'Live'
              : connectionState === 'connecting'
                ? 'Connecting'
                : 'Disconnected'}
          </span>
          <span
            className={`status-pill ${
              processing ? 'status-pill--warning' : 'status-pill--neutral'
            }`}
          >
            {processing ? <LoaderCircle className="spin" size={14} /> : <Radio size={14} />}
            {processing ? 'Running' : 'Idle'}
          </span>
          <span className="status-pill status-pill--neutral">
            {autoFollow ? 'Following output' : 'Scroll locked'}
          </span>
        </div>
      </header>

      <div className="entity-log-viewer__body">
        <div ref={hostRef} className="entity-log-viewer__terminal" />

        {snapshotState === 'loading' && !hasContent ? (
          <EmptyState className="entity-log-viewer__empty" icon={<LoaderCircle className="spin" size={16} />}>
            Loading log
          </EmptyState>
        ) : null}

        {snapshotState === 'error' && errorMessage ? (
          <div className="inline-error entity-log-viewer__error">{errorMessage}</div>
        ) : null}

        {snapshotState === 'ready' && !hasContent ? (
          <EmptyState className="entity-log-viewer__empty">
            No entity log exists yet
          </EmptyState>
        ) : null}
      </div>
    </section>
  )
}

function parseEventPayload<T>(event: Event) {
  const message = event as MessageEvent<string>
  return JSON.parse(message.data) as T
}

function isNearBottom(terminal: Terminal) {
  const buffer = terminal.buffer.active
  return buffer.baseY - buffer.viewportY <= 2
}

function formatError(error: unknown) {
  if (error instanceof ApiError) {
    return error.message
  }
  if (error instanceof Error) {
    return error.message
  }
  return 'Unable to load entity log'
}
