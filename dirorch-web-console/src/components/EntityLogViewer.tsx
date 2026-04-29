import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import clsx from 'clsx'
import { LoaderCircle, Maximize2, Minimize2, Radio, WifiOff } from 'lucide-react'
import { FitAddon } from '@xterm/addon-fit'
import type { ITerminalOptions, Terminal } from '@xterm/xterm'
import { useXTerm } from 'react-xtermjs'

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
  fullscreen?: boolean
  onCloseFullscreen?: () => void
  onOpenFullscreen?: () => void
}

type SnapshotState = 'loading' | 'ready' | 'error'
type ConnectionState = 'connecting' | 'live' | 'disconnected'

export function EntityLogViewer({
  entityId,
  fullscreen = false,
  onCloseFullscreen,
  onOpenFullscreen,
}: EntityLogViewerProps) {
  const viewer = (
    <EntityLogViewerSurface
      key={fullscreen ? 'fullscreen' : 'embedded'}
      entityId={entityId}
      fullscreen={fullscreen}
      onCloseFullscreen={onCloseFullscreen}
      onOpenFullscreen={onOpenFullscreen}
    />
  )

  if (fullscreen && typeof document !== 'undefined') {
    return createPortal(viewer, document.body)
  }

  return viewer
}

function EntityLogViewerSurface({
  entityId,
  fullscreen = false,
  onCloseFullscreen,
  onOpenFullscreen,
}: EntityLogViewerProps) {
  const autoFollowRef = useRef(true)
  const fitAddon = useMemo(() => new FitAddon(), [])
  const terminalOptions = useMemo<ITerminalOptions>(
    () => ({
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
    }),
    [],
  )
  const terminalAddons = useMemo(() => [fitAddon], [fitAddon])
  const { instance: terminal, ref: terminalRef } = useXTerm({
    addons: terminalAddons,
    options: terminalOptions,
  })

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
    if (!terminal) {
      return
    }

    terminal.reset()
    terminal.clear()
  })

  useEffect(() => {
    if (!terminal) {
      return
    }

    let fitFrameId: number | null = null

    const fitTerminal = () => {
      try {
        fitAddon.fit()
      } catch {
        // xterm can report incomplete dimensions before its renderer finishes mounting.
      }
    }

    const scheduleFit = () => {
      if (fitFrameId !== null) {
        window.cancelAnimationFrame(fitFrameId)
      }
      fitFrameId = window.requestAnimationFrame(() => {
        fitFrameId = null
        fitTerminal()
      })
    }

    scheduleFit()
    const host = terminalRef.current
    const resizeObserver = host
      ? new ResizeObserver(() => {
        scheduleFit()
        if (autoFollowRef.current) {
          terminal.scrollToBottom()
        }
      })
      : null

    if (host) {
      resizeObserver?.observe(host)
    }

    return () => {
      if (fitFrameId !== null) {
        window.cancelAnimationFrame(fitFrameId)
      }
      resizeObserver?.disconnect()
    }
  }, [fitAddon, terminal, terminalRef])

  useEffect(() => {
    if (!terminal) {
      return
    }

    const scrollDisposable = terminal.onScroll(() => {
      setAutoFollow(isNearBottom(terminal))
    })

    return () => {
      scrollDisposable.dispose()
    }
  }, [terminal])

  useEffect(() => {
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
  }, [entityId, terminal])

  return (
    <section className={clsx('entity-log-viewer', fullscreen && 'entity-log-viewer--fullscreen')}>
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
        </div>
        {fullscreen ? (
          <button
            className="icon-button"
            type="button"
            aria-label="Exit fullscreen log viewer"
            onClick={onCloseFullscreen}
          >
            <Minimize2 size={16} />
          </button>
        ) : onOpenFullscreen ? (
          <button
            className="icon-button"
            type="button"
            aria-label="Open log viewer fullscreen"
            onClick={onOpenFullscreen}
          >
            <Maximize2 size={16} />
          </button>
        ) : null}
      </header>

      <div className="entity-log-viewer__body">
        <div ref={terminalRef} className="entity-log-viewer__terminal" />

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
