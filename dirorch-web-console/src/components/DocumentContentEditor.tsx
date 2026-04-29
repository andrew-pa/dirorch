import clsx from 'clsx'
import { useCallback, useMemo } from 'react'

import type { ContentFormat, JsonValue } from '../api/types'
import { tryParseJson } from '../lib/json'
import { CodeEditor } from './CodeEditor'
import { StructuredJsonEditor } from './StructuredJsonEditor'

interface DocumentContentEditorProps {
  format: ContentFormat
  editorMode: 'raw' | 'structured'
  rawContent: string
  onFormatChange: (format: ContentFormat) => void
  onEditorModeChange: (mode: 'raw' | 'structured') => void
  onRawContentChange: (value: string) => void
  height?: string
  readOnly?: boolean
}

export function DocumentContentEditor({
  format,
  editorMode,
  rawContent,
  onFormatChange,
  onEditorModeChange,
  onRawContentChange,
  height,
  readOnly = false,
}: DocumentContentEditorProps) {
  const parsedJson = useMemo(
    () => (format === 'json' ? tryParseJson(rawContent) : null),
    [format, rawContent],
  )
  const structuredDisabled = format !== 'json' || parsedJson?.ok === false
  const handleStructuredChange = useCallback(
    (nextValue: JsonValue) => {
      onRawContentChange(JSON.stringify(nextValue, null, 2))
    },
    [onRawContentChange],
  )

  return (
    <section className="document-editor">
      <div className="document-editor__toolbar">
        <div className="segmented-control" role="tablist" aria-label="Format">
          <button
            className={clsx('segmented-control__button', format === 'text' && 'is-active')}
            disabled={readOnly}
            type="button"
            onClick={() => onFormatChange('text')}
          >
            Text
          </button>
          <button
            className={clsx('segmented-control__button', format === 'json' && 'is-active')}
            disabled={readOnly}
            type="button"
            onClick={() => onFormatChange('json')}
          >
            JSON
          </button>
        </div>

        <div className="segmented-control" role="tablist" aria-label="Editor mode">
          <button
            className={clsx(
              'segmented-control__button',
              editorMode === 'raw' && 'is-active',
            )}
            disabled={readOnly}
            type="button"
            onClick={() => onEditorModeChange('raw')}
          >
            Raw
          </button>
          <button
            className={clsx(
              'segmented-control__button',
              editorMode === 'structured' && 'is-active',
            )}
            type="button"
            disabled={readOnly || structuredDisabled}
            onClick={() => onEditorModeChange('structured')}
          >
            Structured
          </button>
        </div>
      </div>

      {format === 'json' && parsedJson?.ok === false ? (
        <div className="inline-error">{parsedJson.error}</div>
      ) : null}

      {editorMode === 'structured' && parsedJson?.ok ? (
        <StructuredJsonEditor
          height={height}
          readOnly={readOnly}
          value={parsedJson.value as JsonValue}
          onChange={handleStructuredChange}
        />
      ) : (
        <CodeEditor
          height={height}
          language={format === 'json' ? 'json' : 'plaintext'}
          readOnly={readOnly}
          value={rawContent}
          onChange={onRawContentChange}
        />
      )}
    </section>
  )
}
