import { useEffect, useRef } from 'react'
import type { Content, JSONEditorPropsOptional } from 'vanilla-jsoneditor'

import type { JsonValue } from '../api/types'

interface StructuredJsonEditorProps {
  value: JsonValue
  onChange: (value: JsonValue) => void
  height?: string
  readOnly?: boolean
}

export function StructuredJsonEditor({
  value,
  onChange,
  height = '28rem',
  readOnly = false,
}: StructuredJsonEditorProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const editorRef = useRef<{
    destroy: () => Promise<void>
    updateProps: (props: JSONEditorPropsOptional) => void
  } | null>(null)
  const latestValueRef = useRef(value)
  const latestOnChangeRef = useRef(onChange)

  latestValueRef.current = value
  latestOnChangeRef.current = onChange

  useEffect(() => {
    if (!containerRef.current) {
      return
    }

    let cancelled = false

    void import('vanilla-jsoneditor').then(({ createJSONEditor }) => {
      if (!containerRef.current || cancelled) {
        return
      }

      editorRef.current = createJSONEditor({
        target: containerRef.current,
        props: {},
      })

      editorRef.current.updateProps(
        buildProps(latestValueRef.current, latestOnChangeRef.current, readOnly),
      )
    })

    return () => {
      cancelled = true
      void editorRef.current?.destroy()
      editorRef.current = null
    }
  }, [readOnly])

  useEffect(() => {
    editorRef.current?.updateProps(buildProps(value, onChange, readOnly))
  }, [readOnly, value, onChange])

  return (
    <div className="editor-surface editor-surface--json" style={{ minHeight: height }}>
      <div className="json-editor-host" ref={containerRef} />
    </div>
  )
}

function buildProps(
  value: JsonValue,
  onChange: (value: JsonValue) => void,
  readOnly: boolean,
): JSONEditorPropsOptional {
  return {
    content: { json: value },
    indentation: 2,
    mainMenuBar: false,
    navigationBar: true,
    readOnly,
    statusBar: false,
    onChange: (updatedContent: Content) => {
      if ('json' in updatedContent) {
        onChange(updatedContent.json as JsonValue)
      }
    },
  }
}
