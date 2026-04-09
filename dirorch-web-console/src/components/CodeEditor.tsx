import { Suspense, lazy } from 'react'

import { EmptyState } from './ui/EmptyState'

const MonacoEditor = lazy(() => import('@monaco-editor/react'))

interface CodeEditorProps {
  language: 'json' | 'plaintext'
  value: string
  onChange: (value: string) => void
  height?: string
  readOnly?: boolean
}

export function CodeEditor({
  language,
  value,
  onChange,
  height = '28rem',
  readOnly = false,
}: CodeEditorProps) {
  return (
    <div className="editor-surface editor-surface--code">
      <Suspense fallback={<EmptyState className="panel-placeholder">Loading editor</EmptyState>}>
        <MonacoEditor
          height={height}
          language={language}
          theme="vs-dark"
          value={value}
          onChange={(nextValue) => onChange(nextValue ?? '')}
          options={{
            automaticLayout: true,
            fontFamily: 'JetBrains Mono, ui-monospace, monospace',
            fontSize: 13,
            lineHeight: 20,
            minimap: { enabled: false },
            padding: { top: 14, bottom: 14 },
            quickSuggestions: language === 'json',
            readOnly,
            roundedSelection: false,
            scrollBeyondLastLine: false,
            tabSize: 2,
            wordWrap: 'on',
          }}
        />
      </Suspense>
    </div>
  )
}
