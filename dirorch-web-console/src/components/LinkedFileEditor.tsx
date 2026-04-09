import clsx from 'clsx'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { FilePenLine, FilePlus2, LoaderCircle, Save } from 'lucide-react'
import { useEffect, useState } from 'react'

import {
  ApiError,
  createFile,
  getFile,
  queryKeys,
  updateFile,
} from '../api/dirorch'
import type { ContentFormat, FileDetail } from '../api/types'
import { tryParseJson } from '../lib/json'
import { DocumentContentEditor } from './DocumentContentEditor'
import { EmptyState } from './ui/EmptyState'
import { SectionHeader } from './ui/SectionHeader'
import { Surface } from './ui/Surface'

interface LinkedFileEditorProps {
  path: string
  readOnly?: boolean
}

interface FileDraft {
  format: ContentFormat
  rawContent: string
  editorMode: 'raw' | 'structured'
}

export function LinkedFileEditor({ path, readOnly = false }: LinkedFileEditorProps) {
  const queryClient = useQueryClient()
  const fileQuery = useQuery({
    queryKey: queryKeys.file(path),
    queryFn: () => getFile(path),
    enabled: Boolean(path),
    retry: false,
    staleTime: Infinity,
  })

  const [draft, setDraft] = useState<FileDraft>({
    format: 'text',
    rawContent: '',
    editorMode: 'raw',
  })
  const [fileExists, setFileExists] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [loadedFile, setLoadedFile] = useState<FileDetail | null>(null)

  useEffect(() => {
    setSaveError(null)
    setLoadedFile(null)
    setDraft({
      format: 'text',
      rawContent: '',
      editorMode: 'raw',
    })
    setFileExists(false)
  }, [path])

  useEffect(() => {
    if (!fileQuery.data) {
      return
    }

    setLoadedFile(fileQuery.data)
    setFileExists(true)
    setDraft({
      format: fileQuery.data.format,
      rawContent: fileQuery.data.content,
      editorMode: fileQuery.data.format === 'json' ? 'structured' : 'raw',
    })
  }, [fileQuery.data])

  useEffect(() => {
    if (!(fileQuery.error instanceof ApiError) || fileQuery.error.status !== 404) {
      return
    }

    setLoadedFile(null)
    setFileExists(false)
    setDraft({
      format: 'text',
      rawContent: '',
      editorMode: 'raw',
    })
  }, [fileQuery.error])

  async function handleSave() {
    setSaveError(null)

    if (draft.format === 'json') {
      const parsed = tryParseJson(draft.rawContent)
      if (!parsed.ok) {
        setSaveError(parsed.error)
        return
      }
    }

    setIsSaving(true)

    try {
      if (fileExists) {
        await updateFile(path, {
          format: draft.format,
          content: draft.rawContent,
        })
      } else {
        await createFile(path, {
          format: draft.format,
          content: draft.rawContent,
        })
      }

      await queryClient.invalidateQueries({ queryKey: queryKeys.file(path) })
      setFileExists(true)
    } catch (error) {
      setSaveError(formatError(error))
    } finally {
      setIsSaving(false)
    }
  }

  const hasChanges =
    loadedFile === null
      ? draft.rawContent.length > 0 || draft.format !== 'text'
      : loadedFile.format !== draft.format || loadedFile.content !== draft.rawContent

  return (
    <Surface as="aside" className="linked-file-panel" padding="none">
      <SectionHeader
        className="linked-file-panel__header"
        eyebrow="Referenced file"
        title={<div className="linked-file-panel__path">{path}</div>}
        actions={
          <span
            className={clsx(
              'status-pill',
              fileExists ? 'status-pill--success' : 'status-pill--neutral',
            )}
          >
            {fileExists ? 'Existing' : 'Missing'}
          </span>
        }
      />

      {fileQuery.isLoading ? (
        <EmptyState className="panel-placeholder" icon={<LoaderCircle className="spin" size={16} />}>
          Loading file
        </EmptyState>
      ) : fileQuery.error instanceof ApiError && fileQuery.error.status !== 404 ? (
        <div className="inline-error">{formatError(fileQuery.error)}</div>
      ) : (
        <>
          <DocumentContentEditor
            format={draft.format}
            editorMode={draft.editorMode}
            rawContent={draft.rawContent}
            height="24rem"
            readOnly={readOnly}
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

          {saveError ? <div className="inline-error">{saveError}</div> : null}

          <div className="linked-file-panel__actions">
            <button
              className="button button--primary"
              type="button"
              disabled={readOnly || isSaving || !hasChanges}
              onClick={() => void handleSave()}
            >
              {fileExists ? <FilePenLine size={16} /> : <FilePlus2 size={16} />}
              {isSaving ? 'Saving' : fileExists ? 'Save file' : 'Create file'}
            </button>
            <button
              className="button button--ghost"
              type="button"
              disabled={readOnly || isSaving || !hasChanges}
              onClick={() => {
                if (!loadedFile) {
                  setDraft({
                    format: 'text',
                    rawContent: '',
                    editorMode: 'raw',
                  })
                  return
                }

                setDraft({
                  format: loadedFile.format,
                  rawContent: loadedFile.content,
                  editorMode: loadedFile.format === 'json' ? 'structured' : 'raw',
                })
              }}
            >
              <Save size={16} />
              Reset
            </button>
          </div>
        </>
      )}
    </Surface>
  )
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
