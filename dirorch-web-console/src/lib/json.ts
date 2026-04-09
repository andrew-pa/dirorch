import type { Content } from 'vanilla-jsoneditor'

import type { JsonValue } from '../api/types'

export function tryParseJson(rawContent: string) {
  if (!rawContent.trim()) {
    return {
      ok: false as const,
      error: 'JSON content is empty',
    }
  }

  try {
    return {
      ok: true as const,
      value: JSON.parse(rawContent) as JsonValue,
    }
  } catch (error) {
    return {
      ok: false as const,
      error: error instanceof Error ? error.message : 'Invalid JSON',
    }
  }
}

export function stringifyJson(value: JsonValue) {
  return JSON.stringify(value, null, 2)
}

export function contentToJsonValue(content: Content) {
  if ('json' in content) {
    return content.json as JsonValue
  }

  const parsed = tryParseJson(content.text)
  return parsed.ok ? parsed.value : null
}

export interface PathReference {
  fieldName: string
  value: string
  location: string
}

export function extractPathReferences(value: JsonValue, parentPath = '$'): PathReference[] {
  if (Array.isArray(value)) {
    return value.flatMap((item, index) =>
      extractPathReferences(item, `${parentPath}[${index}]`),
    )
  }

  if (value === null || typeof value !== 'object') {
    return []
  }

  return Object.entries(value).flatMap(([key, child]) => {
    const location = parentPath === '$' ? `$.${key}` : `${parentPath}.${key}`
    const nested = extractPathReferences(child, location)
    if (key.endsWith('Path') && typeof child === 'string') {
      return [
        {
          fieldName: key,
          value: child,
          location,
        },
        ...nested,
      ]
    }
    return nested
  })
}
