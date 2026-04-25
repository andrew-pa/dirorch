const FALLBACK_BACKEND_HOST = '127.0.0.1'
const DEFAULT_BACKEND_PORT = '8000'
const STORAGE_KEY = 'dirorch.backendEndpoint'

const DEFAULT_BACKEND_ENDPOINT = normalizeBackendEndpoint(
  import.meta.env.VITE_DIRORCH_API_BASE || browserBackendEndpoint(),
)

let backendEndpoint = readBackendEndpoint()

export function getBackendEndpoint() {
  return backendEndpoint
}

export function getDefaultBackendEndpoint() {
  return DEFAULT_BACKEND_ENDPOINT
}

export function setBackendEndpoint(nextEndpoint: string) {
  const normalized = normalizeBackendEndpoint(nextEndpoint)
  backendEndpoint = normalized

  if (typeof window !== 'undefined') {
    window.sessionStorage.setItem(STORAGE_KEY, normalized)
  }

  return normalized
}

export function resetBackendEndpoint() {
  backendEndpoint = DEFAULT_BACKEND_ENDPOINT

  if (typeof window !== 'undefined') {
    window.sessionStorage.removeItem(STORAGE_KEY)
  }

  return backendEndpoint
}

export function normalizeBackendEndpoint(endpoint: string) {
  const trimmed = endpoint.trim()
  if (!trimmed) {
    throw new Error('Backend endpoint is required')
  }

  let url: URL
  try {
    url = new URL(trimmed)
  } catch {
    throw new Error('Backend endpoint must be a valid URL')
  }

  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('Backend endpoint must start with http:// or https://')
  }

  url.hash = ''
  url.search = ''
  if (url.pathname === '/') {
    url.pathname = ''
  } else {
    url.pathname = url.pathname.replace(/\/+$/, '')
  }

  return url.toString().replace(/\/$/, '')
}

function readBackendEndpoint() {
  if (typeof window === 'undefined') {
    return DEFAULT_BACKEND_ENDPOINT
  }

  const storedEndpoint = window.sessionStorage.getItem(STORAGE_KEY)
  if (!storedEndpoint) {
    return DEFAULT_BACKEND_ENDPOINT
  }

  try {
    return normalizeBackendEndpoint(storedEndpoint)
  } catch {
    window.sessionStorage.removeItem(STORAGE_KEY)
    return DEFAULT_BACKEND_ENDPOINT
  }
}

function browserBackendEndpoint() {
  if (typeof window === 'undefined' || !window.location.hostname) {
    return `http://${FALLBACK_BACKEND_HOST}:${DEFAULT_BACKEND_PORT}`
  }

  return `http://${window.location.hostname}:${DEFAULT_BACKEND_PORT}`
}
