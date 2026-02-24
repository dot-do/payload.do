/**
 * E2E test helpers for dotdo-payload.
 *
 * Hits the real deployed worker at dotdo-payload.dotdo.workers.dev.
 * No mocks — all requests go to production Cloudflare infrastructure.
 */

export const ENDPOINTS = {
  PAYLOAD_WORKER: process.env.PAYLOAD_WORKER_URL || 'https://dotdo-payload.dotdo.workers.dev',
  PLATFORM_API: process.env.PLATFORM_API_URL || 'https://dashboard.platform.do',
  CLICKHOUSE_URL: process.env.CLICKHOUSE_URL || '',
  CLICKHOUSE_USER: process.env.CLICKHOUSE_USER || '',
  CLICKHOUSE_PASSWORD: process.env.CLICKHOUSE_PASSWORD || '',
  DIAGNOSTIC_TOKEN: process.env.DIAGNOSTIC_TOKEN || '',
  ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || '',
} as const

/** Build URL for /test/* diagnostic endpoints with auth token */
export function diagnosticUrl(base: string, path: string): string {
  const url = new URL(path, base)
  if (ENDPOINTS.DIAGNOSTIC_TOKEN) {
    url.searchParams.set('token', ENDPOINTS.DIAGNOSTIC_TOKEN)
  }
  return url.toString()
}

export function generateTestId(): string {
  return `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Wait for a condition to become truthy, polling at intervals.
 */
export async function waitFor<T>(
  condition: () => Promise<T | false | null | undefined>,
  opts: { timeoutMs?: number; intervalMs?: number; description?: string } = {},
): Promise<T> {
  const { timeoutMs = 30000, intervalMs = 2000, description = 'condition' } = opts
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    const result = await condition()
    if (result) return result
    await sleep(intervalMs)
  }

  throw new Error(`Timeout waiting for ${description} after ${timeoutMs}ms`)
}

/**
 * Query ClickHouse Cloud directly.
 * Returns empty array if credentials are not configured.
 */
export async function queryClickHouse<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  if (!ENDPOINTS.CLICKHOUSE_URL || !ENDPOINTS.CLICKHOUSE_PASSWORD) {
    return []
  }

  const response = await fetch(`${ENDPOINTS.CLICKHOUSE_URL}/?default_format=JSONEachRow`, {
    method: 'POST',
    headers: {
      'X-ClickHouse-User': ENDPOINTS.CLICKHOUSE_USER || 'default',
      'X-ClickHouse-Key': ENDPOINTS.CLICKHOUSE_PASSWORD,
    },
    body: sql,
  })

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`ClickHouse query failed: ${error}`)
  }

  const text = await response.text()
  if (!text.trim()) return []
  return text
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as T)
}
