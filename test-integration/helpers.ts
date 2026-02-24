/**
 * Integration test helpers for PayloadDatabaseDO.
 *
 * Uses cloudflare:test to get real miniflare bindings — real SQLite,
 * real Durable Object instantiation. No hand-coded mocks.
 */

import { env } from 'cloudflare:test'

let testCounter = 0

/**
 * Get a fresh DO stub scoped to a unique namespace.
 * Each call returns a different DO instance so tests are isolated.
 */
export function getTestDO(prefix?: string) {
  testCounter++
  const namespace = prefix ?? `test-${Date.now()}-${testCounter}`
  const doId = env.PAYLOAD_DO.idFromName(namespace)
  const stub = env.PAYLOAD_DO.get(doId)

  return {
    namespace,
    stub,
    async fetch(path: string, init?: RequestInit): Promise<Response> {
      const headers = new Headers(init?.headers)
      headers.set('X-Namespace', namespace)
      return stub.fetch(`https://do${path}`, { ...init, headers })
    },
  }
}

type DOProxy = ReturnType<typeof getTestDO>

/** Create an entity via the DO's HTTP route */
export async function createEntity(doProxy: DOProxy, type: string, data: Record<string, unknown>) {
  const res = await doProxy.fetch(`/entity/${encodeURIComponent(type)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

/** Get an entity by type + id */
export async function getEntity(doProxy: DOProxy, type: string, id: string) {
  const res = await doProxy.fetch(`/entity/${encodeURIComponent(type)}/${encodeURIComponent(id)}`)
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

/** Update an entity by type + id */
export async function updateEntity(doProxy: DOProxy, type: string, id: string, data: Record<string, unknown>) {
  const res = await doProxy.fetch(`/entity/${encodeURIComponent(type)}/${encodeURIComponent(id)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

/** Delete an entity by type + id */
export async function deleteEntity(doProxy: DOProxy, type: string, id: string) {
  const res = await doProxy.fetch(`/entity/${encodeURIComponent(type)}/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  })
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

/** Run a raw SQL query via the DO's /query endpoint */
export async function querySQL(doProxy: DOProxy, sql: string, params: unknown[] = []) {
  const res = await doProxy.fetch('/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
  return { status: res.status, body: (await res.json()) as { rows?: Record<string, unknown>[] } }
}

/** Run a raw SQL exec via the DO's /run endpoint */
export async function runSQL(doProxy: DOProxy, sql: string, params: unknown[] = []) {
  const res = await doProxy.fetch('/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
  return { status: res.status, body: (await res.json()) as { changes?: number } }
}

/** Use the DO's RPC find() method directly */
export async function findViaRPC(doProxy: DOProxy, type: string, filter?: Record<string, unknown>, opts?: Record<string, unknown>) {
  const result = await (doProxy.stub as any).find(type, filter, opts)
  return JSON.parse(JSON.stringify(result)) as { items: Record<string, unknown>[]; total: number; hasMore: boolean }
}

/** Use the DO's RPC countEntities() method directly */
export async function countViaRPC(doProxy: DOProxy, type: string) {
  return (doProxy.stub as any).countEntities(type) as Promise<number>
}

/** Sleep helper */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
