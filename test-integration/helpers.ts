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

// ===========================================================================
// Compound method helpers (call DO methods directly via stub)
// ===========================================================================

/** payloadFind: compound find with pagination */
export async function payloadFindViaRPC(doProxy: DOProxy, collection: string, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean) {
  const result = await (doProxy.stub as any).payloadFind(collection, where, sort, limit, page, pagination)
  return JSON.parse(JSON.stringify(result)) as { docs: Record<string, unknown>[]; totalDocs: number; totalPages: number; page: number; limit: number; hasNextPage: boolean; hasPrevPage: boolean }
}

/** payloadFindOne: compound findOne */
export async function payloadFindOneViaRPC(doProxy: DOProxy, collection: string, where?: Record<string, unknown>) {
  const result = await (doProxy.stub as any).payloadFindOne(collection, where)
  return result ? JSON.parse(JSON.stringify(result)) as Record<string, unknown> : null
}

/** payloadCount: compound count */
export async function payloadCountViaRPC(doProxy: DOProxy, collection: string, where?: Record<string, unknown>) {
  const result = await (doProxy.stub as any).payloadCount(collection, where)
  return JSON.parse(JSON.stringify(result)) as { totalDocs: number }
}

/** payloadCreate: compound create */
export async function payloadCreateViaRPC(doProxy: DOProxy, collection: string, data: Record<string, unknown>, context?: string) {
  const result = await (doProxy.stub as any).payloadCreate(collection, data, context)
  return JSON.parse(JSON.stringify(result)) as { doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }
}

/** payloadUpdateOne: compound updateOne */
export async function payloadUpdateOneViaRPC(doProxy: DOProxy, collection: string, where: Record<string, unknown> | undefined, id: string | undefined, data: Record<string, unknown>, context?: string) {
  const result = await (doProxy.stub as any).payloadUpdateOne(collection, where, id, data, context)
  return JSON.parse(JSON.stringify(result)) as { doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }
}

/** payloadDeleteOne: compound deleteOne */
export async function payloadDeleteOneViaRPC(doProxy: DOProxy, collection: string, where: Record<string, unknown>, context?: string) {
  const result = await (doProxy.stub as any).payloadDeleteOne(collection, where, context)
  return JSON.parse(JSON.stringify(result)) as { doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }
}

/** payloadUpsert: compound upsert */
export async function payloadUpsertViaRPC(doProxy: DOProxy, collection: string, where: Record<string, unknown>, data: Record<string, unknown>, context?: string) {
  const result = await (doProxy.stub as any).payloadUpsert(collection, where, data, context)
  return JSON.parse(JSON.stringify(result)) as { doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }
}

/** payloadThingsFind: compound Things find */
export async function payloadThingsFindViaRPC(doProxy: DOProxy, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean) {
  const result = await (doProxy.stub as any).payloadThingsFind(where, sort, limit, page, pagination)
  return JSON.parse(JSON.stringify(result)) as { docs: Record<string, unknown>[]; totalDocs: number; totalPages: number; page: number; limit: number; hasNextPage: boolean; hasPrevPage: boolean }
}

/** payloadThingsCount: compound Things count */
export async function payloadThingsCountViaRPC(doProxy: DOProxy, where?: Record<string, unknown>) {
  const result = await (doProxy.stub as any).payloadThingsCount(where)
  return JSON.parse(JSON.stringify(result)) as { totalDocs: number }
}

/** Sleep helper */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
