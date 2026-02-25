import type { BaseDatabaseAdapter } from 'payload'
import type { VersionStore } from './versions.js'

/**
 * RPC interface for the PayloadDatabaseRPC WorkerEntrypoint.
 *
 * This is a collection-level API that delegates to DatabaseDO's getCollection().
 * CDC events are emitted automatically by the DO — callers don't need to send events explicitly.
 */
export interface PayloadDatabaseService {
  // Collection operations (delegate to DO's getCollection())
  find(
    ns: string,
    type: string,
    filter?: Record<string, unknown>,
    opts?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> },
  ): Promise<{ items: Record<string, unknown>[]; total: number; hasMore: boolean }>
  findOne(ns: string, type: string, filter?: Record<string, unknown>): Promise<Record<string, unknown> | null>
  get(ns: string, type: string, id: string): Promise<Record<string, unknown> | null>
  create(ns: string, type: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  update(ns: string, type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null>
  delete(ns: string, type: string, id: string): Promise<{ deletedCount: number }>
  count(ns: string, type: string, filter?: Record<string, unknown>): Promise<number>

  // SQL escape hatch (for complex queries that need json_extract on entities table)
  query(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown>[]>
  run(ns: string, sql: string, ...params: unknown[]): Promise<{ changes: number }>

  // SQL helpers (for legacy compatibility with packages/payload operations)
  exec(ns: string, sql: string): Promise<void>
  queryFirst(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown> | null>

  // Batch + atomic operations (prevent TOCTOU races)
  batchInsert(
    ns: string,
    type: string,
    rows: Array<{ title: string | null; c: number; v: number; data: string }>,
  ): Promise<{ changes: number; firstRowId: number; lastRowId: number }>
  atomicCreate(
    ns: string,
    type: string,
    title: string | null,
    c: number,
    v: number,
    data: string,
    uniqueChecks: Array<{ field: string; value: unknown }>,
    emailCheck: { email: string } | null,
  ): Promise<{ lastRowId: number; error?: string; code?: string }>
  atomicUpsert(
    ns: string,
    findSql: string,
    findParams: unknown[],
    insertType: string,
    insertTitle: string | null,
    insertC: number,
    insertV: number,
    insertData: string,
    uniqueChecks?: Array<{ field: string; value: unknown }>,
    emailCheck?: { email: string } | null,
  ): Promise<{ existing: Record<string, unknown> | null; lastRowId: number; changes: number; error?: string; code?: string }>

  // Event emission (for version tracking)
  sendEvent(ns: string, event: Record<string, unknown>): Promise<void>

  // ClickHouse operations (for analytics collections + version queries)
  chQuery(sql: string, params?: Record<string, string | number>): Promise<{ data: Record<string, unknown>[] }>
  chInsert(table: string, rows: Record<string, unknown>[]): Promise<void>

  // Compound methods — entire Payload operation in a single DO call
  // Reads: slug→type + where translation + entity query + migration-on-read + pagination
  payloadFind(ns: string, collection: string, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean): Promise<Record<string, unknown>>
  payloadFindOne(ns: string, collection: string, where?: Record<string, unknown>): Promise<Record<string, unknown> | null>
  payloadCount(ns: string, collection: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }>
  payloadThingsFind(ns: string, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean): Promise<Record<string, unknown>>
  payloadThingsCount(ns: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }>
  // Writes: slug→type + noun stamping + entity CRUD + CDC event (Pipeline sent by DO, ClickHouse/webhooks by RPC)
  payloadCreate(ns: string, collection: string, data: Record<string, unknown>, context?: string): Promise<Record<string, unknown>>
  payloadUpdateOne(ns: string, collection: string, where: Record<string, unknown> | undefined, id: string | undefined, data: Record<string, unknown>, context?: string): Promise<Record<string, unknown>>
  payloadDeleteOne(ns: string, collection: string, where: Record<string, unknown>, context?: string): Promise<Record<string, unknown>>
  payloadUpsert(ns: string, collection: string, where: Record<string, unknown>, data: Record<string, unknown>, context?: string): Promise<Record<string, unknown>>
}

export interface DoPayloadArgs {
  service: PayloadDatabaseService
  namespace?: string
  tenantNum?: number
  context?: string
}

export interface DoPayloadAdapter extends BaseDatabaseAdapter {
  name: 'do'
  namespace: string
  tenantNum: number
  context: string
  _service: PayloadDatabaseService
  versionStore: VersionStore
}

/**
 * Row shape from the DatabaseDO entities table.
 */
export interface EntityRow {
  id: string
  type: string
  data: string
  created_at: string
  updated_at: string
  deleted_at: string | null
}
