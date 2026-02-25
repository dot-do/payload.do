/**
 * Capnweb RpcTarget for PayloadDatabaseDO methods.
 *
 * Wraps ALL DO methods (entity CRUD + raw SQL) as a capnweb RpcTarget so
 * they can be called via WebSocket with automatic batching/pipelining.
 * Only methods listed here are exposed to capnweb clients.
 */

import { RpcTarget } from '@dotdo/capnweb/server'

/** Subset of PayloadDatabaseDO methods exposed via capnweb */
export interface PayloadSqlMethods {
  // Entity CRUD (from DB base class — operates on `entities` table)
  find(
    type: string,
    filter?: Record<string, unknown>,
    options?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> },
  ): Promise<{ items: Record<string, unknown>[]; total: number; hasMore: boolean }>
  findOne(type: string, filter?: Record<string, unknown>): Promise<Record<string, unknown> | null>
  get(type: string, id: string): Promise<Record<string, unknown> | null>
  create(type: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null>
  delete(type: string, id: string): Promise<{ deletedCount: number }>
  count(type: string, filter?: Record<string, unknown>): Promise<number>

  // Raw SQL (operates on `data` table or any table)
  query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): T[]
  queryFirst<T = Record<string, unknown>>(sql: string, ...params: unknown[]): T | null
  run(sql: string, ...params: unknown[]): { changes: number; lastRowId: number }
  exec(sql: string): void
  batchInsert(
    type: string,
    rows: Array<{ title: string | null; c: number; v: number; data: string }>,
  ): { changes: number; firstRowId: number; lastRowId: number }
  atomicCreate(
    type: string,
    title: string | null,
    c: number,
    v: number,
    data: string,
    uniqueChecks: Array<{ field: string; value: unknown }>,
    emailCheck: { email: string } | null,
  ): { lastRowId: number; error?: string; code?: string }
  atomicUpsert(
    findSql: string,
    findParams: unknown[],
    insertType: string,
    insertTitle: string | null,
    insertC: number,
    insertV: number,
    insertData: string,
    uniqueChecks?: Array<{ field: string; value: unknown }>,
    emailCheck?: { email: string } | null,
  ): { existing: Record<string, unknown> | null; lastRowId: number; changes: number; error?: string; code?: string }
  getColo(): string
  sendEvent(event: Record<string, unknown>): void

  // Compound methods — entire Payload operation in one DO call
  payloadFind(collection: string, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean): Promise<Record<string, unknown>>
  payloadFindOne(collection: string, where?: Record<string, unknown>): Promise<Record<string, unknown> | null>
  payloadCount(collection: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }>
  payloadThingsFind(where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean): Promise<Record<string, unknown>>
  payloadThingsCount(where?: Record<string, unknown>): Promise<{ totalDocs: number }>
  payloadCreate(collection: string, data: Record<string, unknown>, context?: string): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }>
  payloadUpdateOne(collection: string, where: Record<string, unknown> | undefined, id: string | undefined, data: Record<string, unknown>, context?: string): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }>
  payloadDeleteOne(collection: string, where: Record<string, unknown>, context?: string): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }>
  payloadUpsert(collection: string, where: Record<string, unknown>, data: Record<string, unknown>, context?: string): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }>
}

export class PayloadSqlTarget extends RpcTarget {
  private do_: PayloadSqlMethods

  constructor(do_: PayloadSqlMethods) {
    super()
    this.do_ = do_
  }

  // =========================================================================
  // Entity CRUD (from DB base class)
  // =========================================================================

  find(type: string, filter?: Record<string, unknown>, options?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> }) {
    return this.do_.find(type, filter, options)
  }

  findOne(type: string, filter?: Record<string, unknown>) {
    return this.do_.findOne(type, filter)
  }

  get(type: string, id: string) {
    return this.do_.get(type, id)
  }

  /** Alias for get() — PayloadDatabaseRPC.get() calls getEntity() */
  getEntity(type: string, id: string) {
    return this.do_.get(type, id)
  }

  create(type: string, data: Record<string, unknown>) {
    return this.do_.create(type, data)
  }

  update(type: string, id: string, data: Record<string, unknown>) {
    return this.do_.update(type, id, data)
  }

  delete(type: string, id: string) {
    return this.do_.delete(type, id)
  }

  count(type: string, filter?: Record<string, unknown>) {
    return this.do_.count(type, filter)
  }

  sendEvent(event: Record<string, unknown>) {
    return this.do_.sendEvent(event)
  }

  // =========================================================================
  // Raw SQL
  // =========================================================================

  query(sql: string, ...params: unknown[]) {
    return this.do_.query(sql, ...params)
  }

  queryFirst(sql: string, ...params: unknown[]) {
    return this.do_.queryFirst(sql, ...params)
  }

  run(sql: string, ...params: unknown[]) {
    return this.do_.run(sql, ...params)
  }

  exec(sql: string) {
    this.do_.exec(sql)
  }

  batchInsert(type: string, rows: Array<{ title: string | null; c: number; v: number; data: string }>) {
    return this.do_.batchInsert(type, rows)
  }

  atomicCreate(
    type: string,
    title: string | null,
    c: number,
    v: number,
    data: string,
    uniqueChecks: Array<{ field: string; value: unknown }>,
    emailCheck: { email: string } | null,
  ) {
    return this.do_.atomicCreate(type, title, c, v, data, uniqueChecks, emailCheck)
  }

  atomicUpsert(
    findSql: string,
    findParams: unknown[],
    insertType: string,
    insertTitle: string | null,
    insertC: number,
    insertV: number,
    insertData: string,
    uniqueChecks?: Array<{ field: string; value: unknown }>,
    emailCheck?: { email: string } | null,
  ) {
    return this.do_.atomicUpsert(findSql, findParams, insertType, insertTitle, insertC, insertV, insertData, uniqueChecks, emailCheck)
  }

  getColo() {
    return this.do_.getColo()
  }

  // =========================================================================
  // Compound methods (entire Payload operation in one DO call)
  // =========================================================================

  payloadFind(collection: string, where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean) {
    return this.do_.payloadFind(collection, where, sort, limit, page, pagination)
  }

  payloadFindOne(collection: string, where?: Record<string, unknown>) {
    return this.do_.payloadFindOne(collection, where)
  }

  payloadCount(collection: string, where?: Record<string, unknown>) {
    return this.do_.payloadCount(collection, where)
  }

  payloadThingsFind(where?: Record<string, unknown>, sort?: string, limit?: number, page?: number, pagination?: boolean) {
    return this.do_.payloadThingsFind(where, sort, limit, page, pagination)
  }

  payloadThingsCount(where?: Record<string, unknown>) {
    return this.do_.payloadThingsCount(where)
  }

  payloadCreate(collection: string, data: Record<string, unknown>, context?: string) {
    return this.do_.payloadCreate(collection, data, context)
  }

  payloadUpdateOne(collection: string, where: Record<string, unknown> | undefined, id: string | undefined, data: Record<string, unknown>, context?: string) {
    return this.do_.payloadUpdateOne(collection, where, id, data, context)
  }

  payloadDeleteOne(collection: string, where: Record<string, unknown>, context?: string) {
    return this.do_.payloadDeleteOne(collection, where, context)
  }

  payloadUpsert(collection: string, where: Record<string, unknown>, data: Record<string, unknown>, context?: string) {
    return this.do_.payloadUpsert(collection, where, data, context)
  }
}
