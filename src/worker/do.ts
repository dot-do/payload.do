/**
 * PayloadDatabaseDO — Durable Object for Payload CMS data.
 *
 * Extends the base DB class from @dotdo/db which provides:
 * - entities table (id, type, data, created_at, updated_at, deleted_at)
 * - events table (CDC WAL)
 * - getCollection() with MongoDB-style filtering
 * - EventLogger with automatic CDC event forwarding
 * - Write mutex for serialized mutations
 * - Event compaction via alarm()
 *
 * Adds HTTP route handling for entity CRUD so the RPC entrypoint
 * can delegate via stub.fetch().
 *
 * Capnweb WebSocket: Accepts hibernatable WS connections. All SQL
 * methods are exposed via PayloadSqlTarget so concurrent calls from
 * the worker isolate batch into single WS frames.
 */

import { DB } from '@dotdo/db/do'
import { HibernatableWebSocketTransport, TransportRegistry, RpcSession } from '@dotdo/capnweb/server'
import type { RpcSessionOptions } from '@dotdo/capnweb/server'
import { PayloadSqlTarget } from './rpc-target.js'
import { sanitizeField, buildWhereSql, buildOrderSql } from '../queries/sql.js'
import { slugToType, entityToDocument, documentToEntityData, type NounContext } from '../utilities/transforms.js'
import { translateWhere } from '../queries/where.js'
import { translateSort } from '../queries/sort.js'
import { buildPagination } from '../utilities/pagination.js'
import { computeSchemaHash } from '../utilities/schema-hash.js'
import type { MigrationDef } from '../utilities/migrate.js'
import { buildCdcEvent } from '../utilities/cdc.js'

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

export class PayloadDatabaseDO extends DB {
  private _colo: string = 'unknown'
  private _transportRegistry = new TransportRegistry()
  private _sessions = new Map<WebSocket, RpcSession>()
  private _nounCache = new Map<string, { id: string; schemaVersion: number; schemaHash: string; migrations: MigrationDef[] }>()

  constructor(ctx: DurableObjectState, env: any) {
    super(ctx, env)
    // Create the Payload data table alongside the entities table from DB
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      title TEXT,
      c INTEGER NOT NULL,
      v INTEGER NOT NULL,
      data TEXT
    )`)
    // Indexes for fast collection lookups + email uniqueness
    ctx.storage.sql.exec('CREATE INDEX IF NOT EXISTS idx_type ON data(type)')
    ctx.storage.sql.exec('CREATE INDEX IF NOT EXISTS idx_type_active ON data(type) WHERE data IS NOT NULL')
    ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS idx_users_email ON data(json_extract(data, '$.email')) WHERE type = 'users' AND data IS NOT NULL")
    try {
      ctx.storage.sql.exec("CREATE UNIQUE INDEX IF NOT EXISTS uniq_users_email ON data(json_extract(data, '$.email')) WHERE type = 'users' AND data IS NOT NULL")
    } catch {
      // Unique index creation fails if duplicate emails already exist
      console.warn('@dotdo/payload: Could not create unique email index — duplicate emails may exist')
    }
  }

  /** Create the RpcTarget for capnweb dispatch */
  private createRpcTarget(): PayloadSqlTarget {
    // Cast needed: DB base class provides entity methods (find, create, etc.)
    // but TypeScript can't match DBEntity return types to Record<string, unknown>
    return new PayloadSqlTarget(this as unknown as import('./rpc-target.js').PayloadSqlMethods)
  }

  /** RPC session options — log capnweb errors for debugging */
  private getRpcSessionOptions(): RpcSessionOptions {
    return {
      onSendError: (error: Error) => {
        console.error('[PayloadDatabaseDO] capnweb send error:', error.message)
        return new Error(error.message)
      },
    }
  }

  exec(sql: string): void {
    this.sql.exec(sql)
  }

  query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): T[] {
    return this.sql.exec(sql, ...params).toArray() as T[]
  }

  queryFirst<T = Record<string, unknown>>(sql: string, ...params: unknown[]): T | null {
    const rows = this.sql.exec(sql, ...params).toArray() as T[]
    return rows[0] ?? null
  }

  run(sql: string, ...params: unknown[]): { changes: number; lastRowId: number } {
    const cursor = this.sql.exec(sql, ...params)
    const lastRowId = this.sql.exec('SELECT last_insert_rowid() as id').one().id as number
    return { changes: cursor.rowsWritten, lastRowId }
  }

  /**
   * Bulk insert rows using individual INSERT statements within a single RPC call.
   * Avoids the 100-variable SQL limit while still doing one network round-trip.
   */
  batchInsert(
    type: string,
    rows: Array<{ title: string | null; c: number; v: number; data: string }>,
  ): { changes: number; firstRowId: number; lastRowId: number } {
    if (rows.length === 0) return { changes: 0, firstRowId: 0, lastRowId: 0 }

    let totalChanges = 0
    let firstRowId = 0

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i]
      const cursor = this.sql.exec('INSERT INTO data (type, title, c, v, data) VALUES (?, ?, ?, ?, ?)', type, r.title, r.c, r.v, r.data)
      totalChanges += cursor.rowsWritten
      if (i === 0) {
        firstRowId = this.sql.exec('SELECT last_insert_rowid() as id').one().id as number
      }
    }

    const lastRowId = this.sql.exec('SELECT last_insert_rowid() as id').one().id as number
    return { changes: totalChanges, firstRowId, lastRowId }
  }

  /**
   * Forward event to the Pipeline binding for CDC.
   * Validates ULID format before sending.
   */
  sendEvent(event: Record<string, unknown>): void {
    const id = event.id as string | undefined
    if (!id || id.length !== 26 || !/^[0-9A-HJKMNP-TV-Z]{26}$/.test(id)) {
      console.error(`[cdc] Rejecting event with invalid ULID id: ${id}`)
      return
    }
    if ((this.env as any).EVENTS_PIPELINE) {
      ;(this.env as any).EVENTS_PIPELINE.send([event])
    }
  }

  /**
   * Atomically check uniqueness constraints and insert a row.
   * Runs within a single DO method call — serialized by Cloudflare DO.
   */
  atomicCreate(
    type: string,
    title: string | null,
    c: number,
    v: number,
    data: string,
    uniqueChecks: Array<{ field: string; value: unknown }>,
    emailCheck: { email: string } | null,
  ): { lastRowId: number; error?: string; code?: string } {
    // Enforce uniqueness
    for (const check of uniqueChecks) {
      const safeField = sanitizeField(check.field)
      const rows = this.sql
        .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.${safeField}') = ? LIMIT 1`, type, check.value)
        .toArray()
      if (rows.length > 0) {
        return { lastRowId: -1, error: `Value for field "${safeField}" must be unique. Duplicate: ${check.value}`, code: 'DUPLICATE_KEY' }
      }
    }
    if (emailCheck) {
      const rows = this.sql
        .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.email') = ? LIMIT 1`, type, emailCheck.email)
        .toArray()
      if (rows.length > 0) {
        return { lastRowId: -1, error: `A "${type}" document with email "${emailCheck.email}" already exists.`, code: 'DUPLICATE_KEY' }
      }
    }
    this.sql.exec('INSERT INTO data (type, title, c, v, data) VALUES (?, ?, ?, ?, ?)', type, title, c, v, data)
    const lastRowId = this.sql.exec('SELECT last_insert_rowid() as id').one().id as number
    return { lastRowId }
  }

  /**
   * Atomically SELECT + INSERT within a single DO method call.
   */
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
  ): { existing: Record<string, unknown> | null; lastRowId: number; changes: number; error?: string; code?: string } {
    const rows = this.sql.exec(findSql, ...findParams).toArray()
    const existing = rows[0] as Record<string, unknown> | undefined
    if (existing) {
      return { existing: JSON.parse(JSON.stringify(existing)), lastRowId: existing.id as number, changes: 0 }
    }
    if (uniqueChecks) {
      for (const check of uniqueChecks) {
        const safeField = sanitizeField(check.field)
        const dupes = this.sql
          .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.${safeField}') = ? LIMIT 1`, insertType, check.value)
          .toArray()
        if (dupes.length > 0) {
          return {
            existing: null,
            lastRowId: -1,
            changes: 0,
            error: `Value for field "${safeField}" must be unique. Duplicate: ${check.value}`,
            code: 'DUPLICATE_KEY',
          }
        }
      }
    }
    if (emailCheck) {
      const dupes = this.sql
        .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.email') = ? LIMIT 1`, insertType, emailCheck.email)
        .toArray()
      if (dupes.length > 0) {
        return { existing: null, lastRowId: -1, changes: 0, error: `A "${insertType}" document with email "${emailCheck.email}" already exists.`, code: 'DUPLICATE_KEY' }
      }
    }
    this.sql.exec('INSERT INTO data (type, title, c, v, data) VALUES (?, ?, ?, ?, ?)', insertType, insertTitle, insertC, insertV, insertData)
    const lastRowId = this.sql.exec('SELECT last_insert_rowid() as id').one().id as number
    return { existing: null, lastRowId, changes: 1 }
  }

  /** Return the colo where this DO is running */
  getColo(): string {
    return this._colo
  }

  // ===========================================================================
  // Noun resolution (local, zero network)
  // ===========================================================================

  /** Resolve noun metadata locally from entities table. Cached in-memory (no TTL — DO is source of truth). */
  private resolveNounLocal(slug: string): (NounContext & { id: string }) | null {
    const cached = this._nounCache.get(slug)
    if (cached) return cached

    const rows = this.sql
      .exec(`SELECT id, data FROM entities WHERE type = 'Noun' AND json_extract(data, '$.slug') = ? AND deleted_at IS NULL LIMIT 1`, slug)
      .toArray()
    if (rows.length === 0) return null

    const row = rows[0]
    let parsed: Record<string, unknown> = {}
    try {
      parsed = typeof row.data === 'string' ? JSON.parse(row.data) : ((row.data as unknown as Record<string, unknown>) ?? {})
    } catch {
      return null
    }

    let migrations: MigrationDef[] = []
    try {
      const raw = parsed.migrations
      const arr = Array.isArray(raw) ? raw : typeof raw === 'string' ? JSON.parse(raw) : []
      migrations = (arr as MigrationDef[]).filter((m: any) => m && typeof m.version === 'number')
    } catch {}

    const schemaVersion = migrations.length > 0 ? Math.max(...migrations.map((m) => m.version ?? 0)) : 1
    const schemaHash = computeSchemaHash(parsed.schema ?? {})
    const entry = { id: row.id as string, schemaVersion, schemaHash, migrations }
    this._nounCache.set(slug, entry)
    return entry
  }

  /** Invalidate noun cache when Noun entities are modified. */
  private invalidateNounLocal(slug?: string): void {
    if (slug) this._nounCache.delete(slug)
    else this._nounCache.clear()
  }

  // ===========================================================================
  // Compound read methods — single RPC call per Payload operation
  // ===========================================================================

  /**
   * Compound find: slug→type + where translation + entity find + migration-on-read + pagination.
   * Replaces find() + resolveNounContext() (2 RPC calls → 0 network).
   */
  async payloadFind(
    collection: string,
    where?: Record<string, unknown>,
    sort?: string,
    limit?: number,
    page?: number,
    pagination?: boolean,
  ): Promise<Record<string, unknown>> {
    // Cast needed: DB base class provides entity methods at runtime but TS can't see them on the subclass
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const sortObj = translateSort(sort)
    const rawLimit = limit ?? 10
    const effectiveLimit = pagination !== false ? rawLimit : 0
    const currentPage = page ?? 1
    const offset = effectiveLimit > 0 ? (currentPage - 1) * effectiveLimit : 0

    const result = await db.find(type, filter, {
      limit: effectiveLimit > 0 ? effectiveLimit : 10000,
      offset,
      sort: sortObj,
    })

    const nounCtx = this.resolveNounLocal(collection)
    const docs = result.items.map((entity) => entityToDocument(entity, nounCtx ?? undefined))

    return { docs, ...buildPagination(result.total, effectiveLimit, currentPage) }
  }

  /** Compound findOne: slug→type + where translation + entity find + migration-on-read. */
  async payloadFindOne(collection: string, where?: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const entity = await db.findOne(type, filter)
    if (!entity) return null

    const nounCtx = this.resolveNounLocal(collection)
    return entityToDocument(entity, nounCtx ?? undefined)
  }

  /** Compound count: slug→type + where translation + count. */
  async payloadCount(collection: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }> {
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const total = await db.count(type, filter)
    return { totalDocs: total }
  }

  /** Things find: raw SQL across ALL entity types (no type filter). */
  async payloadThingsFind(
    where?: Record<string, unknown>,
    sort?: string,
    limit?: number,
    page?: number,
    pagination?: boolean,
  ): Promise<Record<string, unknown>> {
    const { sql: whereSql, params } = buildWhereSql(null, where)
    const orderSql = sort ? buildOrderSql(sort) : 'ORDER BY updated_at DESC'
    const rawLimit = limit ?? 10
    const effectiveLimit = pagination !== false ? rawLimit : 0
    const currentPage = page ?? 1
    const offset = effectiveLimit > 0 ? (currentPage - 1) * effectiveLimit : 0

    const countRows = this.sql.exec(`SELECT COUNT(*) as cnt FROM entities WHERE ${whereSql}`, ...params).toArray()
    const totalDocs = (countRows[0]?.cnt as number) ?? 0

    let dataSql = `SELECT id, type, data, created_at, updated_at FROM entities WHERE ${whereSql} ${orderSql}`
    const dataParams = [...params]
    if (effectiveLimit > 0) {
      dataSql += ' LIMIT ? OFFSET ?'
      dataParams.push(effectiveLimit, offset)
    }

    const rows = this.sql.exec(dataSql, ...dataParams).toArray()
    const docs = rows.map((row: any) => {
      let parsed: Record<string, unknown> = {}
      try {
        parsed = typeof row.data === 'string' ? JSON.parse(row.data) : ((row.data as unknown as Record<string, unknown>) ?? {})
      } catch {}
      return { id: row.id, type: row.type, ...parsed, createdAt: row.created_at, updatedAt: row.updated_at }
    })

    return { docs, ...buildPagination(totalDocs, effectiveLimit, currentPage) }
  }

  /** Things count: count across ALL entity types. */
  async payloadThingsCount(where?: Record<string, unknown>): Promise<{ totalDocs: number }> {
    const { sql: whereSql, params } = buildWhereSql(null, where)
    const countRows = this.sql.exec(`SELECT COUNT(*) as cnt FROM entities WHERE ${whereSql}`, ...params).toArray()
    return { totalDocs: (countRows[0]?.cnt as number) ?? 0 }
  }

  // ===========================================================================
  // Compound write methods — entity CRUD + noun stamping + CDC in one call
  // ===========================================================================

  /**
   * Compound create: slug→type + noun stamping + entity create + CDC event.
   * Returns { doc, cdcEvent } — RPC worker forwards cdcEvent to ClickHouse/webhooks.
   */
  async payloadCreate(
    collection: string,
    data: Record<string, unknown>,
    context?: string,
  ): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }> {
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const entityData = documentToEntityData(data)

    const noun = this.resolveNounLocal(collection)
    if (noun) {
      if (!entityData.noun) entityData.noun = noun.id
      entityData._schemaVersion = noun.schemaVersion
      entityData._schemaHash = noun.schemaHash
    }

    const entity = await db.create(type, entityData)
    const doc = entityToDocument(entity)

    let cdcEvent: Record<string, unknown> | null = null
    try {
      cdcEvent = buildCdcEvent({
        id: entity.$id as string,
        ns: context ?? '',
        event: `${collection}.created`,
        entityType: type,
        entityData,
        meta: noun ? { schemaHash: noun.schemaHash, schemaVersion: noun.schemaVersion } : undefined,
      })
      this.sendEvent(cdcEvent)
    } catch (err) {
      console.error('[compound] CDC event failed:', err)
    }

    if (type === 'Noun') this.invalidateNounLocal(entityData.slug as string)
    return { doc, cdcEvent }
  }

  /**
   * Compound updateOne: find existing + noun stamping + entity update + CDC event.
   * Accepts either `where` (Payload Where) or `id` (direct lookup).
   */
  async payloadUpdateOne(
    collection: string,
    where: Record<string, unknown> | undefined,
    id: string | undefined,
    data: Record<string, unknown>,
    context?: string,
  ): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }> {
    const type = slugToType(collection)
    const updateData = documentToEntityData(data)

    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods

    let entityId: string
    if (where && Object.keys(where).length > 0) {
      const filter = translateWhere(where)
      const existing = await db.findOne(type, filter)
      if (!existing) throw new Error(`Document not found in ${collection}`)
      entityId = existing.$id as string
    } else if (id) {
      entityId = id
    } else {
      throw new Error(`payloadUpdateOne requires either 'id' or 'where' for ${collection}`)
    }

    const noun = this.resolveNounLocal(collection)
    if (noun) {
      updateData._schemaVersion = noun.schemaVersion
      updateData._schemaHash = noun.schemaHash
    }

    const updated = await db.update(type, entityId, updateData)
    if (!updated) throw new Error(`Failed to update document in ${collection}`)

    const doc = entityToDocument(updated)

    let cdcEvent: Record<string, unknown> | null = null
    try {
      cdcEvent = buildCdcEvent({
        id: entityId,
        ns: context ?? '',
        event: `${collection}.updated`,
        entityType: type,
        entityData: updateData,
        meta: noun ? { schemaHash: noun.schemaHash, schemaVersion: noun.schemaVersion } : undefined,
      })
      this.sendEvent(cdcEvent)
    } catch (err) {
      console.error('[compound] CDC event failed:', err)
    }

    if (type === 'Noun') this.invalidateNounLocal((updateData.slug as string) ?? undefined)
    return { doc, cdcEvent }
  }

  /**
   * Compound deleteOne: find existing + soft delete + CDC event.
   * Returns the deleted doc for Payload's return contract.
   */
  async payloadDeleteOne(
    collection: string,
    where: Record<string, unknown>,
    context?: string,
  ): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }> {
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const existing = await db.findOne(type, filter)
    if (!existing) return { doc: {} as Record<string, unknown>, cdcEvent: null }

    const entityId = existing.$id as string
    const doc = entityToDocument(existing)

    await db.delete(type, entityId)

    let cdcEvent: Record<string, unknown> | null = null
    try {
      cdcEvent = buildCdcEvent({
        id: entityId,
        ns: context ?? '',
        event: `${collection}.deleted`,
        entityType: type,
        entityData: { $id: entityId },
      })
      this.sendEvent(cdcEvent)
    } catch (err) {
      console.error('[compound] CDC event failed:', err)
    }

    if (type === 'Noun') this.invalidateNounLocal()
    return { doc, cdcEvent }
  }

  /**
   * Compound upsert: find existing → update or create + CDC event.
   */
  async payloadUpsert(
    collection: string,
    where: Record<string, unknown>,
    data: Record<string, unknown>,
    context?: string,
  ): Promise<{ doc: Record<string, unknown>; cdcEvent: Record<string, unknown> | null }> {
    const db = this as unknown as import('./rpc-target.js').PayloadSqlMethods
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const existing = await db.findOne(type, filter)

    if (existing) {
      const entityId = existing.$id as string
      const updateData = documentToEntityData(data)
      const noun = this.resolveNounLocal(collection)
      if (noun) {
        updateData._schemaVersion = noun.schemaVersion
        updateData._schemaHash = noun.schemaHash
      }

      const updated = await db.update(type, entityId, updateData)
      if (!updated) throw new Error(`Failed to update document in ${collection}`)

      const doc = entityToDocument(updated)

      let cdcEvent: Record<string, unknown> | null = null
      try {
        cdcEvent = buildCdcEvent({
          id: entityId,
          ns: context ?? '',
          event: `${collection}.updated`,
          entityType: type,
          entityData: updateData,
          meta: noun ? { schemaHash: noun.schemaHash, schemaVersion: noun.schemaVersion } : undefined,
        })
        this.sendEvent(cdcEvent)
      } catch (err) {
        console.error('[compound] CDC event failed:', err)
      }

      return { doc, cdcEvent }
    }

    return this.payloadCreate(collection, data, context)
  }

  // ===========================================================================
  // Hibernatable WebSocket handler for capnweb
  // ===========================================================================

  protected async onRequest(request: Request, path: string, method: string, url: URL): Promise<Response | null> {
    // Capture colo from incoming request
    if (this._colo === 'unknown') {
      this._colo = (request as any).cf?.colo ?? 'unknown'
    }

    // WebSocket upgrade — capnweb RPC session
    if (request.headers.get('Upgrade') === 'websocket') {
      const pair = new WebSocketPair()
      const client = pair[0]
      const server = pair[1]

      // Create hibernatable transport + register
      const transport = new HibernatableWebSocketTransport(server)
      this._transportRegistry.register(transport)

      // Serialize transport ID so webSocketMessage can look it up after hibernation
      server.serializeAttachment({ transportId: transport.id })

      // Accept via hibernation API (DO can sleep between messages)
      this.ctx.acceptWebSocket(server)

      // Create capnweb session — dispatches RPC calls to PayloadSqlTarget
      const session = new RpcSession(transport, this.createRpcTarget(), this.getRpcSessionOptions())
      this._sessions.set(server, session)

      return new Response(null, { status: 101, webSocket: client })
    }

    // POST /entity/:type — Create
    // GET  /entity/:type — List (unused, find() goes through RPC)
    // GET  /entity/:type/:id — Get
    // PUT  /entity/:type/:id — Update
    // DELETE /entity/:type/:id — Delete
    const entityMatch = path.match(/^\/entity\/([^/]+)(?:\/(.+))?$/)
    if (entityMatch) {
      const type = decodeURIComponent(entityMatch[1])
      const id = entityMatch[2] ? decodeURIComponent(entityMatch[2]) : undefined
      const collection = this.getCollection(type)

      if (method === 'POST' && !id) {
        const data = (await request.json()) as Record<string, unknown>
        return this.withWriteLock(async () => {
          const result = await collection.create(data)
          this.eventLogger.safeLogEvent('create', type, (result as any)?.$id ?? '', data)
          return json(result, 201)
        })
      }

      if (method === 'GET' && id) {
        const result = await collection.get(id)
        return result ? json(result) : json({ error: 'Not found' }, 404)
      }

      if (method === 'PUT' && id) {
        const data = (await request.json()) as Record<string, unknown>
        return this.withWriteLock(async () => {
          const result = await collection.update(id, data)
          if (!result) return json({ error: 'Not found' }, 404)
          this.eventLogger.safeLogEvent('update', type, id, data)
          return json(result)
        })
      }

      if (method === 'DELETE' && id) {
        return this.withWriteLock(async () => {
          const result = await collection.delete(id)
          this.eventLogger.safeLogEvent('delete', type, id)
          return json(result)
        })
      }
    }

    // POST /query — Raw SQL query
    if (path === '/query' && method === 'POST') {
      const { sql: sqlText, params } = (await request.json()) as { sql: string; params: unknown[] }
      const rows = this.sql.exec(sqlText, ...(params ?? [])).toArray()
      return json({ rows })
    }

    // POST /run — Raw SQL execution
    if (path === '/run' && method === 'POST') {
      const { sql: sqlText, params } = (await request.json()) as { sql: string; params: unknown[] }
      this.sql.exec(sqlText, ...(params ?? []))
      const changesRow = this.sql.exec('SELECT changes() as cnt').toArray()
      const changes = (changesRow[0]?.cnt as number) ?? 0
      return json({ changes })
    }

    return null
  }

  /**
   * Called by Cloudflare runtime when a hibernatable WS receives a message.
   * If the DO woke from hibernation, recreates transport + session.
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== 'string') return

    const attachment = ws.deserializeAttachment() as { transportId?: string } | null
    let transport: HibernatableWebSocketTransport | undefined

    if (attachment?.transportId) {
      transport = this._transportRegistry.get(attachment.transportId)
    }

    // Transport not found — DO woke from hibernation, recreate
    if (!transport) {
      transport = new HibernatableWebSocketTransport(ws)
      this._transportRegistry.register(transport)

      const session = new RpcSession(transport, this.createRpcTarget(), this.getRpcSessionOptions())
      this._sessions.set(ws, session)

      // Update attachment with new transport ID
      ws.serializeAttachment({ transportId: transport.id })
    }

    transport.enqueueMessage(message)
  }

  /**
   * Called by Cloudflare runtime when a hibernatable WS closes.
   */
  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
    const attachment = ws.deserializeAttachment() as { transportId?: string } | null
    if (attachment?.transportId) {
      const transport = this._transportRegistry.get(attachment.transportId)
      if (transport) {
        transport.handleClose(code, reason)
        this._transportRegistry.remove(attachment.transportId)
      }
    }
    this._sessions.delete(ws)
  }

  /**
   * Called by Cloudflare runtime when a hibernatable WS errors.
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const err = error instanceof Error ? error : new Error(String(error))
    const attachment = ws.deserializeAttachment() as { transportId?: string } | null
    if (attachment?.transportId) {
      const transport = this._transportRegistry.get(attachment.transportId)
      if (transport) {
        transport.handleError(err)
        this._transportRegistry.remove(attachment.transportId)
      }
    }
    this._sessions.delete(ws)
  }
}
