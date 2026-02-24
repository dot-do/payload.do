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
 */

import { DB } from '@dotdo/db/do'

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

export class PayloadDatabaseDO extends DB {
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
      const rows = this.sql
        .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.${check.field}') = ? LIMIT 1`, type, check.value)
        .toArray()
      if (rows.length > 0) {
        return { lastRowId: -1, error: `Value for field "${check.field}" must be unique. Duplicate: ${check.value}`, code: 'DUPLICATE_KEY' }
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
        const dupes = this.sql
          .exec(`SELECT id FROM data WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.${check.field}') = ? LIMIT 1`, insertType, check.value)
          .toArray()
        if (dupes.length > 0) {
          return {
            existing: null,
            lastRowId: -1,
            changes: 0,
            error: `Value for field "${check.field}" must be unique. Duplicate: ${check.value}`,
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

  protected async onRequest(request: Request, path: string, method: string, url: URL): Promise<Response | null> {
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
}
