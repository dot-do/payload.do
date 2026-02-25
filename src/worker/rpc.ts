/**
 * PayloadDatabaseRPC — WorkerEntrypoint that proxies to PayloadDatabaseDO.
 *
 * Consumer apps bind to this entrypoint via service binding:
 *   { "binding": "PAYLOAD_DB", "service": "dotdo-payload", "entrypoint": "PayloadDatabaseRPC" }
 *
 * Entity CRUD (find, findOne, get, create, update, delete, count) uses
 * direct Workers RPC via DO stub — each call is a single atomic operation.
 *
 * SQLite methods (query, queryFirst, run, exec, batchInsert, atomicCreate,
 * atomicUpsert) route through a capnweb WebSocket to the DO. The WS session
 * is cached in module-scope memory so it persists across requests in the same
 * isolate. Capnweb auto-batches concurrent calls into single WS frames.
 *
 * Non-SQLite paths (ClickHouse, Pipeline CDC) bypass the DO entirely.
 */

import { WorkerEntrypoint } from 'cloudflare:workers'
import { newWebSocketRpcSession } from '@dotdo/capnweb'
import type { PayloadDatabaseService } from '../types.js'

/** Slack notification rule shape stored in Integration.configSchema */
interface SlackNotificationRule {
  entityType: string
  event: string
  condition?: Record<string, unknown>
  channel: string
  template?: string
  mention?: string
}

interface Env {
  PAYLOAD_DO: DurableObjectNamespace
  EVENTS_PIPELINE?: { send(messages: unknown[]): Promise<void> }
  CLICKHOUSE_URL?: string
  CLICKHOUSE_USERNAME?: string
  CLICKHOUSE_PASSWORD?: string
}

/**
 * Force ProxyStub → plain object (same pattern as @headlessly/payload).
 */
function materialize<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

// =============================================================================
// Module-scope capnweb WebSocket session cache
// =============================================================================

/** Cached WS session per namespace — persists across requests in same isolate */
const wsSessions = new Map<string, { remote: any; ws: WebSocket; closed: boolean }>()

/** In-flight connection promises — prevents duplicate connections for same ns */
const wsConnecting = new Map<string, Promise<any>>()

/**
 * Get or create a capnweb WS session to the DO for the given namespace.
 * The session is cached in module-scope memory so isolate reuse = WS reuse.
 */
async function getWsSession(doNs: DurableObjectNamespace, ns: string): Promise<any> {
  // Fast path: reuse existing session
  const cached = wsSessions.get(ns)
  if (cached && !cached.closed) return cached.remote

  // Deduplicate concurrent connection attempts
  const pending = wsConnecting.get(ns)
  if (pending) return pending

  const promise = (async () => {
    try {
      const doId = doNs.idFromName(ns)
      const stub = doNs.get(doId)

      // Open WS to the DO via stub.fetch() with Upgrade header
      const resp = await stub.fetch('http://do/ws', {
        headers: { Upgrade: 'websocket' },
      })

      const ws = resp.webSocket
      if (!ws) throw new Error('DO did not return WebSocket')
      ws.accept()

      // Create capnweb session — remote is a Proxy with .query(), .run(), etc.
      const remote = newWebSocketRpcSession(ws)

      const entry = { remote, ws, closed: false }
      ws.addEventListener('close', () => {
        entry.closed = true
        wsSessions.delete(ns)
      })
      ws.addEventListener('error', () => {
        entry.closed = true
        wsSessions.delete(ns)
      })

      wsSessions.set(ns, entry)
      return remote
    } finally {
      wsConnecting.delete(ns)
    }
  })()

  wsConnecting.set(ns, promise)
  return promise
}

// =============================================================================
// PayloadDatabaseRPC
// =============================================================================

export class PayloadDatabaseRPC extends WorkerEntrypoint<Env> implements PayloadDatabaseService {
  /** Cached DO colo (fetched once per entrypoint instance) */
  private _doColo: Map<string, string> = new Map()

  /** Get a DO stub for direct Workers RPC (entity CRUD) */
  private getStub(ns: string): any {
    const doId = this.env.PAYLOAD_DO.idFromName(ns)
    return this.env.PAYLOAD_DO.get(doId)
  }

  /** Get a capnweb WS remote for the given namespace (SQL methods) */
  private getWs(ns: string): Promise<any> {
    return getWsSession(this.env.PAYLOAD_DO, ns)
  }

  /**
   * Call a method on the capnweb WS remote with single retry on transport failure.
   * If the WS is stale (DO hibernated, network blip), clears cache and retries once.
   */
  private async callWs(ns: string, method: string, args: unknown[]): Promise<any> {
    try {
      const remote = await this.getWs(ns)
      return await (remote as any)[method](...args)
    } catch (err) {
      // Clear stale session and retry once
      wsSessions.delete(ns)
      const remote = await this.getWs(ns)
      return await (remote as any)[method](...args)
    }
  }

  /** Log perf trace (picked up by tail worker → ClickHouse) */
  private logPerf(method: string, ns: string, ms: number, extra?: Record<string, unknown>) {
    console.log(JSON.stringify({
      _tag: 'payload-rpc',
      method,
      ns,
      ms,
      doColo: this._doColo.get(ns) ?? 'unknown',
      ...extra,
    }))
  }

  // ===========================================================================
  // Entity CRUD — direct Workers RPC via DO stub
  // Each call is a single atomic operation; no batching benefit from WS.
  // ===========================================================================

  async find(
    ns: string,
    type: string,
    filter?: Record<string, unknown>,
    opts?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> },
  ): Promise<{ items: Record<string, unknown>[]; total: number; hasMore: boolean }> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.find(type, filter, opts)
    const materialized = materialize(result)
    this.logPerf('find', ns, Date.now() - start, { type, count: materialized.items?.length, transport: 'rpc' })
    return materialized
  }

  async findOne(ns: string, type: string, filter?: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.findOne(type, filter)
    const materialized = result ? materialize(result) : null
    this.logPerf('findOne', ns, Date.now() - start, { type, transport: 'rpc' })
    return materialized
  }

  async get(ns: string, type: string, id: string): Promise<Record<string, unknown> | null> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.get(type, id)
    const materialized = result ? materialize(result) : null
    this.logPerf('get', ns, Date.now() - start, { type, id, transport: 'rpc' })
    return materialized
  }

  async create(ns: string, type: string, data: Record<string, unknown>): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.create(type, data)
    const materialized = materialize(result)
    this.logPerf('create', ns, Date.now() - start, { type, transport: 'rpc' })
    return materialized
  }

  async update(ns: string, type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.update(type, id, data)
    const materialized = result ? materialize(result) : null
    this.logPerf('update', ns, Date.now() - start, { type, id, transport: 'rpc' })
    return materialized
  }

  async delete(ns: string, type: string, id: string): Promise<{ deletedCount: number }> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.delete(type, id)
    const materialized = materialize(result)
    this.logPerf('delete', ns, Date.now() - start, { type, id, transport: 'rpc' })
    return materialized
  }

  async count(ns: string, type: string, filter?: Record<string, unknown>): Promise<number> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.count(type, filter)
    const total = typeof result === 'number' ? result : (materialize(result) ?? 0)
    this.logPerf('count', ns, Date.now() - start, { type, total, transport: 'rpc' })
    return total as number
  }

  // ===========================================================================
  // Compound methods — entire Payload operation in a single DO call
  // Reads return Payload-formatted results; writes return doc + forward CDC
  // ===========================================================================

  async payloadFind(
    ns: string,
    collection: string,
    where?: Record<string, unknown>,
    sort?: string,
    limit?: number,
    page?: number,
    pagination?: boolean,
  ): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadFind(collection, where, sort, limit, page, pagination)
    const materialized = materialize(result)
    this.logPerf('payloadFind', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized
  }

  async payloadFindOne(ns: string, collection: string, where?: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadFindOne(collection, where)
    const materialized = result ? materialize(result) : null
    this.logPerf('payloadFindOne', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized
  }

  async payloadCount(ns: string, collection: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadCount(collection, where)
    const materialized = materialize(result)
    this.logPerf('payloadCount', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized
  }

  async payloadThingsFind(
    ns: string,
    where?: Record<string, unknown>,
    sort?: string,
    limit?: number,
    page?: number,
    pagination?: boolean,
  ): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadThingsFind(where, sort, limit, page, pagination)
    const materialized = materialize(result)
    this.logPerf('payloadThingsFind', ns, Date.now() - start, { transport: 'rpc' })
    return materialized
  }

  async payloadThingsCount(ns: string, where?: Record<string, unknown>): Promise<{ totalDocs: number }> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadThingsCount(where)
    const materialized = materialize(result)
    this.logPerf('payloadThingsCount', ns, Date.now() - start, { transport: 'rpc' })
    return materialized
  }

  async payloadCreate(ns: string, collection: string, data: Record<string, unknown>, context?: string): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadCreate(collection, data, context)
    const materialized = materialize(result)
    if (materialized.cdcEvent) {
      this.ctx.waitUntil(this.forwardCdcEvent(ns, materialized.cdcEvent))
    }
    this.logPerf('payloadCreate', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized.doc
  }

  async payloadUpdateOne(
    ns: string,
    collection: string,
    where: Record<string, unknown> | undefined,
    id: string | undefined,
    data: Record<string, unknown>,
    context?: string,
  ): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadUpdateOne(collection, where, id, data, context)
    const materialized = materialize(result)
    if (materialized.cdcEvent) {
      this.ctx.waitUntil(this.forwardCdcEvent(ns, materialized.cdcEvent))
    }
    this.logPerf('payloadUpdateOne', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized.doc
  }

  async payloadDeleteOne(ns: string, collection: string, where: Record<string, unknown>, context?: string): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadDeleteOne(collection, where, context)
    const materialized = materialize(result)
    if (materialized.cdcEvent) {
      this.ctx.waitUntil(this.forwardCdcEvent(ns, materialized.cdcEvent))
    }
    this.logPerf('payloadDeleteOne', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized.doc
  }

  async payloadUpsert(
    ns: string,
    collection: string,
    where: Record<string, unknown>,
    data: Record<string, unknown>,
    context?: string,
  ): Promise<Record<string, unknown>> {
    const start = Date.now()
    const stub = this.getStub(ns)
    const result = await stub.payloadUpsert(collection, where, data, context)
    const materialized = materialize(result)
    if (materialized.cdcEvent) {
      this.ctx.waitUntil(this.forwardCdcEvent(ns, materialized.cdcEvent))
    }
    this.logPerf('payloadUpsert', ns, Date.now() - start, { collection, transport: 'rpc' })
    return materialized.doc
  }

  // ===========================================================================
  // SQLite methods — routed through capnweb WS (batched, pipelined)
  // ===========================================================================

  async query(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown>[]> {
    const start = Date.now()
    const result = await this.callWs(ns, 'query', [sql, ...params])
    const materialized = materialize(result) as Record<string, unknown>[]
    this.logPerf('query', ns, Date.now() - start, { rows: materialized.length, transport: 'ws' })
    return materialized
  }

  async run(ns: string, sql: string, ...params: unknown[]): Promise<{ changes: number }> {
    const start = Date.now()
    const result = await this.callWs(ns, 'run', [sql, ...params])
    const materialized = materialize(result)
    this.logPerf('run', ns, Date.now() - start, { transport: 'ws' })
    return materialized
  }

  async exec(ns: string, sql: string): Promise<void> {
    const start = Date.now()
    await this.callWs(ns, 'exec', [sql])
    this.logPerf('exec', ns, Date.now() - start, { transport: 'ws' })
  }

  async queryFirst(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown> | null> {
    const start = Date.now()
    const row = await this.callWs(ns, 'queryFirst', [sql, ...params])
    const materialized = row === null ? null : materialize(row)
    this.logPerf('queryFirst', ns, Date.now() - start, { transport: 'ws' })
    return materialized
  }

  async batchInsert(
    ns: string,
    type: string,
    rows: Array<{ title: string | null; c: number; v: number; data: string }>,
  ): Promise<{ changes: number; firstRowId: number; lastRowId: number }> {
    const start = Date.now()
    const result = await this.callWs(ns, 'batchInsert', [type, rows])
    const materialized = materialize(result)
    this.logPerf('batchInsert', ns, Date.now() - start, { type, count: rows.length, transport: 'ws' })
    return materialized
  }

  async atomicCreate(
    ns: string,
    type: string,
    title: string | null,
    c: number,
    v: number,
    data: string,
    uniqueChecks: Array<{ field: string; value: unknown }>,
    emailCheck: { email: string } | null,
  ): Promise<{ lastRowId: number; error?: string; code?: string }> {
    const start = Date.now()
    const result = await this.callWs(ns, 'atomicCreate', [type, title, c, v, data, uniqueChecks, emailCheck])
    const materialized = materialize(result)
    this.logPerf('atomicCreate', ns, Date.now() - start, { type, transport: 'ws' })
    return materialized
  }

  async atomicUpsert(
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
  ): Promise<{ existing: Record<string, unknown> | null; lastRowId: number; changes: number; error?: string; code?: string }> {
    const start = Date.now()
    const result = await this.callWs(ns, 'atomicUpsert', [findSql, findParams, insertType, insertTitle, insertC, insertV, insertData, uniqueChecks, emailCheck])
    const materialized = materialize(result)
    this.logPerf('atomicUpsert', ns, Date.now() - start, { type: insertType, transport: 'ws' })
    return materialized
  }

  // ===========================================================================
  // Non-DO methods — sendEvent, ClickHouse, webhooks, Slack
  // (These bypass the DO entirely — no batching needed)
  // ===========================================================================

  /**
   * Forward a CDC event from a compound DO method to ClickHouse + webhooks + Slack.
   * Pipeline send already happened inside the DO — this handles the external paths.
   */
  private async forwardCdcEvent(ns: string, event: Record<string, unknown>): Promise<void> {
    // Direct ClickHouse insert
    if (this.env.CLICKHOUSE_URL) {
      try {
        await this.chInsert('events', [event])
      } catch (err) {
        console.error('[cdc] ClickHouse forward failed:', err)
      }
    }

    // Webhook + Slack dispatch (fire-and-forget)
    if (event.type === 'cdc' && typeof event.event === 'string' && event.event.includes('.')) {
      this.ctx.waitUntil(
        this.dispatchWebhooks(ns, event).catch((err) => {
          console.error('[PayloadDatabaseRPC] Webhook dispatch error:', err)
        }),
      )
      this.ctx.waitUntil(
        this.dispatchSlackNotifications(ns, event).catch((err) => {
          console.error('[PayloadDatabaseRPC] Slack notification error:', err)
        }),
      )
    }
  }

  async sendEvent(ns: string, event: Record<string, unknown>): Promise<void> {
    const start = Date.now()
    // Validate event ID is a 26-char Crockford Base32 ULID
    const id = event.id as string | undefined
    if (!id || id.length !== 26 || !/^[0-9A-HJKMNP-TV-Z]{26}$/.test(id)) {
      console.error(`[cdc] Rejecting event with invalid ULID id: ${id}`)
      return
    }

    const promises: Promise<void>[] = []

    // Pipeline (durable log — R2 → S3Queue → ClickHouse)
    if (this.env.EVENTS_PIPELINE) {
      promises.push(
        this.env.EVENTS_PIPELINE.send([event]).catch((err: unknown) => {
          console.error('[cdc] Pipeline send failed:', err)
        }),
      )
    }

    // Direct ClickHouse insert (reliable, fast)
    if (this.env.CLICKHOUSE_URL) {
      promises.push(
        this.chInsert('events', [event]).catch((err: unknown) => {
          console.error('[cdc] ClickHouse direct insert failed:', err)
        }),
      )
    }

    await Promise.allSettled(promises)

    // Dispatch to registered webhook subscriptions via waitUntil (non-blocking)
    if (event.type === 'cdc' && typeof event.event === 'string' && event.event.includes('.')) {
      this.ctx.waitUntil(
        this.dispatchWebhooks(ns, event).catch((err) => {
          console.error('[PayloadDatabaseRPC] Webhook dispatch error:', err)
        }),
      )
      this.ctx.waitUntil(
        this.dispatchSlackNotifications(ns, event).catch((err) => {
          console.error('[PayloadDatabaseRPC] Slack notification error:', err)
        }),
      )
    }

    this.logPerf('sendEvent', ns, Date.now() - start, { eventType: event.type, event: event.event })
  }

  /**
   * Return the colo where the DO for this namespace is running.
   * Useful for diagnosing latency issues.
   */
  async getDoLocation(ns: string): Promise<{ doColo: string }> {
    const cached = this._doColo.get(ns)
    if (cached) return { doColo: cached }
    try {
      const stub = this.getStub(ns)
      const colo = await stub.getColo() as string
      if (colo && colo !== 'unknown') this._doColo.set(ns, colo)
      return { doColo: colo ?? 'unknown' }
    } catch {
      return { doColo: 'unknown' }
    }
  }

  /**
   * Query registered webhook subscriptions and POST event payloads to matching targets.
   * Fire-and-forget — never fails the primary sendEvent flow.
   */
  private async dispatchWebhooks(namespace: string, event: Record<string, unknown>): Promise<void> {
    const rows = await this.query(
      namespace,
      "SELECT id, data FROM data WHERE type = 'webhooks' AND data IS NOT NULL AND json_extract(data, '$.status') = 'Active'",
    )
    if (!rows || rows.length === 0) return

    const eventName = event.event as string
    const encoder = new TextEncoder()

    for (const row of rows) {
      try {
        const webhook = JSON.parse(row.data as string) as {
          id: string
          targetUrl: string
          event: string
          secret: string | null
          status: string
        }

        const matches =
          webhook.event === '*' || webhook.event === eventName || (webhook.event.endsWith('.*') && eventName.startsWith(webhook.event.slice(0, -2) + '.'))
        if (!matches) continue

        const payload = JSON.stringify({
          id: event.id,
          event: eventName,
          timestamp: event.ts,
          tenant: namespace,
          data: event.data,
        })

        let signature: string | undefined
        if (webhook.secret) {
          const key = await crypto.subtle.importKey('raw', encoder.encode(webhook.secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
          const sig = await crypto.subtle.sign('HMAC', key, encoder.encode(payload))
          signature = 'sha256=' + [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('')
        }

        let delivered = false
        let lastStatusCode = 0
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            const headers: Record<string, string> = {
              'Content-Type': 'application/json',
              'User-Agent': 'Headlessly-Webhooks/1.0',
              'X-Headlessly-Delivery': crypto.randomUUID(),
              'X-Headlessly-Event-Timestamp': (event.ts as string) || new Date().toISOString(),
            }
            if (signature) headers['X-Headlessly-Signature'] = signature

            const resp = await fetch(webhook.targetUrl, {
              method: 'POST',
              headers,
              body: payload,
              signal: AbortSignal.timeout(10_000),
            })

            lastStatusCode = resp.status
            if (resp.status >= 200 && resp.status < 300) {
              delivered = true
              break
            }
            if (resp.status >= 400 && resp.status < 500) break
          } catch {
            // Network error — retry
          }
          if (attempt < 3) {
            await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, attempt - 1)))
          }
        }

        // Update webhook metrics atomically
        try {
          const now = Date.now()
          const ts = new Date(now).toISOString()
          if (delivered) {
            await this.run(
              namespace,
              `UPDATE data SET data = json_set(data,
                '$.deliveryCount', COALESCE(json_extract(data, '$.deliveryCount'), 0) + 1,
                '$.failureCount', 0,
                '$.lastDeliveredAt', ?,
                '$.lastStatusCode', ?,
                '$.updatedAt', ?
              ), v = ? WHERE id = ?`,
              ts,
              lastStatusCode,
              ts,
              now,
              row.id,
            )
          } else {
            await this.run(
              namespace,
              `UPDATE data SET data = json_set(data,
                '$.deliveryCount', COALESCE(json_extract(data, '$.deliveryCount'), 0) + 1,
                '$.failureCount', COALESCE(json_extract(data, '$.failureCount'), 0) + 1,
                '$.lastDeliveredAt', ?,
                '$.lastStatusCode', ?,
                '$.updatedAt', ?,
                '$.status', CASE WHEN COALESCE(json_extract(data, '$.failureCount'), 0) + 1 >= 10 THEN 'Failed' ELSE json_extract(data, '$.status') END
              ), v = ? WHERE id = ?`,
              ts,
              lastStatusCode,
              ts,
              now,
              row.id,
            )
          }
        } catch {
          // Best-effort metrics update
        }
      } catch (err) {
        console.error(`[PayloadDatabaseRPC] Failed to dispatch webhook for row ${row.id}:`, err)
      }
    }
  }

  /**
   * Dispatch Slack notifications for CDC events based on per-tenant integration config.
   * Fire-and-forget — never fails the primary sendEvent flow.
   */
  private async dispatchSlackNotifications(namespace: string, event: Record<string, unknown>): Promise<void> {
    const integrationRows = await this.query(
      namespace,
      "SELECT id, data FROM data WHERE type = 'integrations' AND data IS NOT NULL AND json_extract(data, '$.provider') = 'slack' AND json_extract(data, '$.status') = 'Connected'",
    )
    if (!integrationRows || integrationRows.length === 0) return

    const integrationData = JSON.parse(integrationRows[0].data as string)
    const config = integrationData.configSchema as { botToken?: string; defaultChannel?: string; rules?: SlackNotificationRule[] } | undefined
    if (!config?.botToken) return

    const eventName = event.event as string
    const dotIdx = eventName.indexOf('.')
    if (dotIdx === -1) return

    const entityType = eventName.slice(0, dotIdx)
    const verb = eventName.slice(dotIdx + 1)
    const data = (event.data as Record<string, unknown>) || {}

    const rules: SlackNotificationRule[] = config.rules?.length
      ? config.rules
      : [
          { entityType: 'leads', event: 'created', channel: '#leads' },
          { entityType: 'deals', event: 'updated', condition: { stage: 'Won' }, channel: '#wins', mention: '@here' },
          { entityType: 'deals', event: 'updated', condition: { stage: 'Lost' }, channel: '#deals' },
          { entityType: 'tickets', event: 'created', channel: '#support' },
          { entityType: 'subscriptions', event: 'created', channel: '#revenue' },
          { entityType: 'payments', event: 'created', channel: '#revenue' },
          { entityType: 'invoices', event: 'updated', condition: { status: 'Overdue' }, channel: '#billing' },
        ]

    const matchingRules = rules.filter((rule) => {
      if (rule.entityType !== entityType) return false
      if (rule.event !== '*' && rule.event !== verb) return false
      if (rule.condition) {
        for (const [key, expected] of Object.entries(rule.condition)) {
          if (data[key] !== expected) return false
        }
      }
      return true
    })
    if (matchingRules.length === 0) return

    const entityDisplay: Record<string, { emoji: string; noun: string; fields: string[]; verbLabels: Record<string, string> }> = {
      leads: { emoji: ':dart:', noun: 'Lead', fields: ['name', 'email', 'company', 'source', 'status'], verbLabels: { created: 'New Lead', updated: 'Lead Updated', deleted: 'Lead Removed' } },
      deals: { emoji: ':handshake:', noun: 'Deal', fields: ['name', 'value', 'stage', 'owner', 'probability'], verbLabels: { created: 'New Deal', updated: 'Deal Updated', deleted: 'Deal Removed' } },
      tickets: { emoji: ':ticket:', noun: 'Ticket', fields: ['title', 'priority', 'status', 'assignee', 'category'], verbLabels: { created: 'New Ticket', updated: 'Ticket Updated', deleted: 'Ticket Closed' } },
      contacts: { emoji: ':bust_in_silhouette:', noun: 'Contact', fields: ['name', 'email', 'company', 'phone', 'title'], verbLabels: { created: 'New Contact', updated: 'Contact Updated', deleted: 'Contact Removed' } },
      subscriptions: { emoji: ':repeat:', noun: 'Subscription', fields: ['plan', 'status', 'interval', 'amount'], verbLabels: { created: 'New Subscription', updated: 'Subscription Updated', deleted: 'Subscription Cancelled' } },
      payments: { emoji: ':moneybag:', noun: 'Payment', fields: ['amount', 'currency', 'status', 'method'], verbLabels: { created: 'Payment Received', updated: 'Payment Updated', deleted: 'Payment Refunded' } },
      invoices: { emoji: ':page_facing_up:', noun: 'Invoice', fields: ['number', 'amount', 'status', 'dueDate', 'customer'], verbLabels: { created: 'New Invoice', updated: 'Invoice Updated', deleted: 'Invoice Voided' } },
    }
    const defaultDisplay = { emoji: ':bell:', noun: 'Entity', fields: ['name', 'title', 'status'], verbLabels: { created: 'Created', updated: 'Updated', deleted: 'Deleted' } }

    for (const rule of matchingRules) {
      try {
        const display = entityDisplay[entityType] || defaultDisplay
        const label = display.verbLabels[verb] || `${display.noun} ${verb}`
        const headerText = `${display.emoji} ${label}`
        const entityId = event.id as string
        const ts = (event.ts as string) || new Date().toISOString()

        const blocks: Record<string, unknown>[] = []
        blocks.push({ type: 'header', text: { type: 'plain_text', text: headerText, emoji: true } })

        if (rule.mention) {
          const mentionText = rule.mention.startsWith('@') ? `<!${rule.mention.slice(1)}>` : `<@${rule.mention}>`
          blocks.push({ type: 'section', text: { type: 'mrkdwn', text: mentionText } })
        }

        const fieldTexts: Array<{ type: string; text: string }> = []
        for (const fieldName of display.fields) {
          const value = data[fieldName]
          if (value !== undefined && value !== null) {
            const fieldLabel = fieldName.charAt(0).toUpperCase() + fieldName.slice(1)
            const formatted = typeof value === 'number' ? `$${value.toLocaleString()}` : String(value)
            fieldTexts.push({ type: 'mrkdwn', text: `*${fieldLabel}:*\n${formatted}` })
          }
        }
        if (fieldTexts.length > 0) {
          blocks.push({ type: 'section', fields: fieldTexts.slice(0, 10) })
        }

        blocks.push({ type: 'divider' })
        blocks.push({
          type: 'actions',
          block_id: `notification_${entityId}`,
          elements: [
            { type: 'button', text: { type: 'plain_text', text: 'View in Headless.ly', emoji: true }, url: `https://platform.headless.ly/admin/collections/${entityType}/${entityId}`, action_id: `view_${entityId}` },
            { type: 'button', text: { type: 'plain_text', text: 'Assign Agent', emoji: true }, action_id: `assign_agent_${entityId}`, value: JSON.stringify({ entityType, entityId, tenant: namespace }) },
          ],
        })
        blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `*ID:* \`${entityId}\` | *Tenant:* ${namespace} | *Time:* ${ts}` }] })

        const name = (data.name as string) || (data.title as string) || entityId
        const fallbackText = `${headerText}: ${name}`

        const resp = await fetch('https://slack.com/api/chat.postMessage', {
          method: 'POST',
          headers: { Authorization: `Bearer ${config.botToken}`, 'Content-Type': 'application/json; charset=utf-8' },
          body: JSON.stringify({ channel: rule.channel, text: fallbackText, blocks }),
        })

        if (!resp.ok) {
          console.error(`[PayloadDatabaseRPC] Slack notification failed for ${rule.channel}: HTTP ${resp.status}`)
        } else {
          const result = (await resp.json()) as { ok: boolean; error?: string }
          if (!result.ok) {
            console.error(`[PayloadDatabaseRPC] Slack API error for ${rule.channel}: ${result.error}`)
          }
        }
      } catch (err) {
        console.error(`[PayloadDatabaseRPC] Slack notification error for ${rule.channel}:`, err)
      }
    }
  }

  async chQuery(sql: string, params?: Record<string, string | number>): Promise<{ data: Record<string, unknown>[] }> {
    const chUrl = this.env.CLICKHOUSE_URL
    if (!chUrl) return { data: [] }

    const trimmed = chUrl.trim()
    const url = new URL(trimmed.startsWith('http') ? trimmed : `https://${trimmed}`)
    url.searchParams.set('default_format', 'JSON')
    url.searchParams.set('database', 'platform')
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(`param_${k}`, String(v))
      }
    }

    const resp = await fetch(url.toString(), {
      method: 'POST',
      body: sql,
      headers: {
        'Content-Type': 'text/plain',
        'X-ClickHouse-User': this.env.CLICKHOUSE_USERNAME || 'default',
        'X-ClickHouse-Key': this.env.CLICKHOUSE_PASSWORD || '',
      },
    })

    if (!resp.ok) {
      const text = await resp.text()
      throw new Error(`ClickHouse error (${resp.status}): ${text.slice(0, 500)}`)
    }

    return resp.json() as Promise<{ data: Record<string, unknown>[] }>
  }

  async chInsert(table: string, rows: Record<string, unknown>[]): Promise<void> {
    if (rows.length === 0) return
    const chUrl = this.env.CLICKHOUSE_URL
    if (!chUrl) return

    const trimmed = chUrl.trim()
    const url = new URL(trimmed.startsWith('http') ? trimmed : `https://${trimmed}`)
    url.searchParams.set('database', 'platform')
    url.searchParams.set('query', `INSERT INTO ${table} FORMAT JSONEachRow`)
    url.searchParams.set('date_time_input_format', 'best_effort')

    const body = rows.map((r) => JSON.stringify(r)).join('\n')

    const resp = await fetch(url.toString(), {
      method: 'POST',
      body,
      headers: {
        'Content-Type': 'text/plain',
        'X-ClickHouse-User': this.env.CLICKHOUSE_USERNAME || 'default',
        'X-ClickHouse-Key': this.env.CLICKHOUSE_PASSWORD || '',
      },
    })

    if (!resp.ok) {
      const text = await resp.text()
      throw new Error(`ClickHouse insert error (${resp.status}): ${text.slice(0, 500)}`)
    }
  }
}
