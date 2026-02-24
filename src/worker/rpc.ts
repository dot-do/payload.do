/**
 * PayloadDatabaseRPC — WorkerEntrypoint that proxies to PayloadDatabaseDO.
 *
 * Consumer apps bind to this entrypoint via service binding:
 *   { "binding": "PAYLOAD_DB", "service": "dotdo-payload", "entrypoint": "PayloadDatabaseRPC" }
 *
 * Each method resolves a DO stub from the namespace parameter and delegates
 * to the DO's collection interface via fetch() or RPC methods.
 */

import { WorkerEntrypoint } from 'cloudflare:workers'
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

export class PayloadDatabaseRPC extends WorkerEntrypoint<Env> implements PayloadDatabaseService {
  private getStub(ns: string): DurableObjectStub {
    const id = this.env.PAYLOAD_DO.idFromName(ns)
    return this.env.PAYLOAD_DO.get(id)
  }

  async find(
    ns: string,
    type: string,
    filter?: Record<string, unknown>,
    opts?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> },
  ): Promise<{ items: Record<string, unknown>[]; total: number; hasMore: boolean }> {
    const stub = this.getStub(ns)
    const result = await (stub as any).find(type, filter, opts)
    return materialize(result)
  }

  async findOne(ns: string, type: string, filter?: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const stub = this.getStub(ns)
    // Use the DO's find() with limit 1 and translate
    const result = await (stub as any).find(type, filter, { limit: 1 })
    const materialized = materialize(result)
    return materialized.items[0] ?? null
  }

  async get(ns: string, type: string, id: string): Promise<Record<string, unknown> | null> {
    const stub = this.getStub(ns)
    const result = await (stub as any).getEntity(type, id)
    return result ? materialize(result) : null
  }

  async create(ns: string, type: string, data: Record<string, unknown>): Promise<Record<string, unknown>> {
    const stub = this.getStub(ns)
    // Use the fetch API to hit the DO's collection create
    const url = `https://do/entity/${encodeURIComponent(type)}`
    const response = await stub.fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Namespace': ns,
      },
      body: JSON.stringify(data),
    })
    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Create failed: ${error}`)
    }
    return response.json()
  }

  async update(ns: string, type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const stub = this.getStub(ns)
    const url = `https://do/entity/${encodeURIComponent(type)}/${encodeURIComponent(id)}`
    const response = await stub.fetch(url, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'X-Namespace': ns,
      },
      body: JSON.stringify(data),
    })
    if (!response.ok) {
      if (response.status === 404) return null
      const error = await response.text()
      throw new Error(`Update failed: ${error}`)
    }
    return response.json()
  }

  async delete(ns: string, type: string, id: string): Promise<{ deletedCount: number }> {
    const stub = this.getStub(ns)
    const url = `https://do/entity/${encodeURIComponent(type)}/${encodeURIComponent(id)}`
    const response = await stub.fetch(url, {
      method: 'DELETE',
      headers: { 'X-Namespace': ns },
    })
    if (!response.ok) {
      return { deletedCount: 0 }
    }
    return response.json()
  }

  async count(ns: string, type: string, filter?: Record<string, unknown>): Promise<number> {
    const stub = this.getStub(ns)
    // Use the DO's find to count (filter in-memory)
    const result = await (stub as any).find(type, filter, { limit: 0 })
    return materialize(result).total ?? 0
  }

  async query(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown>[]> {
    const stub = this.getStub(ns)
    const url = `https://do/query`
    const response = await stub.fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Namespace': ns,
      },
      body: JSON.stringify({ sql, params }),
    })
    if (!response.ok) return []
    const result = (await response.json()) as { rows?: Record<string, unknown>[] }
    return result.rows ?? []
  }

  async run(ns: string, sql: string, ...params: unknown[]): Promise<{ changes: number }> {
    const stub = this.getStub(ns)
    const url = `https://do/run`
    const response = await stub.fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Namespace': ns,
      },
      body: JSON.stringify({ sql, params }),
    })
    if (!response.ok) return { changes: 0 }
    return response.json()
  }

  async exec(ns: string, sql: string): Promise<void> {
    const stub = this.getStub(ns)
    await (stub as any).exec(sql)
  }

  async queryFirst(ns: string, sql: string, ...params: unknown[]): Promise<Record<string, unknown> | null> {
    const stub = this.getStub(ns)
    const row = await (stub as any).queryFirst(sql, ...params)
    if (row === null) return null
    return materialize(row)
  }

  async batchInsert(
    ns: string,
    type: string,
    rows: Array<{ title: string | null; c: number; v: number; data: string }>,
  ): Promise<{ changes: number; firstRowId: number; lastRowId: number }> {
    const stub = this.getStub(ns)
    const result = await (stub as any).batchInsert(type, rows)
    return { changes: result.changes as number, firstRowId: result.firstRowId as number, lastRowId: result.lastRowId as number }
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
    const stub = this.getStub(ns)
    const result = await (stub as any).atomicCreate(type, title, c, v, data, uniqueChecks, emailCheck)
    if (result.error) return { lastRowId: -1, error: result.error as string, code: result.code as string }
    return { lastRowId: result.lastRowId as number }
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
    const stub = this.getStub(ns)
    const result = await (stub as any).atomicUpsert(findSql, findParams, insertType, insertTitle, insertC, insertV, insertData, uniqueChecks, emailCheck)
    return materialize(result)
  }

  async sendEvent(ns: string, event: Record<string, unknown>): Promise<void> {
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
