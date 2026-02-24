import type { Where } from 'payload'
import type { PayloadDatabaseService } from '../types.js'

export const CH_COLLECTIONS = new Set([
  'events',
  'traces',
  'errors',
  'cdc-events',
  'webhook-events',
  'github-events',
  'stripe-events',
  'api-requests',
])

/** Per-collection WHERE filters applied to the shared `events` table. */
const CH_COLLECTION_FILTERS: Record<string, string> = {
  traces: "source = 'tail' AND type = 'trace'",
  errors: "source = 'tail' AND event LIKE '%.exception%'",
  'cdc-events': "type = 'cdc'",
  'webhook-events': "type = 'webhook'",
  'github-events': "source = 'github'",
  'stripe-events': "event LIKE 'webhook.stripe.%'",
  'api-requests': "source = 'tail' AND type = 'request'",
}

/** Per-collection field extractors — pull normalized fields from raw `data` JSON. */
const FIELD_EXTRACTORS: Record<string, Record<string, (data: Record<string, unknown>, row: Record<string, unknown>) => unknown>> = {
  traces: {
    scriptName: (d) => d.scriptName,
    outcome: (d) => d.outcome,
  },
  errors: {
    scriptName: (d) => d.scriptName,
    exceptionName: (d) => {
      const exceptions = d.exceptions as Array<{ name?: string }> | undefined
      if (exceptions?.[0]?.name) return exceptions[0].name
      // Parse from event name: headlessly-api.fetch.exception → exception
      return undefined
    },
    exceptionMessage: (d) => {
      const exceptions = d.exceptions as Array<{ message?: string }> | undefined
      return exceptions?.[0]?.message
    },
  },
  'cdc-events': {
    collection: (_, r) => {
      const ev = r.event as string | undefined
      return ev?.split('.')[0]
    },
    action: (_, r) => {
      const ev = r.event as string | undefined
      return ev?.split('.')[1]
    },
    documentId: (d) => (d.id as string) || (d.$id as string),
  },
  'webhook-events': {
    provider: (_, r) => {
      // event format: webhook.github.push → provider = github
      const ev = r.event as string | undefined
      const parts = ev?.split('.')
      return parts && parts.length >= 2 ? parts[1] : parts?.[0]
    },
    webhookEvent: (_, r) => r.event,
  },
  'github-events': {
    action: (d, r) => {
      // Try data.action first, then parse from event name (webhook.github.push → push)
      if (d.action) return d.action
      const ev = r.event as string | undefined
      const parts = ev?.split('.')
      return parts && parts.length >= 3 ? parts[2] : undefined
    },
    repository: (d) => {
      const repo = d.repository as { full_name?: string } | string | undefined
      if (typeof repo === 'string') return repo
      return repo?.full_name
    },
    sender: (d) => {
      const sender = d.sender as { login?: string } | string | undefined
      if (typeof sender === 'string') return sender
      return sender?.login
    },
  },
  'stripe-events': {
    stripeEvent: (d, r) => {
      // Try data.type first, then parse from event name (webhook.stripe.payment_intent.succeeded)
      if (d.type) return d.type
      const ev = r.event as string | undefined
      if (ev?.startsWith('webhook.stripe.')) return ev.slice('webhook.stripe.'.length)
      return undefined
    },
    objectType: (d) => {
      const obj = (d.data as Record<string, unknown>)?.object as Record<string, unknown> | undefined
      return obj?.object
    },
    objectId: (d) => {
      const obj = (d.data as Record<string, unknown>)?.object as Record<string, unknown> | undefined
      return obj?.id
    },
  },
  'api-requests': {
    method: (d) => {
      // Tail trace format: data.request.method or data.method
      const req = d.request as Record<string, unknown> | undefined
      return req?.method ?? d.method
    },
    path: (d) => {
      const req = d.request as Record<string, unknown> | undefined
      if (req?.url) {
        try {
          return new URL(req.url as string).pathname
        } catch {}
      }
      return d.path ?? d.url
    },
    status: (d) => {
      const resp = d.response as Record<string, unknown> | undefined
      return resp?.status ?? d.status
    },
    latency: (d) => d.latency ?? d.duration,
  },
}

interface CHWhereResult {
  sql: string
  params: Record<string, string | number>
}

export function buildCHWhere(where: Where | undefined, context: string, skipNsFilter = false, baseFilter?: string): CHWhereResult {
  const params: Record<string, string | number> = {}
  const conditions: string[] = []
  if (!skipNsFilter) {
    params.ctx = context
    conditions.push('ns = {ctx:String}')
  }
  if (baseFilter) {
    conditions.push(baseFilter)
  }
  let paramIdx = 0

  if (where && Object.keys(where).length > 0) {
    const result = buildCHConditions(where, params, paramIdx)
    if (result.sql) conditions.push(result.sql)
    paramIdx = result.paramIdx
  }

  return { sql: conditions.length > 0 ? conditions.join(' AND ') : '1=1', params }
}

function buildCHConditions(where: Where, params: Record<string, string | number>, paramIdx: number): { sql: string; paramIdx: number } {
  const parts: string[] = []

  if (where.and && Array.isArray(where.and)) {
    const andParts: string[] = []
    for (const w of where.and) {
      const result = buildCHConditions(w as Where, params, paramIdx)
      if (result.sql) {
        andParts.push(result.sql)
        paramIdx = result.paramIdx
      }
    }
    if (andParts.length > 0) {
      parts.push(`(${andParts.join(' AND ')})`)
    }
  }

  if (where.or && Array.isArray(where.or)) {
    const orParts: string[] = []
    for (const w of where.or) {
      const result = buildCHConditions(w as Where, params, paramIdx)
      if (result.sql) {
        orParts.push(result.sql)
        paramIdx = result.paramIdx
      }
    }
    if (orParts.length > 0) {
      parts.push(`(${orParts.join(' OR ')})`)
    }
  }

  for (const [field, operators] of Object.entries(where)) {
    if (field === 'and' || field === 'or') continue
    if (typeof operators !== 'object' || operators === null) continue
    const result = buildCHFieldConditions(field, operators as Record<string, unknown>, params, paramIdx)
    if (result.sql) {
      parts.push(result.sql)
      paramIdx = result.paramIdx
    }
  }

  return { sql: parts.join(' AND '), paramIdx }
}

const CH_COLUMN_MAP: Record<string, string> = {
  id: 'id',
  ns: 'ns',
  ts: 'ts',
  type: 'type',
  event: 'event',
  source: 'source',
  data: 'data',
  createdAt: 'ts',
  updatedAt: 'ts',
  timestamp: 'ts',
  name: 'event',
  url: 'url',
  actor: 'actor',
  meta: 'meta',
}

function chColumn(field: string): string | null {
  return CH_COLUMN_MAP[field] ?? null
}

function buildCHFieldConditions(
  field: string,
  operators: Record<string, unknown>,
  params: Record<string, string | number>,
  paramIdx: number,
): { sql: string; paramIdx: number } {
  const col = chColumn(field)
  if (!col) return { sql: '', paramIdx }
  const parts: string[] = []

  for (const [op, value] of Object.entries(operators)) {
    const pname = `p${paramIdx++}`

    switch (op) {
      case 'equals':
        if (value === null || value === undefined) {
          parts.push(`${col} IS NULL`)
        } else {
          params[pname] = value as string | number
          parts.push(`${col} = {${pname}:String}`)
        }
        break
      case 'not_equals':
        if (value === null || value === undefined) {
          parts.push(`${col} IS NOT NULL`)
        } else {
          params[pname] = value as string | number
          parts.push(`${col} != {${pname}:String}`)
        }
        break
      case 'greater_than':
        params[pname] = value as string | number
        parts.push(`${col} > {${pname}:String}`)
        break
      case 'greater_than_equal':
        params[pname] = value as string | number
        parts.push(`${col} >= {${pname}:String}`)
        break
      case 'less_than':
        params[pname] = value as string | number
        parts.push(`${col} < {${pname}:String}`)
        break
      case 'less_than_equal':
        params[pname] = value as string | number
        parts.push(`${col} <= {${pname}:String}`)
        break
      case 'contains':
        if (value) {
          const escaped = String(value).replace(/[%_\\]/g, '\\$&')
          params[pname] = `%${escaped}%`
          parts.push(`${col} LIKE {${pname}:String}`)
        }
        break
      case 'in': {
        const arr = Array.isArray(value) ? value : [value]
        const inParts: string[] = []
        for (const v of arr) {
          const inP = `p${paramIdx++}`
          params[inP] = v as string | number
          inParts.push(`{${inP}:String}`)
        }
        parts.push(`${col} IN (${inParts.join(', ')})`)
        break
      }
      case 'not_in': {
        const arr = Array.isArray(value) ? value : [value]
        const inParts: string[] = []
        for (const v of arr) {
          const inP = `p${paramIdx++}`
          params[inP] = v as string | number
          inParts.push(`{${inP}:String}`)
        }
        parts.push(`${col} NOT IN (${inParts.join(', ')})`)
        break
      }
      case 'exists':
        parts.push(value ? `${col} IS NOT NULL` : `${col} IS NULL`)
        break
    }
  }

  return { sql: parts.join(' AND '), paramIdx }
}

/** Lightweight columns for list views — avoids reading heavy JSON blobs. */
const CH_LIST_COLUMNS = 'id, ray, ns, ts, type, event, source, url, file, ingested'

export async function chFind(
  service: PayloadDatabaseService,
  collection: string,
  context: string,
  args: { where?: Where; sort?: string | string[]; limit?: number; page?: number; pagination?: boolean },
): Promise<{ docs: Record<string, unknown>[]; totalDocs: number }> {
  try {
    const table = 'events'
    const { where, sort, limit: rawLimit = 10, page = 1, pagination = true } = args
    // All event collections are platform-wide; skip ns tenant filter
    const skipNs = true
    const baseFilter = CH_COLLECTION_FILTERS[collection]
    const { sql: whereSql, params } = buildCHWhere(where, context, skipNs, baseFilter)
    const limit = pagination ? rawLimit : 0

    // Run count + data queries in parallel to halve latency
    const countParams = { ...params }
    const dataParams = { ...params }

    let dataSql = `SELECT ${CH_LIST_COLUMNS} FROM ${table} WHERE ${whereSql}`
    dataSql += ` ${buildCHOrderClause(sort)}`

    if (limit > 0) {
      dataParams.lim = limit
      dataParams.off = (page - 1) * limit
      dataSql += ' LIMIT {lim:UInt32} OFFSET {off:UInt32}'
    }

    const [countResult, result] = await Promise.all([
      service.chQuery(`SELECT count() AS total FROM ${table} WHERE ${whereSql}`, countParams) as Promise<{ data: { total: number }[] }>,
      service.chQuery(dataSql, dataParams),
    ])

    const totalDocs = countResult.data[0]?.total ?? 0
    const docs = result.data.map((row) => chRowToDocument(row, collection))

    return { docs, totalDocs }
  } catch (err) {
    console.error(`chFind(${collection}) failed:`, err)
    return { docs: [], totalDocs: 0 }
  }
}

export async function chFindOne(service: PayloadDatabaseService, collection: string, context: string, where?: Where): Promise<Record<string, unknown> | null> {
  try {
    const table = 'events'
    const skipNs = true
    const baseFilter = CH_COLLECTION_FILTERS[collection]
    const { sql: whereSql, params } = buildCHWhere(where, context, skipNs, baseFilter)

    const result = await service.chQuery(`SELECT * FROM ${table} WHERE ${whereSql} LIMIT 1`, params)
    if (result.data.length === 0) return null
    return chRowToDocument(result.data[0], collection)
  } catch (err) {
    console.error(`chFindOne(${collection}) failed:`, err)
    return null
  }
}

export async function chCount(service: PayloadDatabaseService, collection: string, context: string, where?: Where): Promise<number> {
  try {
    const table = 'events'
    const skipNs = true
    const baseFilter = CH_COLLECTION_FILTERS[collection]
    const { sql: whereSql, params } = buildCHWhere(where, context, skipNs, baseFilter)

    const result = (await service.chQuery(`SELECT count() AS total FROM ${table} WHERE ${whereSql}`, params)) as { data: { total: number }[] }
    return result.data[0]?.total ?? 0
  } catch (err) {
    console.error(`chCount(${collection}) failed:`, err)
    return 0
  }
}

export function buildCHOrderClause(sort: string | string[] | undefined): string {
  const fields = Array.isArray(sort) ? sort : sort ? [sort] : []
  const parts: string[] = []

  for (const sortStr of fields) {
    if (!sortStr) continue
    const dir = sortStr.startsWith('-') ? 'DESC' : 'ASC'
    const field = sortStr.replace(/^-/, '')
    const col = chColumn(field)
    if (col) {
      parts.push(`${col} ${dir}`)
    }
  }

  if (parts.length === 0) return 'ORDER BY id DESC'
  return `ORDER BY ${parts.join(', ')}`
}

function chRowToDocument(row: Record<string, unknown>, collection?: string): Record<string, unknown> {
  const doc: Record<string, unknown> = {
    ...row,
  }

  // Parse JSON strings
  if (row.data && typeof row.data === 'string') {
    try {
      doc.data = JSON.parse(row.data as string)
    } catch {}
  }
  if (row.meta && typeof row.meta === 'string') {
    try {
      doc.meta = JSON.parse(row.meta as string)
    } catch {}
  }

  // Map ts → createdAt/updatedAt for Payload
  if (row.ts) {
    doc.createdAt = row.ts
    doc.updatedAt = row.ts
  }

  // Extract collection-specific fields from data
  if (collection && FIELD_EXTRACTORS[collection] && doc.data && typeof doc.data === 'object') {
    const extractors = FIELD_EXTRACTORS[collection]
    for (const [field, extractor] of Object.entries(extractors)) {
      doc[field] = extractor(doc.data as Record<string, unknown>, row)
    }
  }

  return doc
}
