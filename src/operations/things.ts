/**
 * Things — Universal data operations.
 *
 * The Things collection is a unified view of ALL entities in the DO's SQLite
 * entities table, regardless of type. It uses raw SQL via service.query()
 * to bypass the per-type getCollection() filter.
 *
 * Analogous to how CH_COLLECTIONS route event reads to ClickHouse,
 * Things routes entity reads to raw SQL over the full entities table.
 */

import type { Where } from 'payload'
import type { PayloadDatabaseService } from '../types.js'
import { sanitizeField } from '../queries/sql.js'

export const THINGS_COLLECTION = 'things'

/** Direct column mappings for the entities table. */
const COLUMN_MAP: Record<string, string> = {
  id: 'id',
  type: 'type',
  createdAt: 'created_at',
  updatedAt: 'updated_at',
}

interface EntityRow {
  id: string
  type: string
  data: string
  created_at: string
  updated_at: string
}

/** Parse a raw entities row into a Payload-compatible document. */
function rowToDocument(row: EntityRow): Record<string, unknown> {
  let parsed: Record<string, unknown> = {}
  try {
    parsed = typeof row.data === 'string' ? JSON.parse(row.data) : (row.data as unknown as Record<string, unknown>) ?? {}
  } catch {}

  return {
    id: row.id,
    type: row.type,
    ...parsed,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}

/** Build a SQLite WHERE clause from a Payload Where object. */
function buildWhere(where?: Where): { sql: string; params: unknown[] } {
  const conditions: string[] = ['deleted_at IS NULL']
  const params: unknown[] = []

  if (!where || Object.keys(where).length === 0) {
    return { sql: conditions.join(' AND '), params }
  }

  // Handle AND combinator
  if (where.and && Array.isArray(where.and)) {
    for (const sub of where.and) {
      const result = buildWhere(sub as Where)
      if (result.sql !== 'deleted_at IS NULL') {
        // Extract the conditions beyond the base one
        conditions.push(`(${result.sql.replace('deleted_at IS NULL AND ', '')})`)
        params.push(...result.params)
      }
    }
  }

  // Handle OR combinator
  if (where.or && Array.isArray(where.or)) {
    const orParts: string[] = []
    for (const sub of where.or) {
      const result = buildWhere(sub as Where)
      const inner = result.sql.replace('deleted_at IS NULL AND ', '').replace('deleted_at IS NULL', '1=1')
      orParts.push(inner)
      params.push(...result.params)
    }
    if (orParts.length > 0) {
      conditions.push(`(${orParts.join(' OR ')})`)
    }
  }

  for (const [field, value] of Object.entries(where)) {
    if (field === 'and' || field === 'or') continue

    const col = COLUMN_MAP[field]
    const target = col ?? `json_extract(data, '$.${sanitizeField(field)}')`

    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      // Direct equality shorthand
      conditions.push(`${target} = ?`)
      params.push(value)
      continue
    }

    const ops = value as Record<string, unknown>
    for (const [op, opValue] of Object.entries(ops)) {
      switch (op) {
        case 'equals':
          if (opValue === null || opValue === undefined) {
            conditions.push(`${target} IS NULL`)
          } else {
            conditions.push(`${target} = ?`)
            params.push(opValue)
          }
          break
        case 'not_equals':
          if (opValue === null || opValue === undefined) {
            conditions.push(`${target} IS NOT NULL`)
          } else {
            conditions.push(`${target} != ?`)
            params.push(opValue)
          }
          break
        case 'greater_than':
          conditions.push(`${target} > ?`)
          params.push(opValue)
          break
        case 'greater_than_equal':
          conditions.push(`${target} >= ?`)
          params.push(opValue)
          break
        case 'less_than':
          conditions.push(`${target} < ?`)
          params.push(opValue)
          break
        case 'less_than_equal':
          conditions.push(`${target} <= ?`)
          params.push(opValue)
          break
        case 'in':
          if (Array.isArray(opValue) && opValue.length > 0) {
            conditions.push(`${target} IN (${opValue.map(() => '?').join(', ')})`)
            params.push(...opValue)
          }
          break
        case 'not_in':
          if (Array.isArray(opValue) && opValue.length > 0) {
            conditions.push(`${target} NOT IN (${opValue.map(() => '?').join(', ')})`)
            params.push(...opValue)
          }
          break
        case 'contains':
        case 'like':
          if (opValue) {
            conditions.push(`${target} LIKE ?`)
            params.push(`%${opValue}%`)
          }
          break
        case 'exists':
          conditions.push(opValue ? `${target} IS NOT NULL` : `${target} IS NULL`)
          break
      }
    }
  }

  return { sql: conditions.join(' AND '), params }
}

/** Build ORDER BY from Payload sort string. */
function buildOrderBy(sort?: string | string[]): string {
  const fields = Array.isArray(sort) ? sort : sort ? [sort] : []
  const parts: string[] = []

  for (const raw of fields) {
    if (!raw) continue
    const dir = raw.startsWith('-') ? 'DESC' : 'ASC'
    const field = raw.replace(/^-/, '')
    const col = COLUMN_MAP[field]
    if (col) {
      parts.push(`${col} ${dir}`)
    } else {
      // Sort by JSON field
      parts.push(`json_extract(data, '$.${sanitizeField(field)}') ${dir}`)
    }
  }

  return parts.length > 0 ? `ORDER BY ${parts.join(', ')}` : 'ORDER BY updated_at DESC'
}

/** Find all entities across all types. */
export async function thingsFind(
  service: PayloadDatabaseService,
  namespace: string,
  args: { where?: Where; sort?: string | string[]; limit?: number; page?: number; pagination?: boolean },
): Promise<{ docs: Record<string, unknown>[]; totalDocs: number }> {
  const { where, sort, limit: rawLimit = 10, page = 1, pagination = true } = args
  const { sql: whereSql, params } = buildWhere(where)
  const limit = pagination ? rawLimit : 0
  const offset = limit > 0 ? (page - 1) * limit : 0

  // Count
  const countRows = await service.query(namespace, `SELECT COUNT(*) as cnt FROM entities WHERE ${whereSql}`, ...params)
  const totalDocs = (countRows[0]?.cnt as number) ?? 0

  // Data
  const orderBy = buildOrderBy(sort)
  let dataSql = `SELECT id, type, data, created_at, updated_at FROM entities WHERE ${whereSql} ${orderBy}`
  const dataParams = [...params]

  if (limit > 0) {
    dataSql += ' LIMIT ? OFFSET ?'
    dataParams.push(limit, offset)
  }

  const rows = await service.query(namespace, dataSql, ...dataParams)
  const docs = rows.map((row) => rowToDocument(row as unknown as EntityRow))

  return { docs, totalDocs }
}

/** Find one entity across all types. */
export async function thingsFindOne(service: PayloadDatabaseService, namespace: string, where?: Where): Promise<Record<string, unknown> | null> {
  const { sql: whereSql, params } = buildWhere(where)
  const rows = await service.query(namespace, `SELECT id, type, data, created_at, updated_at FROM entities WHERE ${whereSql} LIMIT 1`, ...params)
  if (rows.length === 0) return null
  return rowToDocument(rows[0] as unknown as EntityRow)
}

/** Count entities across all types. */
export async function thingsCount(service: PayloadDatabaseService, namespace: string, where?: Where): Promise<number> {
  const { sql: whereSql, params } = buildWhere(where)
  const rows = await service.query(namespace, `SELECT COUNT(*) as cnt FROM entities WHERE ${whereSql}`, ...params)
  return (rows[0]?.cnt as number) ?? 0
}

/** Resolve the entity type for a given ID (used by update/delete operations). */
export async function thingsResolveType(service: PayloadDatabaseService, namespace: string, id: string): Promise<string | null> {
  const rows = await service.query(namespace, 'SELECT type FROM entities WHERE id = ? AND deleted_at IS NULL', id)
  return (rows[0]?.type as string) ?? null
}
