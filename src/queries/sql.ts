/**
 * SQL escape hatch for complex Payload Where clauses.
 *
 * When the collection API's in-memory filter matching isn't sufficient
 * (e.g., DISTINCT queries, complex nested conditions), we fall back to
 * building SQL against the entities table using json_extract().
 */

type PayloadWhere = Record<string, unknown>

const FIELD_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export function sanitizeField(field: string): string {
  if (!FIELD_RE.test(field)) throw new Error(`Invalid field name: ${field}`)
  return field
}

/**
 * Map a Payload field name to a SQL expression for the entities table.
 */
function fieldToSql(field: string): string {
  switch (field) {
    case 'id':
      return 'id'
    case 'createdAt':
      return 'created_at'
    case 'updatedAt':
      return 'updated_at'
    case 'type':
      return 'type'
    default:
      sanitizeField(field)
      return `json_extract(data, '$.${field}')`
  }
}

/**
 * Build a WHERE clause + params from a Payload Where object.
 * Returns { sql, params } for use with the SQL escape hatch.
 */
export function buildWhereSql(type: string, where?: PayloadWhere): { sql: string; params: unknown[] } {
  const conditions: string[] = ['type = ?', 'deleted_at IS NULL']
  const params: unknown[] = [type]

  if (where) {
    const result = buildConditions(where)
    if (result.sql) {
      conditions.push(result.sql)
      params.push(...result.params)
    }
  }

  return { sql: conditions.join(' AND '), params }
}

function buildConditions(where: PayloadWhere): { sql: string; params: unknown[] } {
  const parts: string[] = []
  const params: unknown[] = []

  for (const [key, value] of Object.entries(where)) {
    if (key === 'and' && Array.isArray(value)) {
      const sub = value.map((w) => buildConditions(w as PayloadWhere))
      const sqls = sub.filter((s) => s.sql).map((s) => `(${s.sql})`)
      if (sqls.length > 0) {
        parts.push(`(${sqls.join(' AND ')})`)
        for (const s of sub) params.push(...s.params)
      }
      continue
    }

    if (key === 'or' && Array.isArray(value)) {
      const sub = value.map((w) => buildConditions(w as PayloadWhere))
      const sqls = sub.filter((s) => s.sql).map((s) => `(${s.sql})`)
      if (sqls.length > 0) {
        parts.push(`(${sqls.join(' OR ')})`)
        for (const s of sub) params.push(...s.params)
      }
      continue
    }

    if (value === null || value === undefined || typeof value !== 'object' || Array.isArray(value)) {
      parts.push(`${fieldToSql(key)} = ?`)
      params.push(value)
      continue
    }

    const ops = value as Record<string, unknown>
    for (const [op, opValue] of Object.entries(ops)) {
      const col = fieldToSql(key)
      switch (op) {
        case 'equals':
          if (opValue === null) {
            parts.push(`${col} IS NULL`)
          } else {
            parts.push(`${col} = ?`)
            params.push(opValue)
          }
          break
        case 'not_equals':
          if (opValue === null) {
            parts.push(`${col} IS NOT NULL`)
          } else {
            parts.push(`${col} != ?`)
            params.push(opValue)
          }
          break
        case 'greater_than':
          parts.push(`${col} > ?`)
          params.push(opValue)
          break
        case 'greater_than_equal':
          parts.push(`${col} >= ?`)
          params.push(opValue)
          break
        case 'less_than':
          parts.push(`${col} < ?`)
          params.push(opValue)
          break
        case 'less_than_equal':
          parts.push(`${col} <= ?`)
          params.push(opValue)
          break
        case 'in':
          if (Array.isArray(opValue) && opValue.length > 0) {
            parts.push(`${col} IN (${opValue.map(() => '?').join(', ')})`)
            params.push(...opValue)
          }
          break
        case 'not_in':
          if (Array.isArray(opValue) && opValue.length > 0) {
            parts.push(`${col} NOT IN (${opValue.map(() => '?').join(', ')})`)
            params.push(...opValue)
          }
          break
        case 'like': {
          // Payload's 'like' already includes % wildcards
          parts.push(`${col} LIKE ?`)
          params.push(opValue)
          break
        }
        case 'contains': {
          // Wrap value with % for substring matching
          parts.push(`${col} LIKE ?`)
          params.push(`%${opValue}%`)
          break
        }
        case 'exists':
          parts.push(opValue ? `${col} IS NOT NULL` : `${col} IS NULL`)
          break
      }
    }
  }

  return { sql: parts.join(' AND '), params }
}

/**
 * Build an ORDER BY clause from a Payload sort string.
 */
export function buildOrderSql(sort?: string): string {
  if (!sort) return 'ORDER BY created_at DESC'

  const fields = sort.split(',')
  const parts = fields.map((raw) => {
    const field = raw.trim()
    if (!field) return null
    if (field.startsWith('-')) {
      return `${fieldToSql(field.slice(1))} DESC`
    }
    return `${fieldToSql(field)} ASC`
  }).filter(Boolean)

  return parts.length > 0 ? `ORDER BY ${parts.join(', ')}` : 'ORDER BY created_at DESC'
}
