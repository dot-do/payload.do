import type { FindDistinct } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { buildPagination } from '../utilities/pagination.js'
import { slugToType } from '../utilities/transforms.js'
import { buildWhereSql, sanitizeField } from '../queries/sql.js'

/**
 * DISTINCT queries use the SQL escape hatch since the collection API
 * doesn't support DISTINCT directly.
 */
export const findDistinct: FindDistinct = async function findDistinct(this: DoPayloadAdapter, args: any) {
  const { collection, field, limit = 10, page = 1, sort, where } = args

  const type = slugToType(collection)
  const { sql: whereClause, params } = buildWhereSql(type, where)

  // Map field to SQL expression (sanitize to prevent injection)
  const fieldMap: Record<string, string> = { id: 'id', createdAt: 'created_at', updatedAt: 'updated_at', type: 'type' }
  const fieldExpr = fieldMap[field] ?? `json_extract(data, '$.${sanitizeField(field)}')`

  const dir = sort?.startsWith?.('-') ? 'DESC' : 'ASC'
  const offset = (page - 1) * limit

  // Count distinct values
  const countSql = `SELECT COUNT(DISTINCT ${fieldExpr}) as total FROM entities WHERE ${whereClause}`
  const countRows = await this._service.query(this.namespace, countSql, ...params)
  const totalDocs = (countRows[0]?.total as number) ?? 0

  // Get distinct values
  let dataSql = `SELECT DISTINCT ${fieldExpr} as value FROM entities WHERE ${whereClause} ORDER BY value ${dir}`
  const dataParams = [...params]
  if (limit > 0) {
    dataSql += ' LIMIT ? OFFSET ?'
    dataParams.push(limit, offset)
  }

  const rows = await this._service.query(this.namespace, dataSql, ...dataParams)
  const values = rows.map((row) => ({ [field]: row.value }))

  return { values, ...buildPagination(totalDocs, limit, page) } as any
}
