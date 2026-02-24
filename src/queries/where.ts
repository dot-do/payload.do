/**
 * Translate Payload Where objects to MongoDB-style filters
 * that the DatabaseDO collection API understands.
 *
 * Payload Where: { email: { equals: 'foo@bar.com' }, status: { in: ['active', 'pending'] } }
 * MongoDB-style:  { email: { $eq: 'foo@bar.com' }, status: { $in: ['active', 'pending'] } }
 */

type PayloadWhere = Record<string, unknown>
type MongoFilter = Record<string, unknown>

/**
 * Map Payload operator names to MongoDB-style operators.
 */
const OPERATOR_MAP: Record<string, string> = {
  equals: '$eq',
  not_equals: '$ne',
  greater_than: '$gt',
  greater_than_equal: '$gte',
  less_than: '$lt',
  less_than_equal: '$lte',
  in: '$in',
  not_in: '$nin',
  exists: '$exists',
  like: '$regex',
  contains: '$regex',
}

/**
 * Translate a single Payload field condition to a MongoDB-style filter entry.
 */
function translateFieldCondition(operators: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [op, value] of Object.entries(operators)) {
    const mongoOp = OPERATOR_MAP[op]
    if (!mongoOp) continue

    if (op === 'like' || op === 'contains') {
      // Convert Payload LIKE pattern to regex
      // Payload uses % for wildcard; convert to regex .*
      const pattern = typeof value === 'string' ? value.replace(/%/g, '.*') : String(value)
      result[mongoOp] = pattern
    } else {
      result[mongoOp] = value
    }
  }

  return result
}

/**
 * Map Payload field names to entity data field names.
 * Payload uses 'id' and 'createdAt'/'updatedAt' which map to $id/$createdAt/$updatedAt in the collection API.
 */
function mapFieldName(field: string): string {
  switch (field) {
    case 'id':
      return '$id'
    case 'createdAt':
      return '$createdAt'
    case 'updatedAt':
      return '$updatedAt'
    default:
      return field
  }
}

/**
 * Translate a Payload Where clause to a MongoDB-style filter for the collection API.
 *
 * Handles:
 * - Simple field conditions: { email: { equals: 'foo' } }
 * - AND/OR combinators: { and: [{ ... }, { ... }] }, { or: [{ ... }, { ... }] }
 * - Direct equality shorthand: { email: 'foo@bar.com' }
 * - Nested field paths: { 'user.name': { equals: 'Joe' } }
 */
export function translateWhere(where?: PayloadWhere): MongoFilter | undefined {
  if (!where || Object.keys(where).length === 0) return undefined

  const filter: MongoFilter = {}

  for (const [key, value] of Object.entries(where)) {
    // AND combinator
    if (key === 'and' && Array.isArray(value)) {
      // Merge all AND conditions into the top-level filter
      for (const sub of value) {
        const subFilter = translateWhere(sub as PayloadWhere)
        if (subFilter) Object.assign(filter, subFilter)
      }
      continue
    }

    // OR combinator — not directly supported by the collection API's matchesFilter.
    // We flatten simple OR conditions where possible.
    if (key === 'or' && Array.isArray(value)) {
      // For simple OR cases with a single field per clause using 'equals',
      // we can convert to $in. Otherwise, skip (collection API limitation).
      const fieldValues = new Map<string, unknown[]>()
      let canFlatten = true

      for (const sub of value) {
        const entries = Object.entries(sub as PayloadWhere)
        if (entries.length !== 1) {
          canFlatten = false
          break
        }
        const [field, condition] = entries[0]
        if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
          const ops = condition as Record<string, unknown>
          if ('equals' in ops) {
            const mapped = mapFieldName(field)
            const existing = fieldValues.get(mapped) ?? []
            existing.push(ops.equals)
            fieldValues.set(mapped, existing)
            continue
          }
        }
        canFlatten = false
        break
      }

      if (canFlatten) {
        for (const [field, values] of fieldValues) {
          filter[field] = { $in: values }
        }
      } else {
        // Complex OR not supported by collection API's in-memory filter.
        // Log warning so callers can fall back to SQL escape hatch if needed.
        console.warn('[dotdo-payload] Complex OR condition not supported by collection filter — results may include non-matching docs')
      }
      continue
    }

    const mappedField = mapFieldName(key)

    // Direct equality (value is not an operator object)
    if (value === null || value === undefined || typeof value !== 'object' || Array.isArray(value)) {
      filter[mappedField] = value
      continue
    }

    // Check if value is operator conditions
    const ops = value as Record<string, unknown>
    const hasPayloadOps = Object.keys(ops).some((k) => OPERATOR_MAP[k])

    if (hasPayloadOps) {
      filter[mappedField] = translateFieldCondition(ops)
    } else {
      // Nested condition or direct equality to an object
      filter[mappedField] = value
    }
  }

  return Object.keys(filter).length > 0 ? filter : undefined
}
