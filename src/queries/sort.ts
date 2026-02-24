/**
 * Translate Payload sort strings to MongoDB-style sort objects
 * for the DatabaseDO collection API.
 *
 * Payload sort: '-createdAt' (descending) or 'title' (ascending)
 * MongoDB-style: { createdAt: -1 } or { title: 1 }
 */

/**
 * Map Payload field names to entity meta-field names for sorting.
 */
function mapSortField(field: string): string {
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
 * Parse a Payload sort string into a MongoDB-style sort object.
 * Supports comma-separated fields: '-createdAt,title'
 */
export function translateSort(sort?: string | string[]): Record<string, 1 | -1> | undefined {
  if (!sort) return undefined

  const fields = typeof sort === 'string' ? sort.split(',') : sort
  const result: Record<string, 1 | -1> = {}

  for (const raw of fields) {
    const field = raw.trim()
    if (!field) continue

    if (field.startsWith('-')) {
      result[mapSortField(field.slice(1))] = -1
    } else {
      result[mapSortField(field)] = 1
    }
  }

  return Object.keys(result).length > 0 ? result : undefined
}
