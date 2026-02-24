/**
 * Compute a deterministic 8-char hex hash of a schema object.
 * Used for O(1) schema drift detection — NOT for security.
 *
 * Uses djb2a (xor variant) — synchronous, no crypto dependency,
 * deterministic across Workers/Node/Vitest environments.
 */

/** Sort object keys recursively for deterministic serialization. */
function sortKeys(obj: unknown): unknown {
  if (obj === null || typeof obj !== 'object') return obj
  if (Array.isArray(obj)) return obj.map(sortKeys)
  const sorted: Record<string, unknown> = {}
  for (const key of Object.keys(obj as Record<string, unknown>).sort()) {
    sorted[key] = sortKeys((obj as Record<string, unknown>)[key])
  }
  return sorted
}

/** djb2a hash — fast, deterministic, no dependencies. */
function djb2a(str: string): number {
  let hash = 5381
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) ^ str.charCodeAt(i)
  }
  return hash >>> 0
}

export function computeSchemaHash(schema: unknown): string {
  const json = JSON.stringify(sortKeys(schema))
  return djb2a(json).toString(16).padStart(8, '0')
}
