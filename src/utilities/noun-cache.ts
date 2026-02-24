import type { PayloadDatabaseService } from '../types.js'
import type { NounContext } from './transforms.js'
import type { MigrationDef } from './migrate.js'
import { computeSchemaHash } from './schema-hash.js'

const CACHE_TTL_MS = 60_000 // 60 seconds

interface CachedValue<T> {
  value: T
  ts: number
}

/** Cached slug → Noun entity ID mapping with TTL. */
const nounIdCache = new Map<string, CachedValue<string>>()

/** Cached slug → Noun schema info with TTL. */
const nounSchemaCache = new Map<string, CachedValue<{ schemaVersion: number; schemaHash: string }>>()

/** Cached slug → full NounContext (schema + migrations) with TTL. */
const nounContextCache = new Map<string, CachedValue<NounContext>>()

function getCached<T>(cache: Map<string, CachedValue<T>>, key: string): T | undefined {
  const entry = cache.get(key)
  if (!entry) return undefined
  if (Date.now() - entry.ts > CACHE_TTL_MS) {
    cache.delete(key)
    return undefined
  }
  return entry.value
}

/** Invalidate all cached entries for a given namespace and slug. */
export function invalidateNounCache(namespace: string, slug: string): void {
  const key = `${namespace}:${slug}`
  nounIdCache.delete(key)
  nounSchemaCache.delete(key)
  nounContextCache.delete(key)
}

/** Resolve the Noun entity ID for a collection slug. */
export async function resolveNounId(service: PayloadDatabaseService, namespace: string, slug: string): Promise<string | null> {
  const cacheKey = `${namespace}:${slug}`
  const cached = getCached(nounIdCache, cacheKey)
  if (cached) return cached

  try {
    const rows = await service.query(namespace, "SELECT id FROM entities WHERE type = 'Noun' AND json_extract(data, '$.slug') = ? AND deleted_at IS NULL LIMIT 1", slug)
    const id = rows[0]?.id as string | undefined
    if (id) {
      nounIdCache.set(cacheKey, { value: id, ts: Date.now() })
      return id
    }
  } catch {
    // Noun may not be seeded yet — not fatal
  }
  return null
}

/** Resolve schema version + hash for a collection slug's Noun. */
export async function resolveNounSchema(
  service: PayloadDatabaseService,
  namespace: string,
  slug: string,
): Promise<{ schemaVersion: number; schemaHash: string } | null> {
  const cacheKey = `${namespace}:${slug}`
  const cached = getCached(nounSchemaCache, cacheKey)
  if (cached) return cached

  try {
    const rows = await service.query(
      namespace,
      "SELECT json_extract(data, '$.schema') as schema, json_extract(data, '$.migrations') as migrations FROM entities WHERE type = 'Noun' AND json_extract(data, '$.slug') = ? AND deleted_at IS NULL LIMIT 1",
      slug,
    )
    if (rows[0]) {
      const schema = rows[0].schema
      const migrations = rows[0].migrations
      let parsed: unknown[] = []
      try {
        parsed = typeof migrations === 'string' ? JSON.parse(migrations) : (migrations as unknown[]) ?? []
      } catch {}
      const schemaVersion = parsed.length > 0 ? Math.max(...parsed.map((m: any) => m.version ?? 0)) : 1
      const schemaHash = computeSchemaHash(typeof schema === 'string' ? JSON.parse(schema) : schema ?? {})
      const info = { schemaVersion, schemaHash }
      nounSchemaCache.set(cacheKey, { value: info, ts: Date.now() })
      return info
    }
  } catch {
    // Not fatal
  }
  return null
}

/** Resolve the full NounContext for a collection slug (schema + migrations for read-time migration). */
export async function resolveNounContext(
  service: PayloadDatabaseService,
  namespace: string,
  slug: string,
): Promise<NounContext | null> {
  const cacheKey = `${namespace}:${slug}`
  const cached = getCached(nounContextCache, cacheKey)
  if (cached) return cached

  try {
    const rows = await service.query(
      namespace,
      "SELECT json_extract(data, '$.schema') as schema, json_extract(data, '$.migrations') as migrations FROM entities WHERE type = 'Noun' AND json_extract(data, '$.slug') = ? AND deleted_at IS NULL LIMIT 1",
      slug,
    )
    if (rows[0]) {
      const schema = rows[0].schema
      const rawMigrations = rows[0].migrations
      let migrations: MigrationDef[] = []
      try {
        const parsed = typeof rawMigrations === 'string' ? JSON.parse(rawMigrations) : (rawMigrations as unknown[]) ?? []
        migrations = (parsed as MigrationDef[]).filter((m) => m && typeof m.version === 'number')
      } catch {}
      const schemaVersion = migrations.length > 0 ? Math.max(...migrations.map((m) => m.version ?? 0)) : 1
      const schemaHash = computeSchemaHash(typeof schema === 'string' ? JSON.parse(schema) : schema ?? {})
      const ctx: NounContext = { schemaHash, schemaVersion, migrations }
      nounContextCache.set(cacheKey, { value: ctx, ts: Date.now() })
      return ctx
    }
  } catch {
    // Not fatal
  }
  return null
}
