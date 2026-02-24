/**
 * Lightweight data migration system for .do/payload.
 *
 * Since PayloadDatabaseDO uses entities table with JSON data,
 * there are no schema migrations — only data transformations.
 * Completed migrations are tracked as type = '_migrations' entities.
 *
 * Migrations run automatically during connect() for each adapter instance.
 */

import type { PayloadDatabaseService } from './types.js'

export interface DataMigration {
  /** Unique migration name (timestamp prefix recommended) */
  name: string
  /** Transform data. Receives the service and namespace. */
  up(service: PayloadDatabaseService, namespace: string): Promise<void>
  /** Reverse the transformation (best-effort). */
  down(service: PayloadDatabaseService, namespace: string): Promise<void>
}

/** Ordered list of all registered data migrations. */
const migrations: DataMigration[] = []

/** Register a migration. Call order determines execution order. */
export function registerMigration(migration: DataMigration): void {
  migrations.push(migration)
}

/** Get all registered migrations. */
export function getAllMigrations(): DataMigration[] {
  return [...migrations]
}

/** Get names of migrations that have already been applied. */
async function getAppliedMigrations(service: PayloadDatabaseService, namespace: string): Promise<Set<string>> {
  const rows = await service.query(namespace, "SELECT json_extract(data, '$.name') as name FROM data WHERE type = '_migrations' AND data IS NOT NULL")
  return new Set(rows.map((r) => r.name as string))
}

/** Run all pending migrations in order. Returns count of migrations applied. */
export async function runPendingMigrations(service: PayloadDatabaseService, namespace: string): Promise<number> {
  if (migrations.length === 0) return 0

  const applied = await getAppliedMigrations(service, namespace)
  let count = 0

  for (const migration of migrations) {
    if (applied.has(migration.name)) continue

    console.log(`[migration] Running: ${migration.name}`)
    try {
      await migration.up(service, namespace)

      // Record as applied
      const now = Date.now()
      const record = JSON.stringify({ name: migration.name, appliedAt: new Date(now).toISOString() })
      await service.run(namespace, 'INSERT INTO data (type, title, c, v, data) VALUES (?, ?, ?, ?, ?)', '_migrations', migration.name, now, now, record)

      console.log(`[migration] Completed: ${migration.name}`)
      count++
    } catch (err) {
      console.error(`[migration] Failed: ${migration.name}`, err)
      throw err
    }
  }

  return count
}

/** Get migration status for all registered migrations. */
export async function getMigrationStatus(
  service: PayloadDatabaseService,
  namespace: string,
): Promise<Array<{ name: string; applied: boolean; appliedAt?: string }>> {
  const applied = await getAppliedMigrations(service, namespace)
  const rows = await service.query(
    namespace,
    "SELECT json_extract(data, '$.name') as name, json_extract(data, '$.appliedAt') as appliedAt FROM data WHERE type = '_migrations' AND data IS NOT NULL",
  )
  const appliedMap = new Map(rows.map((r) => [r.name as string, r.appliedAt as string]))

  return migrations.map((m) => ({
    name: m.name,
    applied: applied.has(m.name),
    appliedAt: appliedMap.get(m.name),
  }))
}
