/**
 * Migration 002: Revert migration 001 — rename `organization` back to `tenant`.
 */

import type { DataMigration } from '../migrations.js'
import { registerMigration } from '../migrations.js'

const MIGRATION_NAME = '20260219_002_organization_back_to_tenant'

const SKIP_TYPES = new Set(['users', 'organizations', '_migrations', 'payload-preferences', 'payload-migrations', 'media', 'search'])

const migration: DataMigration = {
  name: MIGRATION_NAME,

  async up(service, namespace) {
    const types = await service.query(
      namespace,
      "SELECT DISTINCT type FROM data WHERE data IS NOT NULL AND json_extract(data, '$.organization') IS NOT NULL",
    )

    for (const { type } of types) {
      if (SKIP_TYPES.has(type as string)) continue

      const result = await service.run(
        namespace,
        `UPDATE data SET data = json_set(
          json_remove(data, '$.organization'),
          '$.tenant', json_extract(data, '$.organization')
        )
        WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.organization') IS NOT NULL`,
        type,
      )

      console.log(`[migration:002] Renamed organization→tenant in ${result.changes} ${type} rows`)
    }
  },

  async down(service, namespace) {
    const types = await service.query(
      namespace,
      "SELECT DISTINCT type FROM data WHERE data IS NOT NULL AND json_extract(data, '$.tenant') IS NOT NULL",
    )

    for (const { type } of types) {
      if (SKIP_TYPES.has(type as string)) continue

      const result = await service.run(
        namespace,
        `UPDATE data SET data = json_set(
          json_remove(data, '$.tenant'),
          '$.organization', json_extract(data, '$.tenant')
        )
        WHERE type = ? AND data IS NOT NULL AND json_extract(data, '$.tenant') IS NOT NULL`,
        type,
      )

      console.log(`[migration:002:down] Renamed tenant→organization in ${result.changes} ${type} rows`)
    }
  },
}

registerMigration(migration)

export default migration
