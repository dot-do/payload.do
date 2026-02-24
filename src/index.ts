import type { DatabaseAdapterObj, Payload } from 'payload'
import { createDatabaseAdapter } from 'payload'
import type { DoPayloadAdapter, DoPayloadArgs } from './types.js'
import { ClickHouseVersionStore } from './versions.js'
import { runPendingMigrations } from './migrations.js'
// Side-effect imports register migrations
import './migrations/001-tenant-to-organization.js'
import './migrations/002-organization-back-to-tenant.js'
import {
  count,
  create,
  createGlobal,
  createVersion,
  countGlobalVersions,
  countVersions,
  deleteMany,
  deleteOne,
  deleteVersions,
  find,
  findGlobal,
  findGlobalVersions,
  findOne,
  findVersions,
  createGlobalVersion,
  updateGlobal,
  updateGlobalVersion,
  updateMany,
  updateOne,
  updateVersion,
  upsert,
  findDistinct,
  queryDrafts,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
} from './operations/index.js'

export type { DoPayloadAdapter, DoPayloadArgs, PayloadDatabaseService, EntityRow } from './types.js'
export type { VersionStore, VersionDoc } from './versions.js'
export { ClickHouseVersionStore, NullVersionStore } from './versions.js'
export { doPayloadCollections } from './collections/index.js'

// Schema evolution
export { computeSchemaHash } from './utilities/schema-hash.js'
export { applyMigration, applyMigrations, registerTransform, type MigrationDef, type MigrationOp } from './utilities/migrate.js'
export { type NounContext } from './utilities/transforms.js'
export { resolveNounId, resolveNounSchema, resolveNounContext, invalidateNounCache } from './utilities/noun-cache.js'

export function doPayload(args: DoPayloadArgs): DatabaseAdapterObj {
  return {
    name: 'do',
    defaultIDType: 'text',
    init({ payload }: { payload: Payload }) {
      if (!args.service) throw new Error('@dotdo/payload: service binding not found')

      return createDatabaseAdapter<DoPayloadAdapter>({
        name: 'do',
        defaultIDType: 'text',
        packageName: '@dotdo/payload',
        namespace: args.namespace ?? 'default',
        tenantNum: args.tenantNum ?? 0,
        context: args.context ?? 'https://headless.ly/~default',
        _service: args.service,
        versionStore: new ClickHouseVersionStore(args.service),
        payload,

        connect: async function (this: DoPayloadAdapter) {
          await runPendingMigrations(this._service, this.namespace)
        },
        destroy: async () => {},
        init: async () => {},

        count,
        create,
        find,
        findOne,
        findDistinct,
        updateOne,
        updateMany,
        deleteOne,
        deleteMany,
        upsert,

        createGlobal,
        findGlobal,
        updateGlobal,

        createVersion,
        findVersions,
        updateVersion,
        deleteVersions,
        countVersions,
        createGlobalVersion,
        findGlobalVersions,
        updateGlobalVersion,
        countGlobalVersions,

        queryDrafts,

        beginTransaction,
        commitTransaction,
        rollbackTransaction,

        createMigration: async ({ migrationName }) => {
          console.log(`[do-adapter] createMigration called: ${migrationName ?? 'unnamed'}`)
          console.log('[do-adapter] Schema migrations are not needed (entities table with JSON data)')
        },
        migrate: async () => {
          console.log('[do-adapter] migrate: no schema migrations needed')
        },
        migrateFresh: async () => {
          console.log('[do-adapter] migrateFresh not supported — DatabaseDO uses immutable event history')
        },
        migrateRefresh: async () => {
          console.log('[do-adapter] migrateRefresh not supported — use migrate instead')
        },
        migrateReset: async () => {
          console.log('[do-adapter] migrateReset not supported — DatabaseDO uses immutable event history')
        },
        migrateStatus: async () => {
          console.log('[do-adapter] No migrations to track — entities table handles schema evolution via JSON')
        },
        migrationDir: '',
      })
    },
  }
}
