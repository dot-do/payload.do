import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersConfig({
  ssr: {
    noExternal: [/@dotdo\/db/],
  },
  test: {
    include: ['test-integration/**/*.integration.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,

    // Single worker to avoid SQLite WAL issues
    maxConcurrency: 1,
    maxWorkers: 1,
    minWorkers: 1,
    fileParallelism: false,

    poolOptions: {
      workers: {
        main: './test-integration/worker-entry.ts',

        singleWorker: true,
        isolatedStorage: false,

        miniflare: {
          compatibilityDate: '2025-08-15',
          compatibilityFlags: ['nodejs_compat'],
          durableObjectsPersist: false,

          durableObjects: {
            PAYLOAD_DO: { className: 'PayloadDatabaseDO', useSQLite: true },
          },

          serviceBindings: {
            // Stub Pipeline — records events for inspection
            EVENTS_PIPELINE: 'pipeline-test-worker',
          },
          workers: [
            {
              name: 'pipeline-test-worker',
              modules: true,
              scriptPath: './test-integration/auxiliary-workers/pipeline-worker.js',
            },
          ],
        },
      },
    },
  },
})
