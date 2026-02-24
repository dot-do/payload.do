/**
 * Minimal worker entry point for integration tests.
 *
 * Exports PayloadDatabaseDO so vitest-pool-workers can instantiate
 * real DOs with real SQLite in miniflare.
 */

export { PayloadDatabaseDO } from '../src/worker/do'

export default {
  async fetch() {
    return new Response('integration test worker')
  },
}
