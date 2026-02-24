import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { defineConfig } from 'vitest/config'

/**
 * E2E tests against the live deployed dotdo-payload worker.
 *
 * Hits real endpoints, verifies data flows through Pipelines → ClickHouse.
 * Loads ClickHouse credentials from .do/db/.env (CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD).
 *
 * Run: pnpm test:e2e
 */

// Parse .do/db/.env for ClickHouse credentials
function loadDbEnv(): Record<string, string> {
  try {
    const envPath = resolve(import.meta.dirname, '../db/.env')
    const content = readFileSync(envPath, 'utf-8')
    const env: Record<string, string> = {}
    for (const line of content.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed || trimmed.startsWith('#')) continue
      const eqIdx = trimmed.indexOf('=')
      if (eqIdx === -1) continue
      const key = trimmed.slice(0, eqIdx).trim()
      let value = trimmed.slice(eqIdx + 1).trim()
      // Strip surrounding quotes
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1)
      }
      env[key] = value
    }
    return env
  } catch {
    return {}
  }
}

const dbEnv = loadDbEnv()

export default defineConfig({
  test: {
    include: ['test-e2e/**/*.e2e.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    pool: 'forks',
    environment: 'node',
    reporters: ['verbose'],
    retry: 1,
    sequence: {
      concurrent: false,
    },
    env: {
      CLICKHOUSE_URL: dbEnv.CLICKHOUSE_URL ?? '',
      CLICKHOUSE_USER: dbEnv.CLICKHOUSE_USER ?? '',
      CLICKHOUSE_PASSWORD: dbEnv.CLICKHOUSE_PASSWORD ?? '',
      DIAGNOSTIC_TOKEN: process.env.DIAGNOSTIC_TOKEN ?? dbEnv.DIAGNOSTIC_TOKEN ?? '',
      ADMIN_PASSWORD: process.env.ADMIN_PASSWORD ?? dbEnv.ADMIN_PASSWORD ?? '',
    },
  },
})
