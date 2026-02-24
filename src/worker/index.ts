/**
 * dotdo-payload worker entry point.
 *
 * Exports:
 * - PayloadDatabaseDO (Durable Object) — entity storage with CDC
 * - PayloadDatabaseRPC (WorkerEntrypoint) — RPC interface for service bindings
 * - Default fetch handler (health check)
 */

import { buildCdcEvent, actorFromRequest } from '../utilities/cdc.js'

export { PayloadDatabaseDO } from './do.js'
export { PayloadDatabaseRPC } from './rpc.js'
// Alias for seamless swap — apps/platform binds to entrypoint "PayloadRPC"
export { PayloadDatabaseRPC as PayloadRPC } from './rpc.js'

interface Env {
  CLICKHOUSE_URL?: string
  CLICKHOUSE_USERNAME?: string
  CLICKHOUSE_PASSWORD?: string
  EVENTS_PIPELINE?: { send(messages: unknown[]): Promise<void> }
  DIAGNOSTIC_TOKEN?: string
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health' || url.pathname === '/') {
      return Response.json({
        ok: true,
        service: 'dotdo-payload',
        version: '0.1.0',
        clickhouse: !!env.CLICKHOUSE_URL,
        pipeline: !!env.EVENTS_PIPELINE,
      })
    }

    // All /test/* diagnostic routes require DIAGNOSTIC_TOKEN
    if (url.pathname.startsWith('/test/')) {
      const token = env.DIAGNOSTIC_TOKEN
      if (!token) return Response.json({ error: 'Diagnostics disabled' }, { status: 403 })
      const provided = url.searchParams.get('token') ?? request.headers.get('x-diagnostic-token')
      if (provided !== token) return Response.json({ error: 'Unauthorized' }, { status: 401 })
    }

    // Diagnostic: test direct ClickHouse insert
    if (url.pathname === '/test/ch-ping') {
      const chUrl = env.CLICKHOUSE_URL
      if (!chUrl) return Response.json({ ok: false, error: 'CLICKHOUSE_URL not set' })

      try {
        const pingUrl = new URL(chUrl)
        pingUrl.searchParams.set('query', 'SELECT 1 FORMAT JSON')

        const resp = await fetch(pingUrl.toString(), {
          method: 'POST',
          headers: {
            'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
            'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
          },
        })

        if (!resp.ok) {
          const text = await resp.text()
          return Response.json({ ok: false, error: `ClickHouse ${resp.status}: ${text.slice(0, 300)}` })
        }

        return Response.json({ ok: true, clickhouse: 'connected' })
      } catch (err) {
        return Response.json({ ok: false, error: String(err) })
      }
    }

    // Diagnostic: test insert with nested object data (mimics CDC event)
    if (url.pathname === '/test/ch-insert-object') {
      const chUrl = env.CLICKHOUSE_URL
      if (!chUrl) return Response.json({ ok: false, error: 'CLICKHOUSE_URL not set' })

      const start = Date.now()
      const testEvent = {
        id: `test_obj_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        ns: 'test',
        ts: new Date().toISOString(),
        type: 'cdc',
        event: 'test.object-data',
        source: 'diagnostic',
        url: 'https://headless.ly/~test',
        actor: actorFromRequest(request),
        data: { type: 'TestEntity', id: `test_obj_${Date.now()}`, name: 'Alice', email: 'alice@test.com', nested: { key: 'value' } },
        meta: {},
      }

      try {
        const insertUrl = new URL(chUrl)
        insertUrl.searchParams.set('database', 'platform')
        insertUrl.searchParams.set('query', 'INSERT INTO events FORMAT JSONEachRow')
        insertUrl.searchParams.set('date_time_input_format', 'best_effort')

        const resp = await fetch(insertUrl.toString(), {
          method: 'POST',
          body: JSON.stringify(testEvent),
          headers: {
            'Content-Type': 'text/plain',
            'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
            'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
          },
        })

        const elapsed = Date.now() - start
        if (!resp.ok) {
          const text = await resp.text()
          return Response.json({ ok: false, elapsed_ms: elapsed, error: `ClickHouse ${resp.status}: ${text.slice(0, 500)}`, body_sent: JSON.stringify(testEvent) })
        }
        return Response.json({ ok: true, elapsed_ms: elapsed, event_id: testEvent.id })
      } catch (err) {
        return Response.json({ ok: false, elapsed_ms: Date.now() - start, error: String(err) })
      }
    }

    // Diagnostic: test direct insert + measure latency
    if (url.pathname === '/test/ch-insert') {
      const chUrl = env.CLICKHOUSE_URL
      if (!chUrl) return Response.json({ ok: false, error: 'CLICKHOUSE_URL not set' })

      const start = Date.now()
      const testEvent = {
        id: `test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        ns: 'test',
        ts: new Date().toISOString(),
        type: 'test',
        event: 'test.ch-insert',
        source: 'diagnostic',
        data: '{}',
        meta: '{}',
      }

      try {
        const insertUrl = new URL(chUrl)
        insertUrl.searchParams.set('database', 'platform')
        insertUrl.searchParams.set('query', 'INSERT INTO events FORMAT JSONEachRow')
        insertUrl.searchParams.set('date_time_input_format', 'best_effort')

        const resp = await fetch(insertUrl.toString(), {
          method: 'POST',
          body: JSON.stringify(testEvent),
          headers: {
            'Content-Type': 'text/plain',
            'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
            'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
          },
        })

        const elapsed = Date.now() - start

        if (!resp.ok) {
          const text = await resp.text()
          return Response.json({ ok: false, elapsed_ms: elapsed, error: `ClickHouse ${resp.status}: ${text.slice(0, 500)}` })
        }

        return Response.json({ ok: true, elapsed_ms: elapsed, event_id: testEvent.id })
      } catch (err) {
        return Response.json({ ok: false, elapsed_ms: Date.now() - start, error: String(err) })
      }
    }

    // Diagnostic: batch test — create N entities with dual-write, measure latency variance
    if (url.pathname === '/test/batch') {
      const count = Math.min(parseInt(url.searchParams.get('n') || '10'), 100)
      const parallel = url.searchParams.get('parallel') === 'true'
      const testNs = 'e2e-test'
      const batchId = `batch_${Date.now()}`
      const results: Array<{ i: number; entity_id: string; create_ms: number; pipeline_ms: number; ch_ms: number; total_ms: number; error?: string }> = []

      const runOne = async (i: number) => {
        const start = Date.now()
        try {
          const doId = (env as any).PAYLOAD_DO.idFromName(testNs)
          const stub = (env as any).PAYLOAD_DO.get(doId)

          const createRes = await stub.fetch(`https://do/entity/TestEntity`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: `${batchId}-${i}`, batch: batchId }),
          })
          const entity = (await createRes.json()) as Record<string, unknown>
          const createMs = Date.now() - start

          const cdcEvent = buildCdcEvent({
            id: entity.$id as string,
            ns: 'https://headless.ly/~e2e-test',
            event: 'test.batch-created',
            entityType: 'TestEntity',
            entityData: { name: `${batchId}-${i}`, batch: batchId, i },
            source: 'diagnostic',
            actor: actorFromRequest(request),
          })

          // Dual-write in parallel
          let pipeMs = 0, chMs = 0
          const dualStart = Date.now()

          const [pipeResult, chResult] = await Promise.allSettled([
            (async () => {
              if (!env.EVENTS_PIPELINE) return
              const s = Date.now()
              await env.EVENTS_PIPELINE.send([cdcEvent])
              pipeMs = Date.now() - s
            })(),
            (async () => {
              if (!env.CLICKHOUSE_URL) return
              const s = Date.now()
              const insertUrl = new URL(env.CLICKHOUSE_URL)
              insertUrl.searchParams.set('query', 'INSERT INTO events FORMAT JSONEachRow')
              insertUrl.searchParams.set('date_time_input_format', 'best_effort')
              const resp = await fetch(insertUrl.toString(), {
                method: 'POST',
                body: JSON.stringify(cdcEvent),
                headers: {
                  'Content-Type': 'text/plain',
                  'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
                  'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
                },
              })
              chMs = Date.now() - s
              if (!resp.ok) throw new Error(`CH ${resp.status}: ${(await resp.text()).slice(0, 200)}`)
            })(),
          ])

          const pipeErr = pipeResult.status === 'rejected' ? String(pipeResult.reason) : undefined
          const chErr = chResult.status === 'rejected' ? String(chResult.reason) : undefined

          results.push({
            i,
            entity_id: entity.$id as string,
            create_ms: createMs,
            pipeline_ms: pipeMs,
            ch_ms: chMs,
            total_ms: Date.now() - start,
            ...(pipeErr || chErr ? { error: [pipeErr, chErr].filter(Boolean).join('; ') } : {}),
          })
        } catch (err) {
          results.push({ i, entity_id: '', create_ms: 0, pipeline_ms: 0, ch_ms: 0, total_ms: Date.now() - start, error: String(err) })
        }
      }

      const batchStart = Date.now()
      if (parallel) {
        await Promise.all(Array.from({ length: count }, (_, i) => runOne(i)))
      } else {
        for (let i = 0; i < count; i++) await runOne(i)
      }
      const batchMs = Date.now() - batchStart

      const createTimes = results.filter(r => !r.error).map(r => r.create_ms)
      const chTimes = results.filter(r => !r.error).map(r => r.ch_ms)
      const totalTimes = results.filter(r => !r.error).map(r => r.total_ms)
      const errors = results.filter(r => r.error).length

      const stats = (arr: number[]) => {
        if (arr.length === 0) return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 }
        const sorted = [...arr].sort((a, b) => a - b)
        return {
          min: sorted[0],
          max: sorted[sorted.length - 1],
          avg: Math.round(sorted.reduce((a, b) => a + b, 0) / sorted.length),
          p50: sorted[Math.floor(sorted.length * 0.5)],
          p95: sorted[Math.floor(sorted.length * 0.95)],
          p99: sorted[Math.floor(sorted.length * 0.99)],
        }
      }

      return Response.json({
        batch_id: batchId,
        count,
        parallel,
        batch_ms: batchMs,
        errors,
        create: stats(createTimes),
        clickhouse: stats(chTimes),
        total: stats(totalTimes),
        results,
      })
    }

    // Diagnostic: test full RPC path (DO create + sendEvent dual-write)
    if (url.pathname === '/test/rpc-create') {
      const start = Date.now()
      const testNs = 'e2e-test'
      const testType = 'TestEntity'
      const testData = { name: `test-${Date.now()}`, source: 'diagnostic' }

      try {
        // Get DO stub and create entity (same path as RPC.create)
        const doId = (env as any).PAYLOAD_DO.idFromName(testNs)
        const stub = (env as any).PAYLOAD_DO.get(doId)

        const createRes = await stub.fetch(`https://do/entity/${testType}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(testData),
        })

        const entity = (await createRes.json()) as Record<string, unknown>
        const createMs = Date.now() - start

        // Now send CDC event via dual-write (same as RPC.sendEvent)
        const eventStart = Date.now()
        const cdcEvent = buildCdcEvent({
          id: entity.$id as string,
          ns: 'https://headless.ly/~e2e-test',
          event: 'test.created',
          entityType: testType,
          entityData: testData,
          source: 'diagnostic',
          actor: actorFromRequest(request),
        })

        const promises: Array<{ path: string; ok: boolean; ms: number; error?: string }> = []

        // Pipeline
        if (env.EVENTS_PIPELINE) {
          const pipeStart = Date.now()
          try {
            await env.EVENTS_PIPELINE.send([cdcEvent])
            promises.push({ path: 'pipeline', ok: true, ms: Date.now() - pipeStart })
          } catch (err) {
            promises.push({ path: 'pipeline', ok: false, ms: Date.now() - pipeStart, error: String(err) })
          }
        }

        // Direct ClickHouse insert
        if (env.CLICKHOUSE_URL) {
          const chStart = Date.now()
          try {
            const insertUrl = new URL(env.CLICKHOUSE_URL)
            insertUrl.searchParams.set('query', 'INSERT INTO events FORMAT JSONEachRow')
            insertUrl.searchParams.set('date_time_input_format', 'best_effort')

            const resp = await fetch(insertUrl.toString(), {
              method: 'POST',
              body: JSON.stringify(cdcEvent),
              headers: {
                'Content-Type': 'text/plain',
                'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
                'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
              },
            })

            if (!resp.ok) {
              const text = await resp.text()
              promises.push({ path: 'clickhouse', ok: false, ms: Date.now() - chStart, error: text.slice(0, 300) })
            } else {
              promises.push({ path: 'clickhouse', ok: true, ms: Date.now() - chStart })
            }
          } catch (err) {
            promises.push({ path: 'clickhouse', ok: false, ms: Date.now() - chStart, error: String(err) })
          }
        }

        return Response.json({
          ok: true,
          entity_id: entity.$id,
          create_ms: createMs,
          event_ms: Date.now() - eventStart,
          total_ms: Date.now() - start,
          paths: promises,
        })
      } catch (err) {
        return Response.json({ ok: false, total_ms: Date.now() - start, error: String(err) })
      }
    }

    // Diagnostic: test version event round-trip (insert + read-back from ClickHouse)
    if (url.pathname === '/test/version-event') {
      const chUrl = env.CLICKHOUSE_URL
      if (!chUrl) return Response.json({ ok: false, error: 'CLICKHOUSE_URL not set' })

      const start = Date.now()
      const testId = `vtest_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const testNs = 'https://headless.ly/~e2e-test'
      const testType = 'testcollection'

      // Build a version event (same shape as operations/versions.ts createVersion)
      const versionEvent = buildCdcEvent({
        id: testId,
        ns: testNs,
        event: `${testType}.versioned`,
        entityType: testType,
        entityData: {
          _version: 1,
          title: `Test Version ${testId}`,
          status: 'draft',
        },
        source: 'diagnostic',
      })

      try {
        // 1. Insert the version event into ClickHouse
        const insertUrl = new URL(chUrl)
        insertUrl.searchParams.set('database', 'platform')
        insertUrl.searchParams.set('query', 'INSERT INTO events FORMAT JSONEachRow')
        insertUrl.searchParams.set('date_time_input_format', 'best_effort')

        const insertResp = await fetch(insertUrl.toString(), {
          method: 'POST',
          body: JSON.stringify(versionEvent),
          headers: {
            'Content-Type': 'text/plain',
            'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
            'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
          },
        })

        const insertMs = Date.now() - start
        if (!insertResp.ok) {
          const text = await insertResp.text()
          return Response.json({ ok: false, phase: 'insert', elapsed_ms: insertMs, error: text.slice(0, 500) })
        }

        // 2. Read it back from ClickHouse (same query pattern as ClickHouseVersionStore)
        const readStart = Date.now()
        const readUrl = new URL(chUrl)
        readUrl.searchParams.set('database', 'platform')
        readUrl.searchParams.set('default_format', 'JSON')

        const readSql = `SELECT id, ts, event, data FROM events WHERE ns = '${testNs}' AND event = '${testType}.versioned' AND id = '${versionEvent.id}' LIMIT 1`
        readUrl.searchParams.set('query', readSql)

        const readResp = await fetch(readUrl.toString(), {
          method: 'GET',
          headers: {
            'X-ClickHouse-User': env.CLICKHOUSE_USERNAME || 'default',
            'X-ClickHouse-Key': env.CLICKHOUSE_PASSWORD || '',
          },
        })

        const readMs = Date.now() - readStart
        if (!readResp.ok) {
          const text = await readResp.text()
          return Response.json({ ok: false, phase: 'read', insert_ms: insertMs, read_ms: readMs, error: text.slice(0, 500) })
        }

        const readResult = await readResp.json() as { data?: Record<string, unknown>[] }
        const found = (readResult.data?.length ?? 0) > 0

        return Response.json({
          ok: true,
          found,
          event_id: versionEvent.id,
          entity_id: testId,
          event_name: `${testType}.versioned`,
          insert_ms: insertMs,
          read_ms: readMs,
          total_ms: Date.now() - start,
          read_rows: readResult.data?.length ?? 0,
          read_data: readResult.data?.[0] ?? null,
        })
      } catch (err) {
        return Response.json({ ok: false, total_ms: Date.now() - start, error: String(err) })
      }
    }

    return Response.json({ error: 'Not found' }, { status: 404 })
  },
}
