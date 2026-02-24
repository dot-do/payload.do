/**
 * Integration tests for PayloadDatabaseDO.
 *
 * These tests instantiate the REAL PayloadDatabaseDO via vitest-pool-workers
 * with real SQLite (in-memory via miniflare). No mocks for DO behavior.
 *
 * Tests the DO's HTTP routes (/entity/:type, /entity/:type/:id, /query, /run)
 * and its RPC methods (find(), countEntities(), getEntity()).
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getTestDO, createEntity, getEntity, updateEntity, deleteEntity, querySQL, runSQL, findViaRPC, countViaRPC, sleep } from './helpers'

describe('PayloadDatabaseDO — Entity CRUD (real DO)', () => {
  let doProxy: ReturnType<typeof getTestDO>

  beforeEach(() => {
    doProxy = getTestDO()
  })

  // ===========================================================================
  // CREATE via HTTP
  // ===========================================================================

  describe('POST /entity/:type', () => {
    it('creates an entity and returns it with $meta fields', async () => {
      const { status, body } = await createEntity(doProxy, 'Contact', {
        name: 'Alice',
        email: 'alice@acme.co',
      })

      expect(status).toBe(201)
      expect(body.$type).toBe('Contact')
      expect(body.$id).toBeDefined()
      expect(typeof body.$id).toBe('string')
      expect(body.$createdAt).toBeDefined()
      expect(body.$updatedAt).toBeDefined()
      expect(body.name).toBe('Alice')
      expect(body.email).toBe('alice@acme.co')
    })

    it('generates $id in {type}_{sqid} format', async () => {
      const { body } = await createEntity(doProxy, 'Contact', { name: 'Bob' })
      const id = body.$id as string
      expect(id).toMatch(/^contact_/)
    })

    it('accepts a custom $id', async () => {
      const { status, body } = await createEntity(doProxy, 'Contact', {
        $id: 'contact_custom123',
        name: 'Custom',
      })
      expect(status).toBe(201)
      expect(body.$id).toBe('contact_custom123')
    })

    it('creates entities of different types independently', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Deal', { title: 'Big Deal' })

      const contacts = await findViaRPC(doProxy, 'Contact')
      const deals = await findViaRPC(doProxy, 'Deal')

      expect(contacts.total).toBe(1)
      expect(deals.total).toBe(1)
    })
  })

  // ===========================================================================
  // READ via HTTP
  // ===========================================================================

  describe('GET /entity/:type/:id', () => {
    it('retrieves a created entity', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string

      const { status, body } = await getEntity(doProxy, 'Contact', id)
      expect(status).toBe(200)
      expect(body.$id).toBe(id)
      expect(body.name).toBe('Alice')
    })

    it('returns 404 for non-existent entity', async () => {
      const { status } = await getEntity(doProxy, 'Contact', 'contact_nonexistent')
      expect(status).toBe(404)
    })
  })

  // ===========================================================================
  // UPDATE via HTTP
  // ===========================================================================

  describe('PUT /entity/:type/:id', () => {
    it('updates an entity and preserves existing fields', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', {
        name: 'Alice',
        email: 'alice@old.com',
        stage: 'Lead',
      })
      const id = created.$id as string

      const { status, body } = await updateEntity(doProxy, 'Contact', id, { email: 'alice@new.com' })

      expect(status).toBe(200)
      expect(body.email).toBe('alice@new.com')
      expect(body.name).toBe('Alice') // preserved
      expect(body.stage).toBe('Lead') // preserved
    })

    it('updates $updatedAt on every update', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string
      const originalUpdatedAt = created.$updatedAt as string

      await sleep(10)
      const { body } = await updateEntity(doProxy, 'Contact', id, { name: 'Alice Updated' })
      expect(body.$updatedAt).not.toBe(originalUpdatedAt)
    })

    it('returns 404 for non-existent entity', async () => {
      const { status } = await updateEntity(doProxy, 'Contact', 'contact_nonexistent', { name: 'Nope' })
      expect(status).toBe(404)
    })

    it('returns 404 for soft-deleted entity', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string
      await deleteEntity(doProxy, 'Contact', id)

      const { status } = await updateEntity(doProxy, 'Contact', id, { name: 'Ghost' })
      expect(status).toBe(404)
    })
  })

  // ===========================================================================
  // SOFT DELETE via HTTP
  // ===========================================================================

  describe('DELETE /entity/:type/:id', () => {
    it('soft-deletes an entity (GET returns 404 after)', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string

      const { status } = await deleteEntity(doProxy, 'Contact', id)
      expect(status).toBe(200)

      const { status: getStatus } = await getEntity(doProxy, 'Contact', id)
      expect(getStatus).toBe(404)
    })

    it('excluded from find results after soft-delete', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Keeper' })
      const { body: toDelete } = await createEntity(doProxy, 'Contact', { name: 'ToDelete' })
      await deleteEntity(doProxy, 'Contact', toDelete.$id as string)

      const result = await findViaRPC(doProxy, 'Contact')
      expect(result.total).toBe(1)
      expect((result.items[0] as any).name).toBe('Keeper')
    })
  })

  // ===========================================================================
  // RPC METHODS (DO's direct async methods)
  // ===========================================================================

  describe('RPC: find()', () => {
    it('returns all entities of a type', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })
      await createEntity(doProxy, 'Deal', { title: 'Deal 1' })

      const result = await findViaRPC(doProxy, 'Contact')
      expect(result.total).toBe(2)
      expect(result.items).toHaveLength(2)
    })

    it('supports limit and offset', async () => {
      for (let i = 0; i < 5; i++) {
        await createEntity(doProxy, 'Contact', { name: `Contact ${i}` })
      }

      const page1 = await findViaRPC(doProxy, 'Contact', undefined, { limit: 2, offset: 0 })
      expect(page1.items).toHaveLength(2)
      expect(page1.total).toBe(5)
      expect(page1.hasMore).toBe(true)

      const page3 = await findViaRPC(doProxy, 'Contact', undefined, { limit: 2, offset: 4 })
      expect(page3.items).toHaveLength(1)
      expect(page3.hasMore).toBe(false)
    })

    it('applies MongoDB-style filters', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice', stage: 'Lead' })
      await createEntity(doProxy, 'Contact', { name: 'Bob', stage: 'Customer' })
      await createEntity(doProxy, 'Contact', { name: 'Charlie', stage: 'Lead' })

      const result = await findViaRPC(doProxy, 'Contact', { stage: { $eq: 'Lead' } })
      expect(result.total).toBe(2)
    })

    it('supports $in filter', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice', stage: 'Lead' })
      await createEntity(doProxy, 'Contact', { name: 'Bob', stage: 'Customer' })
      await createEntity(doProxy, 'Contact', { name: 'Charlie', stage: 'Churned' })

      const result = await findViaRPC(doProxy, 'Contact', { stage: { $in: ['Lead', 'Customer'] } })
      expect(result.total).toBe(2)
    })

    it('supports sort', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Charlie' })
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })

      const result = await findViaRPC(doProxy, 'Contact', undefined, { sort: { name: 1 } })
      expect((result.items[0] as any).name).toBe('Alice')
      expect((result.items[1] as any).name).toBe('Bob')
      expect((result.items[2] as any).name).toBe('Charlie')
    })

    it('returns empty for nonexistent type', async () => {
      const result = await findViaRPC(doProxy, 'Nonexistent')
      expect(result.total).toBe(0)
      expect(result.items).toHaveLength(0)
    })
  })

  describe('RPC: countEntities()', () => {
    it('counts entities of a type', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })
      await createEntity(doProxy, 'Deal', { title: 'Deal 1' })

      expect(await countViaRPC(doProxy, 'Contact')).toBe(2)
      expect(await countViaRPC(doProxy, 'Deal')).toBe(1)
      expect(await countViaRPC(doProxy, 'Nonexistent')).toBe(0)
    })

    it('excludes soft-deleted entities', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })
      await deleteEntity(doProxy, 'Contact', created.$id as string)

      expect(await countViaRPC(doProxy, 'Contact')).toBe(1)
    })
  })

  // ===========================================================================
  // SQL ESCAPE HATCH (/query and /run)
  // ===========================================================================

  describe('SQL escape hatch', () => {
    it('POST /query returns rows from raw SQL', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice', email: 'alice@test.co' })
      await createEntity(doProxy, 'Contact', { name: 'Bob', email: 'bob@test.co' })

      const { status, body } = await querySQL(
        doProxy,
        "SELECT id, json_extract(data, '$.name') as name FROM entities WHERE type = ? AND deleted_at IS NULL ORDER BY json_extract(data, '$.name') ASC",
        ['Contact'],
      )

      expect(status).toBe(200)
      expect(body.rows).toHaveLength(2)
      expect((body.rows![0] as any).name).toBe('Alice')
      expect((body.rows![1] as any).name).toBe('Bob')
    })

    it('POST /query returns DISTINCT values', async () => {
      await createEntity(doProxy, 'Contact', { name: 'A', stage: 'Lead' })
      await createEntity(doProxy, 'Contact', { name: 'B', stage: 'Lead' })
      await createEntity(doProxy, 'Contact', { name: 'C', stage: 'Customer' })

      const { body } = await querySQL(
        doProxy,
        "SELECT DISTINCT json_extract(data, '$.stage') as value FROM entities WHERE type = ? AND deleted_at IS NULL",
        ['Contact'],
      )

      expect(body.rows).toHaveLength(2)
      const values = body.rows!.map((r: any) => r.value).sort()
      expect(values).toEqual(['Customer', 'Lead'])
    })

    it('POST /run executes SQL and returns changes', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })

      const { status, body } = await runSQL(doProxy, "UPDATE entities SET data = json_set(data, '$.tier', 'free') WHERE type = ?", ['Contact'])

      expect(status).toBe(200)
      expect(body.changes).toBe(2)
    })
  })

  // ===========================================================================
  // CDC EVENT LOGGING
  // ===========================================================================

  describe('CDC event logging', () => {
    it('logs create events to events table', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })

      const { body } = await querySQL(doProxy, "SELECT * FROM events WHERE operation = 'create' AND entity_type = 'Contact'")
      expect(body.rows!.length).toBeGreaterThanOrEqual(1)
      const event = body.rows![0] as any
      expect(event.entity_type).toBe('Contact')
      expect(event.operation).toBe('create')
    })

    it('logs update events to events table', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string
      await updateEntity(doProxy, 'Contact', id, { name: 'Alice Updated' })

      const { body } = await querySQL(doProxy, "SELECT * FROM events WHERE operation = 'update' AND entity_id = ?", [id])
      expect(body.rows!.length).toBeGreaterThanOrEqual(1)
    })

    it('logs delete events to events table', async () => {
      const { body: created } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = created.$id as string
      await deleteEntity(doProxy, 'Contact', id)

      const { body } = await querySQL(doProxy, "SELECT * FROM events WHERE operation = 'delete' AND entity_id = ?", [id])
      expect(body.rows!.length).toBeGreaterThanOrEqual(1)
    })
  })

  // ===========================================================================
  // GLOBALS (stored as _global_{slug} type)
  // ===========================================================================

  describe('globals via _global_ type prefix', () => {
    it('creates and retrieves a global entity', async () => {
      const { status, body } = await createEntity(doProxy, '_global_site-settings', {
        siteName: 'My Site',
        theme: 'dark',
      })
      expect(status).toBe(201)

      const id = body.$id as string
      const { body: fetched } = await getEntity(doProxy, '_global_site-settings', id)
      expect(fetched.siteName).toBe('My Site')
      expect(fetched.theme).toBe('dark')
    })

    it('globals are isolated from regular entities', async () => {
      await createEntity(doProxy, '_global_nav', { links: ['Home', 'About'] })
      await createEntity(doProxy, 'Contact', { name: 'Alice' })

      const globals = await findViaRPC(doProxy, '_global_nav')
      const contacts = await findViaRPC(doProxy, 'Contact')

      expect(globals.total).toBe(1)
      expect(contacts.total).toBe(1)
    })
  })

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty string fields', async () => {
      const { status, body } = await createEntity(doProxy, 'Contact', { name: '', email: '' })
      expect(status).toBe(201)
      expect(body.name).toBe('')
    })

    it('handles numeric and boolean fields', async () => {
      const { body } = await createEntity(doProxy, 'Deal', {
        title: 'Deal',
        value: 50000,
        probability: 0.75,
        active: true,
      })
      expect(body.value).toBe(50000)
      expect(body.probability).toBe(0.75)
      expect(body.active).toBe(true)
    })

    it('handles nested objects in data', async () => {
      const { body } = await createEntity(doProxy, 'Contact', {
        name: 'Alice',
        address: { city: 'SF', state: 'CA', zip: '94102' },
      })
      expect(body.address).toEqual({ city: 'SF', state: 'CA', zip: '94102' })
    })

    it('handles null fields in data', async () => {
      const { body } = await createEntity(doProxy, 'Contact', { name: 'Alice', phone: null })
      expect(body.phone).toBeNull()
    })

    it('handles array fields in data', async () => {
      const { body } = await createEntity(doProxy, 'Contact', {
        name: 'Alice',
        tags: ['vip', 'enterprise'],
      })
      expect(body.tags).toEqual(['vip', 'enterprise'])
    })
  })

  // ===========================================================================
  // NAMESPACE ISOLATION
  // ===========================================================================

  describe('namespace isolation', () => {
    it('different namespaces are completely isolated', async () => {
      const doA = getTestDO('tenant-a')
      const doB = getTestDO('tenant-b')

      await createEntity(doA, 'Contact', { name: 'Alice in A' })
      await createEntity(doB, 'Contact', { name: 'Bob in B' })

      const contactsA = await findViaRPC(doA, 'Contact')
      const contactsB = await findViaRPC(doB, 'Contact')

      expect(contactsA.total).toBe(1)
      expect((contactsA.items[0] as any).name).toBe('Alice in A')

      expect(contactsB.total).toBe(1)
      expect((contactsB.items[0] as any).name).toBe('Bob in B')
    })
  })

  // ===========================================================================
  // THINGS — UNIVERSAL DATA VIEW
  // ===========================================================================

  describe('Things — universal data view (all entities, no type filter)', () => {
    it('queries all entities across types via raw SQL', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Deal', { title: 'Big Deal', value: 50000 })
      await createEntity(doProxy, 'Lead', { name: 'New Lead', source: 'web' })

      // This is the SQL that thingsFind() generates
      const { body } = await querySQL(
        doProxy,
        'SELECT id, type, data, created_at, updated_at FROM entities WHERE deleted_at IS NULL ORDER BY updated_at DESC',
      )

      expect(body.rows!.length).toBe(3)
      const types = body.rows!.map((r: any) => r.type).sort()
      expect(types).toEqual(['Contact', 'Deal', 'Lead'])
    })

    it('filters by type across the universal view', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Contact', { name: 'Bob' })
      await createEntity(doProxy, 'Deal', { title: 'Deal 1' })

      const { body } = await querySQL(doProxy, "SELECT id, type, data FROM entities WHERE deleted_at IS NULL AND type = ?", ['Contact'])

      expect(body.rows!.length).toBe(2)
      expect(body.rows!.every((r: any) => r.type === 'Contact')).toBe(true)
    })

    it('filters by json_extract field across all types', async () => {
      await createEntity(doProxy, 'Contact', { name: 'Alice', stage: 'Lead' })
      await createEntity(doProxy, 'Contact', { name: 'Bob', stage: 'Customer' })
      await createEntity(doProxy, 'Deal', { title: 'D1', stage: 'Negotiation' })

      const { body } = await querySQL(
        doProxy,
        "SELECT id, type, data FROM entities WHERE deleted_at IS NULL AND json_extract(data, '$.stage') = ?",
        ['Lead'],
      )

      expect(body.rows!.length).toBe(1)
      const parsed = JSON.parse(body.rows![0].data as string)
      expect(parsed.name).toBe('Alice')
    })

    it('counts all entities regardless of type', async () => {
      await createEntity(doProxy, 'Contact', { name: 'A' })
      await createEntity(doProxy, 'Deal', { title: 'D' })
      await createEntity(doProxy, 'Lead', { name: 'L' })

      const { body } = await querySQL(doProxy, 'SELECT COUNT(*) as cnt FROM entities WHERE deleted_at IS NULL')

      expect(Number(body.rows![0].cnt)).toBe(3)
    })

    it('excludes soft-deleted entities from universal view', async () => {
      const { body: contact } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      await createEntity(doProxy, 'Deal', { title: 'Deal 1' })
      await deleteEntity(doProxy, 'Contact', contact.$id as string)

      const { body } = await querySQL(doProxy, 'SELECT COUNT(*) as cnt FROM entities WHERE deleted_at IS NULL')
      expect(Number(body.rows![0].cnt)).toBe(1)
    })

    it('supports pagination (LIMIT + OFFSET) in universal view', async () => {
      for (let i = 0; i < 5; i++) {
        await createEntity(doProxy, i % 2 === 0 ? 'Contact' : 'Deal', { name: `Entity ${i}` })
      }

      const { body: page1 } = await querySQL(
        doProxy,
        'SELECT id, type FROM entities WHERE deleted_at IS NULL ORDER BY created_at ASC LIMIT ? OFFSET ?',
        [2, 0],
      )
      expect(page1.rows!.length).toBe(2)

      const { body: page3 } = await querySQL(
        doProxy,
        'SELECT id, type FROM entities WHERE deleted_at IS NULL ORDER BY created_at ASC LIMIT ? OFFSET ?',
        [2, 4],
      )
      expect(page3.rows!.length).toBe(1)
    })

    it('resolves entity type by ID (for Things update/delete)', async () => {
      const { body: contact } = await createEntity(doProxy, 'Contact', { name: 'Alice' })
      const id = contact.$id as string

      const { body } = await querySQL(doProxy, 'SELECT type FROM entities WHERE id = ? AND deleted_at IS NULL', [id])

      expect(body.rows!.length).toBe(1)
      expect(body.rows![0].type).toBe('Contact')
    })

    it('supports IN clause for type filtering', async () => {
      await createEntity(doProxy, 'Contact', { name: 'A' })
      await createEntity(doProxy, 'Deal', { title: 'D' })
      await createEntity(doProxy, 'Lead', { name: 'L' })
      await createEntity(doProxy, 'Agent', { name: 'Bot' })

      const { body } = await querySQL(doProxy, 'SELECT id, type FROM entities WHERE deleted_at IS NULL AND type IN (?, ?)', ['Contact', 'Deal'])

      expect(body.rows!.length).toBe(2)
      const types = body.rows!.map((r: any) => r.type).sort()
      expect(types).toEqual(['Contact', 'Deal'])
    })
  })
})
