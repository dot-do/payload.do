import { describe, it, expect } from 'vitest'
import { entityToDocument, documentToEntityData, slugToType, extractTitle } from '../utilities/transforms'

describe('entityToDocument', () => {
  it('converts entity meta-fields to Payload document fields', () => {
    const entity = {
      $id: 'contact_abc123',
      $type: 'Contact',
      $createdAt: '2026-01-15T10:00:00.000Z',
      $updatedAt: '2026-01-15T12:00:00.000Z',
      name: 'Alice',
      email: 'alice@example.com',
    }
    const doc = entityToDocument(entity)
    expect(doc).toEqual({
      id: 'contact_abc123',
      name: 'Alice',
      email: 'alice@example.com',
      createdAt: '2026-01-15T10:00:00.000Z',
      updatedAt: '2026-01-15T12:00:00.000Z',
    })
  })

  it('strips all $ meta-fields', () => {
    const entity = {
      $id: 'thing_xyz',
      $type: 'Thing',
      $createdAt: '2026-01-01T00:00:00Z',
      $updatedAt: '2026-01-01T00:00:00Z',
      $version: 1,
      $createdBy: 'user_1',
      $updatedBy: 'user_1',
      $deletedAt: null,
      title: 'Test',
    }
    const doc = entityToDocument(entity)
    expect(doc.id).toBe('thing_xyz')
    expect(doc.title).toBe('Test')
    expect(doc).not.toHaveProperty('$id')
    expect(doc).not.toHaveProperty('$type')
    expect(doc).not.toHaveProperty('$version')
    expect(doc).not.toHaveProperty('$createdBy')
  })

  it('applies migrations when noun context is provided', () => {
    const entity = {
      $id: 'contact_abc123',
      $type: 'Contact',
      $createdAt: '2026-01-15T10:00:00.000Z',
      $updatedAt: '2026-01-15T12:00:00.000Z',
      company: 'Acme',
      _schemaVersion: 1,
      _schemaHash: 'old',
    }
    const noun = {
      schemaHash: 'new12345',
      schemaVersion: 2,
      migrations: [{ version: 2, name: 'rename-company', ops: [{ op: 'renameField', from: 'company', to: 'organization' }] }],
    }
    const doc = entityToDocument(entity, noun)
    expect(doc.organization).toBe('Acme')
    expect(doc.company).toBeUndefined()
    expect(doc._schemaVersion).toBe(2)
    expect(doc._schemaHash).toBe('new12345')
  })

  it('skips migration when schema hash matches', () => {
    const entity = {
      $id: 'contact_abc123',
      $type: 'Contact',
      $createdAt: '2026-01-15T10:00:00.000Z',
      $updatedAt: '2026-01-15T12:00:00.000Z',
      name: 'Alice',
      _schemaVersion: 2,
      _schemaHash: 'current1',
    }
    const noun = {
      schemaHash: 'current1',
      schemaVersion: 2,
      migrations: [{ version: 2, name: 'add-status', ops: [{ op: 'addField', path: 'status', default: 'active' }] }],
    }
    const doc = entityToDocument(entity, noun)
    expect(doc.status).toBeUndefined()
  })

  it('works without noun context (backward compat)', () => {
    const entity = {
      $id: 'contact_abc123',
      $type: 'Contact',
      $createdAt: '2026-01-15T10:00:00.000Z',
      $updatedAt: '2026-01-15T12:00:00.000Z',
      name: 'Alice',
    }
    const doc = entityToDocument(entity)
    expect(doc.name).toBe('Alice')
    expect(doc.id).toBe('contact_abc123')
  })
})

describe('documentToEntityData', () => {
  it('strips Payload meta-fields from document data', () => {
    const data = {
      id: 'contact_abc123',
      createdAt: '2026-01-15T10:00:00.000Z',
      updatedAt: '2026-01-15T12:00:00.000Z',
      password: 'secret',
      'confirm-password': 'secret',
      name: 'Alice',
      email: 'alice@example.com',
    }
    const entityData = documentToEntityData(data)
    expect(entityData).toEqual({
      name: 'Alice',
      email: 'alice@example.com',
    })
  })
})

describe('slugToType', () => {
  it('converts collection slugs to PascalCase entity types', () => {
    expect(slugToType('contacts')).toBe('Contact')
    expect(slugToType('deals')).toBe('Deal')
    expect(slugToType('users')).toBe('User')
    expect(slugToType('feature-flags')).toBe('FeatureFlag')
    expect(slugToType('api-keys')).toBe('ApiKey')
    expect(slugToType('things')).toBe('Thing')
    expect(slugToType('nouns')).toBe('Noun')
  })

  it('handles already-singular slugs without breaking', () => {
    expect(slugToType('media')).toBe('Media')
  })

  it('does not strip trailing s from non-plural words', () => {
    // 'status' should NOT become 'Statu'
    expect(slugToType('status')).toBe('Status')
    // Unknown slugs use PascalCase fallback without stripping
    expect(slugToType('address')).toBe('Address')
  })
})

describe('extractTitle', () => {
  it('extracts title field', () => {
    expect(extractTitle({ title: 'My Title', name: 'My Name' })).toBe('My Title')
  })

  it('falls back to name field', () => {
    expect(extractTitle({ name: 'My Name' })).toBe('My Name')
  })

  it('returns null when neither exists', () => {
    expect(extractTitle({ email: 'test@test.com' })).toBeNull()
  })
})
