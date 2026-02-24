import { describe, it, expect } from 'vitest'
import { computeSchemaHash } from '../utilities/schema-hash'

describe('computeSchemaHash', () => {
  it('returns an 8-char hex string', () => {
    const hash = computeSchemaHash({ name: { type: 'text', required: true } })
    expect(hash).toMatch(/^[0-9a-f]{8}$/)
  })

  it('returns same hash for same schema regardless of key order', () => {
    const a = computeSchemaHash({ name: { type: 'text' }, email: { type: 'text' } })
    const b = computeSchemaHash({ email: { type: 'text' }, name: { type: 'text' } })
    expect(a).toBe(b)
  })

  it('returns different hash for different schemas', () => {
    const a = computeSchemaHash({ name: { type: 'text' } })
    const b = computeSchemaHash({ name: { type: 'number' } })
    expect(a).not.toBe(b)
  })

  it('handles empty schema', () => {
    const hash = computeSchemaHash({})
    expect(hash).toMatch(/^[0-9a-f]{8}$/)
  })

  it('handles nested objects deterministically', () => {
    const schema = { address: { type: 'group', fields: { city: { type: 'text' }, zip: { type: 'text' } } } }
    const a = computeSchemaHash(schema)
    const b = computeSchemaHash(schema)
    expect(a).toBe(b)
  })
})
