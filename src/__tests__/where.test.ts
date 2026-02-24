import { describe, it, expect } from 'vitest'
import { translateWhere } from '../queries/where'

describe('translateWhere', () => {
  it('returns undefined for empty where', () => {
    expect(translateWhere(undefined)).toBeUndefined()
    expect(translateWhere({})).toBeUndefined()
  })

  it('translates equals operator', () => {
    const result = translateWhere({ email: { equals: 'alice@example.com' } })
    expect(result).toEqual({ email: { $eq: 'alice@example.com' } })
  })

  it('translates not_equals operator', () => {
    const result = translateWhere({ status: { not_equals: 'archived' } })
    expect(result).toEqual({ status: { $ne: 'archived' } })
  })

  it('translates comparison operators', () => {
    const result = translateWhere({
      age: { greater_than: 18, less_than_equal: 65 },
    })
    expect(result).toEqual({ age: { $gt: 18, $lte: 65 } })
  })

  it('translates in/not_in operators', () => {
    const result = translateWhere({
      status: { in: ['active', 'pending'] },
    })
    expect(result).toEqual({ status: { $in: ['active', 'pending'] } })
  })

  it('translates exists operator', () => {
    const result = translateWhere({
      phone: { exists: true },
    })
    expect(result).toEqual({ phone: { $exists: true } })
  })

  it('translates like/contains to regex', () => {
    const result = translateWhere({
      name: { like: '%alice%' },
    })
    expect(result).toEqual({ name: { $regex: '.*alice.*' } })
  })

  it('maps id field to $id', () => {
    const result = translateWhere({ id: { equals: 'contact_abc123' } })
    expect(result).toEqual({ $id: { $eq: 'contact_abc123' } })
  })

  it('maps createdAt field to $createdAt', () => {
    const result = translateWhere({ createdAt: { greater_than: '2026-01-01' } })
    expect(result).toEqual({ $createdAt: { $gt: '2026-01-01' } })
  })

  it('handles AND combinator', () => {
    const result = translateWhere({
      and: [{ name: { equals: 'Alice' } }, { status: { equals: 'active' } }],
    })
    expect(result).toEqual({
      name: { $eq: 'Alice' },
      status: { $eq: 'active' },
    })
  })

  it('handles OR combinator with simple equals (flattens to $in)', () => {
    const result = translateWhere({
      or: [{ status: { equals: 'active' } }, { status: { equals: 'pending' } }],
    })
    expect(result).toEqual({ status: { $in: ['active', 'pending'] } })
  })

  it('handles direct equality values', () => {
    const result = translateWhere({ active: true, count: 42 })
    expect(result).toEqual({ active: true, count: 42 })
  })

  it('handles null values', () => {
    const result = translateWhere({ phone: null })
    expect(result).toEqual({ phone: null })
  })
})
