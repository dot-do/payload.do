import { describe, it, expect } from 'vitest'
import { buildWhereSql, buildOrderSql } from '../queries/sql'

describe('buildWhereSql', () => {
  it('returns base clause for no where', () => {
    const result = buildWhereSql('Contact')
    expect(result.sql).toBe('type = ? AND deleted_at IS NULL')
    expect(result.params).toEqual(['Contact'])
  })

  it('adds equals condition', () => {
    const result = buildWhereSql('Contact', { email: { equals: 'alice@example.com' } })
    expect(result.sql).toContain("json_extract(data, '$.email') = ?")
    expect(result.params).toContain('alice@example.com')
  })

  it('handles null equals', () => {
    const result = buildWhereSql('Contact', { phone: { equals: null } })
    expect(result.sql).toContain("json_extract(data, '$.phone') IS NULL")
  })

  it('handles in operator', () => {
    const result = buildWhereSql('Contact', { status: { in: ['active', 'pending'] } })
    expect(result.sql).toContain("json_extract(data, '$.status') IN (?, ?)")
    expect(result.params).toContain('active')
    expect(result.params).toContain('pending')
  })

  it('handles AND combinator', () => {
    const result = buildWhereSql('Contact', {
      and: [{ email: { equals: 'test@test.com' } }, { status: { equals: 'active' } }],
    })
    expect(result.sql).toContain('AND')
    expect(result.params).toContain('test@test.com')
    expect(result.params).toContain('active')
  })

  it('handles OR combinator', () => {
    const result = buildWhereSql('Contact', {
      or: [{ status: { equals: 'active' } }, { status: { equals: 'pending' } }],
    })
    expect(result.sql).toContain('OR')
  })

  it('maps id field to id column', () => {
    const result = buildWhereSql('Contact', { id: { equals: 'contact_abc' } })
    expect(result.sql).toContain('id = ?')
    expect(result.params).toContain('contact_abc')
  })

  it('maps createdAt field to created_at column', () => {
    const result = buildWhereSql('Contact', { createdAt: { greater_than: '2026-01-01' } })
    expect(result.sql).toContain('created_at > ?')
  })
})

describe('buildOrderSql', () => {
  it('defaults to created_at DESC', () => {
    expect(buildOrderSql()).toBe('ORDER BY created_at DESC')
    expect(buildOrderSql(undefined)).toBe('ORDER BY created_at DESC')
  })

  it('translates ascending sort', () => {
    expect(buildOrderSql('name')).toContain("json_extract(data, '$.name') ASC")
  })

  it('translates descending sort', () => {
    expect(buildOrderSql('-createdAt')).toContain('created_at DESC')
  })
})
