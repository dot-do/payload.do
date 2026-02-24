import { describe, it, expect } from 'vitest'
import { translateSort } from '../queries/sort'

describe('translateSort', () => {
  it('returns undefined for no sort', () => {
    expect(translateSort(undefined)).toBeUndefined()
    expect(translateSort('')).toBeUndefined()
  })

  it('translates ascending sort', () => {
    expect(translateSort('name')).toEqual({ name: 1 })
  })

  it('translates descending sort', () => {
    expect(translateSort('-createdAt')).toEqual({ $createdAt: -1 })
  })

  it('translates comma-separated fields', () => {
    expect(translateSort('-createdAt,name')).toEqual({ $createdAt: -1, name: 1 })
  })

  it('maps Payload field names to entity meta-fields', () => {
    expect(translateSort('-updatedAt')).toEqual({ $updatedAt: -1 })
    expect(translateSort('id')).toEqual({ $id: 1 })
  })

  it('handles array input', () => {
    expect(translateSort(['-createdAt', 'name'])).toEqual({ $createdAt: -1, name: 1 })
  })
})
