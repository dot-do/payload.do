import { describe, it, expect } from 'vitest'
import { applyMigration, applyMigrations, type MigrationDef } from '../utilities/migrate'

describe('applyMigration', () => {
  describe('Layer 1: Declarative transforms', () => {
    it('addField — adds a field with default value', () => {
      const data = { name: 'Alice' }
      const migration: MigrationDef = { version: 2, name: 'add-status', ops: [{ op: 'addField', path: 'status', default: 'active' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice', status: 'active' })
    })

    it('addField — does not overwrite existing field', () => {
      const data = { name: 'Alice', status: 'inactive' }
      const migration: MigrationDef = { version: 2, name: 'add-status', ops: [{ op: 'addField', path: 'status', default: 'active' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice', status: 'inactive' })
    })

    it('removeField — removes a field', () => {
      const data = { name: 'Alice', legacy: true }
      const migration: MigrationDef = { version: 2, name: 'rm-legacy', ops: [{ op: 'removeField', path: 'legacy' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice' })
    })

    it('removeField — noop when field does not exist', () => {
      const data = { name: 'Alice' }
      const migration: MigrationDef = { version: 2, name: 'rm-legacy', ops: [{ op: 'removeField', path: 'legacy' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice' })
    })

    it('renameField — renames a field', () => {
      const data = { company: 'Acme' }
      const migration: MigrationDef = { version: 2, name: 'rename-company', ops: [{ op: 'renameField', from: 'company', to: 'organization' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ organization: 'Acme' })
    })

    it('renameField — noop when source does not exist', () => {
      const data = { organization: 'Acme' }
      const migration: MigrationDef = { version: 2, name: 'rename-company', ops: [{ op: 'renameField', from: 'company', to: 'organization' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ organization: 'Acme' })
    })

    it('coerceType — string to number', () => {
      const data = { priority: '3' }
      const migration: MigrationDef = { version: 2, name: 'coerce-priority', ops: [{ op: 'coerceType', path: 'priority', to: 'number', fallback: 0 }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ priority: 3 })
    })

    it('coerceType — uses fallback for non-coercible value', () => {
      const data = { priority: 'high' }
      const migration: MigrationDef = { version: 2, name: 'coerce-priority', ops: [{ op: 'coerceType', path: 'priority', to: 'number', fallback: 0 }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ priority: 0 })
    })

    it('coerceType — number to string', () => {
      const data = { code: 42 }
      const migration: MigrationDef = { version: 2, name: 'coerce-code', ops: [{ op: 'coerceType', path: 'code', to: 'string', fallback: '' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ code: '42' })
    })

    it('coerceType — to boolean', () => {
      const data = { active: 1 }
      const migration: MigrationDef = { version: 2, name: 'coerce-active', ops: [{ op: 'coerceType', path: 'active', to: 'boolean', fallback: false }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ active: true })
    })

    it('moveField — moves a nested field', () => {
      const data = { meta: { tags: ['a', 'b'] }, name: 'Alice' }
      const migration: MigrationDef = { version: 2, name: 'move-tags', ops: [{ op: 'moveField', from: 'meta.tags', to: 'tags' }] }
      const result = applyMigration(data, migration)
      expect(result.tags).toEqual(['a', 'b'])
      expect((result.meta as Record<string, unknown>).tags).toBeUndefined()
    })

    it('setDefault — sets value when missing', () => {
      const data = { name: 'Alice' }
      const migration: MigrationDef = { version: 2, name: 'default-role', ops: [{ op: 'setDefault', path: 'role', value: 'member', when: 'missing' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice', role: 'member' })
    })

    it('setDefault — does not overwrite when field exists', () => {
      const data = { name: 'Alice', role: 'admin' }
      const migration: MigrationDef = { version: 2, name: 'default-role', ops: [{ op: 'setDefault', path: 'role', value: 'member', when: 'missing' }] }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice', role: 'admin' })
    })
  })

  describe('Layer 2: Expression transforms', () => {
    it('mapValues — maps values using a lookup table', () => {
      const data = { status: '0' }
      const migration: MigrationDef = {
        version: 2,
        name: 'map-status',
        ops: [{ op: 'mapValues', path: 'status', map: { '0': 'draft', '1': 'published', '2': 'archived' } }],
      }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ status: 'draft' })
    })

    it('mapValues — keeps original when no mapping found', () => {
      const data = { status: 'unknown' }
      const migration: MigrationDef = {
        version: 2,
        name: 'map-status',
        ops: [{ op: 'mapValues', path: 'status', map: { '0': 'draft' } }],
      }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ status: 'unknown' })
    })

    it('compute — template expression', () => {
      const data = { firstName: 'Alice', lastName: 'Smith' }
      const migration: MigrationDef = {
        version: 2,
        name: 'compute-fullname',
        ops: [{ op: 'compute', path: 'fullName', expr: '${firstName} ${lastName}' }],
      }
      const result = applyMigration(data, migration)
      expect(result.fullName).toBe('Alice Smith')
    })

    it('splitField — splits a string into multiple fields', () => {
      const data = { name: 'Alice Smith' }
      const migration: MigrationDef = {
        version: 2,
        name: 'split-name',
        ops: [{ op: 'splitField', path: 'name', into: ['firstName', 'lastName'], delimiter: ' ' }],
      }
      const result = applyMigration(data, migration)
      expect(result.firstName).toBe('Alice')
      expect(result.lastName).toBe('Smith')
    })

    it('mergeFields — merges multiple fields into one', () => {
      const data = { firstName: 'Alice', lastName: 'Smith' }
      const migration: MigrationDef = {
        version: 2,
        name: 'merge-name',
        ops: [{ op: 'mergeFields', paths: ['firstName', 'lastName'], into: 'fullName', delimiter: ' ' }],
      }
      const result = applyMigration(data, migration)
      expect(result.fullName).toBe('Alice Smith')
    })
  })

  describe('Layer 3: Named transforms', () => {
    it('transform — calls a registered function by name', () => {
      const data = { name: 'Hello World' }
      const migration: MigrationDef = {
        version: 2,
        name: 'slugify-name',
        ops: [{ op: 'transform', fn: 'slugify', args: { source: 'name', target: 'slug' } }],
      }
      const result = applyMigration(data, migration)
      expect(result.slug).toBe('hello-world')
    })

    it('transform — unknown function is a noop', () => {
      const data = { name: 'Alice' }
      const migration: MigrationDef = {
        version: 2,
        name: 'unknown-fn',
        ops: [{ op: 'transform', fn: 'nonexistent', args: {} }],
      }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ name: 'Alice' })
    })
  })

  describe('multi-op migration', () => {
    it('applies ops sequentially', () => {
      const data = { company: 'Acme', legacy: true }
      const migration: MigrationDef = {
        version: 2,
        name: 'cleanup',
        ops: [
          { op: 'renameField', from: 'company', to: 'organization' },
          { op: 'removeField', path: 'legacy' },
          { op: 'addField', path: 'status', default: 'active' },
        ],
      }
      const result = applyMigration(data, migration)
      expect(result).toEqual({ organization: 'Acme', status: 'active' })
    })
  })
})

describe('applyMigrations (chain)', () => {
  const migrations: MigrationDef[] = [
    { version: 2, name: 'add-status', ops: [{ op: 'addField', path: 'status', default: 'active' }] },
    { version: 3, name: 'rename-company', ops: [{ op: 'renameField', from: 'company', to: 'organization' }] },
    { version: 4, name: 'add-slug', ops: [{ op: 'transform', fn: 'slugify', args: { source: 'name', target: 'slug' } }] },
  ]

  it('applies all migrations from version 1', () => {
    const data: Record<string, unknown> = { name: 'Acme Corp', company: 'Acme' }
    applyMigrations(data, 1, migrations)
    expect(data.status).toBe('active')
    expect(data.organization).toBe('Acme')
    expect(data.company).toBeUndefined()
    expect(data.slug).toBe('acme-corp')
  })

  it('applies only pending migrations from version 2', () => {
    const data: Record<string, unknown> = { name: 'Acme Corp', company: 'Acme', status: 'active' }
    applyMigrations(data, 2, migrations)
    expect(data.organization).toBe('Acme')
    expect(data.slug).toBe('acme-corp')
  })

  it('applies no migrations when already at latest', () => {
    const data: Record<string, unknown> = { name: 'Acme Corp', organization: 'Acme', status: 'active', slug: 'acme-corp' }
    const original = { ...data }
    applyMigrations(data, 4, migrations)
    expect(data).toEqual(original)
  })

  it('handles out-of-order migration array', () => {
    const shuffled = [migrations[2], migrations[0], migrations[1]]
    const data: Record<string, unknown> = { name: 'Acme Corp', company: 'Acme' }
    applyMigrations(data, 1, shuffled)
    expect(data.status).toBe('active')
    expect(data.organization).toBe('Acme')
    expect(data.slug).toBe('acme-corp')
  })

  it('handles empty migrations array', () => {
    const data = { name: 'Alice' }
    applyMigrations(data, 1, [])
    expect(data).toEqual({ name: 'Alice' })
  })
})

describe('nested path support', () => {
  it('addField — creates intermediate objects for nested paths', () => {
    const data = { name: 'Alice' } as Record<string, unknown>
    const migration: MigrationDef = { version: 2, name: 'add-meta-version', ops: [{ op: 'addField', path: 'meta.version', default: 1 }] }
    const result = applyMigration(data, migration)
    expect(result).toEqual({ name: 'Alice', meta: { version: 1 } })
  })

  it('addField — does not overwrite existing nested value', () => {
    const data = { name: 'Alice', meta: { version: 2 } } as Record<string, unknown>
    const migration: MigrationDef = { version: 2, name: 'add-meta-version', ops: [{ op: 'addField', path: 'meta.version', default: 1 }] }
    const result = applyMigration(data, migration)
    expect(result).toEqual({ name: 'Alice', meta: { version: 2 } })
  })

  it('renameField — works with nested paths', () => {
    const data = { address: { zip: '12345' } } as Record<string, unknown>
    const migration: MigrationDef = { version: 2, name: 'rename-zip', ops: [{ op: 'renameField', from: 'address.zip', to: 'address.postalCode' }] }
    const result = applyMigration(data, migration)
    expect(result).toEqual({ address: { postalCode: '12345' } })
  })

  it('renameField — noop when nested source does not exist', () => {
    const data = { address: { city: 'NYC' } } as Record<string, unknown>
    const migration: MigrationDef = { version: 2, name: 'rename-zip', ops: [{ op: 'renameField', from: 'address.zip', to: 'address.postalCode' }] }
    const result = applyMigration(data, migration)
    expect(result).toEqual({ address: { city: 'NYC' } })
  })
})
