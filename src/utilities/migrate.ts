/**
 * Migration op executor — applies a sequence of declarative transforms to entity data.
 *
 * Three layers:
 *   Layer 1: Declarative (addField, removeField, renameField, moveField, coerceType, setDefault)
 *   Layer 2: Expression (compute, mapValues, splitField, mergeFields)
 *   Layer 3: Named transforms (transform — calls a registered function by name)
 */

export interface MigrationOp {
  op: string
  [key: string]: unknown
}

export interface MigrationDef {
  version: number
  name: string
  ops: MigrationOp[]
}

// --- Transform registry (Layer 3) ---

type TransformFn = (data: Record<string, unknown>, args: Record<string, unknown>) => void

const transforms = new Map<string, TransformFn>()

/** Register a named transform function. */
export function registerTransform(name: string, fn: TransformFn): void {
  transforms.set(name, fn)
}

// Built-in transforms
registerTransform('slugify', (data, args) => {
  const source = data[args.source as string]
  if (typeof source === 'string') {
    data[args.target as string] = source
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
  }
})

registerTransform('lowercase', (data, args) => {
  const val = data[args.path as string]
  if (typeof val === 'string') data[args.path as string] = val.toLowerCase()
})

registerTransform('uppercase', (data, args) => {
  const val = data[args.path as string]
  if (typeof val === 'string') data[args.path as string] = val.toUpperCase()
})

registerTransform('trim', (data, args) => {
  const val = data[args.path as string]
  if (typeof val === 'string') data[args.path as string] = val.trim()
})

// --- Nested path helpers ---

function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

function setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
  const parts = path.split('.')
  let current: Record<string, unknown> = obj
  for (let i = 0; i < parts.length - 1; i++) {
    if (current[parts[i]] === undefined || typeof current[parts[i]] !== 'object') {
      current[parts[i]] = {}
    }
    current = current[parts[i]] as Record<string, unknown>
  }
  current[parts[parts.length - 1]] = value
}

function deleteNestedValue(obj: Record<string, unknown>, path: string): void {
  const parts = path.split('.')
  let current: Record<string, unknown> = obj
  for (let i = 0; i < parts.length - 1; i++) {
    if (current[parts[i]] === undefined || typeof current[parts[i]] !== 'object') return
    current = current[parts[i]] as Record<string, unknown>
  }
  delete current[parts[parts.length - 1]]
}

// --- Op executors ---

function coerce(value: unknown, to: string, fallback: unknown): unknown {
  switch (to) {
    case 'number': {
      const n = Number(value)
      return Number.isNaN(n) ? fallback : n
    }
    case 'string':
      return value == null ? fallback : String(value)
    case 'boolean':
      return value == null ? fallback : Boolean(value)
    default:
      return fallback
  }
}

function applyOp(data: Record<string, unknown>, op: MigrationOp): void {
  switch (op.op) {
    // Layer 1
    case 'addField': {
      if (getNestedValue(data, op.path as string) === undefined) {
        setNestedValue(data, op.path as string, op.default)
      }
      break
    }
    case 'removeField': {
      deleteNestedValue(data, op.path as string)
      break
    }
    case 'renameField': {
      const from = op.from as string
      const to = op.to as string
      const value = getNestedValue(data, from)
      if (value !== undefined) {
        setNestedValue(data, to, value)
        deleteNestedValue(data, from)
      }
      break
    }
    case 'moveField': {
      const from = op.from as string
      const to = op.to as string
      const value = getNestedValue(data, from)
      if (value !== undefined) {
        setNestedValue(data, to, value)
        deleteNestedValue(data, from)
      }
      break
    }
    case 'coerceType': {
      const path = op.path as string
      const value = getNestedValue(data, path)
      if (value !== undefined) {
        setNestedValue(data, path, coerce(value, op.to as string, op.fallback))
      }
      break
    }
    case 'setDefault': {
      const path = op.path as string
      const when = (op.when as string) ?? 'missing'
      const existing = getNestedValue(data, path)
      const shouldSet =
        (when === 'missing' && existing === undefined) ||
        (when === 'null' && (existing === null || existing === undefined)) ||
        (when === 'empty' && (existing === null || existing === undefined || existing === ''))
      if (shouldSet) setNestedValue(data, path, op.value)
      break
    }
    // Layer 2
    case 'mapValues': {
      const path = op.path as string
      const map = op.map as Record<string, unknown>
      const current = getNestedValue(data, path)
      if (current !== undefined && String(current) in map) {
        setNestedValue(data, path, map[String(current)])
      }
      break
    }
    case 'compute': {
      const path = op.path as string
      const expr = op.expr as string
      const result = expr.replace(/\$\{(\w+)}/g, (_, key) => String(data[key] ?? ''))
      setNestedValue(data, path, result)
      break
    }
    case 'splitField': {
      const path = op.path as string
      const into = op.into as string[]
      const delimiter = (op.delimiter as string) ?? ' '
      const value = getNestedValue(data, path)
      if (typeof value === 'string') {
        const parts = value.split(delimiter)
        for (let i = 0; i < into.length; i++) {
          setNestedValue(data, into[i], parts[i] ?? '')
        }
      }
      break
    }
    case 'mergeFields': {
      const paths = op.paths as string[]
      const into = op.into as string
      const delimiter = (op.delimiter as string) ?? ' '
      const values = paths.map((p) => getNestedValue(data, p)).filter((v) => v != null)
      setNestedValue(data, into, values.join(delimiter))
      break
    }
    // Layer 3
    case 'transform': {
      const fn = transforms.get(op.fn as string)
      if (fn) fn(data, (op.args as Record<string, unknown>) ?? {})
      break
    }
  }
}

/** Apply a single migration (all its ops) to entity data. Returns mutated data. */
export function applyMigration(data: Record<string, unknown>, migration: MigrationDef): Record<string, unknown> {
  for (const op of migration.ops) {
    applyOp(data, op)
  }
  return data
}

/**
 * Apply all pending migrations to entity data.
 *
 * @param data - Entity data (will be mutated)
 * @param currentVersion - Entity's current _schemaVersion (default 1)
 * @param migrations - Full migration chain from the Noun, sorted by version ascending
 * @returns Mutated data with all migrations applied
 */
export function applyMigrations(data: Record<string, unknown>, currentVersion: number, migrations: MigrationDef[]): Record<string, unknown> {
  const pending = migrations.filter((m) => m.version > currentVersion).sort((a, b) => a.version - b.version)
  for (const migration of pending) {
    applyMigration(data, migration)
  }
  return data
}
