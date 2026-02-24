import { applyMigrations, type MigrationDef } from './migrate.js'

/**
 * Transform between Payload documents and DatabaseDO entities.
 *
 * DatabaseDO entities table schema:
 *   id TEXT PRIMARY KEY    — "{type}_{sqid}"
 *   type TEXT              — PascalCase entity type
 *   data TEXT              — JSON blob (all fields)
 *   created_at TEXT        — ISO timestamp
 *   updated_at TEXT        — ISO timestamp
 *   deleted_at TEXT        — ISO timestamp or null (soft delete)
 *
 * Payload documents expect:
 *   id: string             — Entity ID
 *   createdAt: string      — ISO timestamp
 *   updatedAt: string      — ISO timestamp
 *   ...fields              — All collection fields
 */

export interface NounContext {
  schemaHash: string
  schemaVersion: number
  migrations: MigrationDef[]
}

/**
 * Convert a DatabaseDO entity (from collection API) to a Payload document.
 * The collection API returns entities with $id, $type, $createdAt, $updatedAt meta-fields.
 *
 * When a NounContext is provided, applies pending schema migrations to bring
 * the entity data up to the current schema version before returning.
 */
export function entityToDocument(entity: Record<string, unknown>, noun?: NounContext): Record<string, unknown> {
  const { $id, $type, $createdAt, $updatedAt, $version, $createdBy, $updatedBy, $deletedAt, ...fields } = entity

  // Fast path: no noun context — return as-is (backward compat)
  if (!noun || !noun.migrations || noun.migrations.length === 0) {
    return {
      id: $id as string,
      ...fields,
      createdAt: $createdAt as string,
      updatedAt: $updatedAt as string,
    }
  }

  // Fast path: schema hash matches — entity is current
  if (fields._schemaHash === noun.schemaHash) {
    return {
      id: $id as string,
      ...fields,
      createdAt: $createdAt as string,
      updatedAt: $updatedAt as string,
    }
  }

  // Slow path: apply pending migrations
  const currentVersion = (fields._schemaVersion as number) ?? 1
  applyMigrations(fields, currentVersion, noun.migrations)
  fields._schemaVersion = noun.schemaVersion
  fields._schemaHash = noun.schemaHash

  return {
    id: $id as string,
    ...fields,
    createdAt: $createdAt as string,
    updatedAt: $updatedAt as string,
  }
}

/**
 * Strip Payload meta-fields from a document before storing as entity data.
 */
export function documentToEntityData(data: Record<string, unknown>): Record<string, unknown> {
  const { id: _id, createdAt: _ca, updatedAt: _ua, password: _pw, 'confirm-password': _cpw, ...rest } = data
  return rest
}

/**
 * Known plural → singular mappings for Payload collection slugs.
 * Only strips trailing 's' for known standard plurals.
 */
const PLURAL_MAP: Record<string, string> = {
  contacts: 'Contact',
  deals: 'Deal',
  leads: 'Lead',
  users: 'User',
  things: 'Thing',
  nouns: 'Noun',
  verbs: 'Verb',
  actions: 'Action',
  events: 'Event',
  agents: 'Agent',
  models: 'Model',
  prompts: 'Prompt',
  tools: 'Tool',
  functions: 'Function',
  workflows: 'Workflow',
  packages: 'Package',
  modules: 'Module',
  components: 'Component',
  integrations: 'Integration',
  connections: 'Connection',
  webhooks: 'Webhook',
  domains: 'Domain',
  directories: 'Directory',
  sources: 'Source',
  resources: 'Resource',
  orgs: 'Org',
  roles: 'Role',
  accounts: 'Account',
  teams: 'Team',
  memories: 'Memory',
  subscriptions: 'Subscription',
  invoices: 'Invoice',
  payments: 'Payment',
  customers: 'Customer',
  products: 'Product',
  plans: 'Plan',
  prices: 'Price',
  projects: 'Project',
  issues: 'Issue',
  comments: 'Comment',
  tickets: 'Ticket',
  campaigns: 'Campaign',
  segments: 'Segment',
  forms: 'Form',
  experiments: 'Experiment',
  messages: 'Message',
  sites: 'Site',
  assets: 'Asset',
  contents: 'Content',
  metrics: 'Metric',
  funnels: 'Funnel',
  goals: 'Goal',
  pipelines: 'Pipeline',
  activities: 'Activity',
  organizations: 'Organization',
  'feature-flags': 'FeatureFlag',
  'api-keys': 'ApiKey',
  traces: 'Trace',
  errors: 'ErrorEvent',
  'cdc-events': 'CdcEvent',
  'webhook-events': 'WebhookEvent',
  'github-events': 'GithubEvent',
  'stripe-events': 'StripeEvent',
  'api-requests': 'ApiRequest',
}

/**
 * Convert a Payload collection slug to a PascalCase entity type.
 * Uses a known mapping for standard plurals, falls back to PascalCase without stripping.
 * e.g., 'contacts' → 'Contact', 'feature-flags' → 'FeatureFlag', 'media' → 'Media'
 */
export function slugToType(slug: string): string {
  const mapped = PLURAL_MAP[slug]
  if (mapped) return mapped

  // Fallback: PascalCase each segment, don't strip trailing 's' (could be part of the word)
  return slug
    .split(/[-_\s]+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('')
}

/**
 * Extract a display title from entity data.
 */
export function extractTitle(data: Record<string, unknown>): string | null {
  return (data.title as string) ?? (data.name as string) ?? null
}
