import type { CollectionConfig } from 'payload'

/**
 * Things — Universal data view across all entity types.
 *
 * Unlike other collections which map to a single entity type,
 * Things provides a unified view of ALL records in the entities table.
 * The `type` field is exposed for filtering by entity type (Contact, Deal, etc.).
 *
 * This is analogous to how the Events collection views all event types
 * from ClickHouse — Things views all entity types from SQLite.
 */
export const Thing: CollectionConfig = {
  slug: 'things',
  admin: {
    useAsTitle: 'name',
    group: 'Data',
    description: 'Universal data view — all entities across all types.',
  },
  fields: [
    { name: 'type', type: 'text', required: true, index: true, admin: { description: 'Entity type (Contact, Deal, Lead, etc.)' } },
    { name: 'name', type: 'text', admin: { description: 'Display name (extracted from entity data)' } },
    { name: 'status', type: 'text', admin: { description: 'Entity status' } },
    { name: 'data', type: 'json', admin: { description: 'Full entity data' } },
  ],
}
