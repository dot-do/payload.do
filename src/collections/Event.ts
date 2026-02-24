import type { CollectionConfig } from 'payload'

export const Event: CollectionConfig = {
  slug: 'events',
  admin: {
    useAsTitle: 'event',
    group: 'Data',
    description: 'Append-only CDC events — read-only access to entity change history.',
  },
  fields: [
    { name: 'event', type: 'text', required: true },
    { name: 'source', type: 'text' },
    { name: 'type', type: 'select', options: ['cdc', 'trace', 'metric', 'log'], defaultValue: 'cdc' },
    { name: 'ns', type: 'text' },
    { name: 'ts', type: 'text' },
    { name: 'data', type: 'json' },
  ],
}
