import type { CollectionConfig } from 'payload'

export const Noun: CollectionConfig = {
  slug: 'nouns',
  admin: {
    useAsTitle: 'name',
    group: 'Data',
    description: 'Schema definitions — each Noun becomes a tenant collection.',
  },
  fields: [
    { name: 'name', type: 'text', required: true },
    { name: 'plural', type: 'text', required: true },
    { name: 'slug', type: 'text', unique: true },
    { name: 'description', type: 'textarea' },
    { name: 'group', type: 'text' },
    { name: 'source', type: 'select', options: ['Platform', 'Seed', 'Tenant', 'AI'], defaultValue: 'Platform' },
    { name: 'schema', type: 'json' },
    { name: 'verbs', type: 'json' },
    { name: 'layout', type: 'json' },
    { name: 'access', type: 'json' },
    { name: 'hooks', type: 'json' },
    { name: 'validate', type: 'json' },
  ],
}
