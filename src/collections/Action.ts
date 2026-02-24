import type { CollectionConfig } from 'payload'

export const Action: CollectionConfig = {
  slug: 'actions',
  admin: {
    useAsTitle: 'name',
    group: 'Data',
    description: 'Subject-verb-object bindings — connects Nouns to Verbs.',
  },
  fields: [
    { name: 'name', type: 'text', required: true },
    { name: 'slug', type: 'text', unique: true },
    { name: 'noun', type: 'relationship', relationTo: 'nouns', required: true },
    { name: 'verb', type: 'relationship', relationTo: 'verbs', required: true },
    { name: 'description', type: 'textarea' },
    { name: 'schema', type: 'json' },
    { name: 'access', type: 'json' },
  ],
}
