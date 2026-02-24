import type { CollectionConfig } from 'payload'

export const Verb: CollectionConfig = {
  slug: 'verbs',
  admin: {
    useAsTitle: 'name',
    group: 'Data',
    description: 'Action definitions — verbs with conjugation lifecycle.',
  },
  fields: [
    { name: 'name', type: 'text', required: true },
    { name: 'slug', type: 'text', unique: true },
    { name: 'description', type: 'textarea' },
    { name: 'past', type: 'text' },
    { name: 'present', type: 'text' },
    { name: 'gerund', type: 'text' },
    { name: 'reverse', type: 'text' },
    { name: 'schema', type: 'json' },
  ],
}
