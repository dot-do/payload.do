import type { Create } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData, slugToType } from '../utilities/transforms.js'
import { buildCdcEvent, ulid } from '../utilities/cdc.js'
import { CH_COLLECTIONS } from './analytics.js'
import { THINGS_COLLECTION } from './things.js'

export const create: Create = async function create(this: DoPayloadAdapter, { collection, data }: any) {
  const ts = new Date().toISOString()

  // Events collection: send to Pipeline only, skip SQLite
  if (CH_COLLECTIONS.has(collection)) {
    const entityData = documentToEntityData(data)
    const eventId = ulid()
    try {
      await this._service.sendEvent(this.namespace, {
        id: eventId,
        ray: '',
        ns: this.context,
        ts,
        type: entityData.type ?? 'custom',
        event: entityData.event ?? 'events.created',
        source: entityData.source ?? 'platform',
        url: this.context,
        actor: { id: 'system' },
        data: entityData,
        meta: {},
      })
    } catch {
      // fire-and-forget
    }
    return { id: eventId, ...data, createdAt: ts, updatedAt: ts } as any
  }

  // Things = universal view: type comes from data, not collection slug
  if (collection === THINGS_COLLECTION) {
    const entityData = documentToEntityData(data)
    const type = entityData.type as string
    if (!type) throw new Error('Things collection requires a "type" field')
    delete entityData.type

    const entity = (await this._service.create(this.namespace, type, entityData)) as Record<string, unknown>
    const doc = entityToDocument(entity)
    doc.type = type

    try {
      await this._service.sendEvent(this.namespace, buildCdcEvent({
        id: entity.$id as string, ns: this.context, event: `${collection}.created`,
        entityType: type, entityData,
      }))
    } catch (err) {
      console.error('[cdc] Event emission failed:', err)
    }

    return doc as any
  }

  // Standard collections: single compound call (slug→type + noun stamping + create + CDC)
  return this._service.payloadCreate(this.namespace, collection, data, this.context) as any
}
