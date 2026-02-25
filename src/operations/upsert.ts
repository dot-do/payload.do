import type { Upsert } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { THINGS_COLLECTION, thingsFindOne, thingsResolveType } from './things.js'

export const upsert: Upsert = async function upsert(this: DoPayloadAdapter, args) {
  const { collection, data, where } = args

  if (collection === THINGS_COLLECTION) {
    // Things: find across all types
    const existing = await thingsFindOne(this._service, this.namespace, where)

    if (existing) {
      const entityId = existing.id as string
      const type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (existing.type as string)
      if (!type) throw new Error(`Cannot resolve type for entity ${entityId}`)

      const updateData = documentToEntityData(data)
      delete updateData.type
      const updated = await this._service.update(this.namespace, type, entityId, updateData)
      if (!updated) throw new Error(`Failed to update document in ${collection}`)

      const doc = entityToDocument(updated)
      doc.type = type

      try {
        await this._service.sendEvent(this.namespace, buildCdcEvent({
          id: entityId, ns: this.context, event: `${collection}.updated`,
          entityType: type, entityData: updateData,
        }))
      } catch (err) {
        console.error('[cdc] Event emission failed:', err)
      }

      return doc as any
    }

    // CREATE path — type required in data
    const entityData = documentToEntityData(data)
    const type = entityData.type as string
    if (!type) throw new Error('Things collection requires a "type" field for creation')
    delete entityData.type

    const created = (await this._service.create(this.namespace, type, entityData)) as Record<string, unknown>
    const doc = entityToDocument(created)
    doc.type = type

    try {
      await this._service.sendEvent(this.namespace, buildCdcEvent({
        id: created.$id as string, ns: this.context, event: `${collection}.created`,
        entityType: type, entityData,
      }))
    } catch (err) {
      console.error('[cdc] Event emission failed:', err)
    }

    return doc as any
  }

  // Standard collections: single compound call (find → update or create + CDC)
  return this._service.payloadUpsert(this.namespace, collection, where, data, this.context) as any
}
