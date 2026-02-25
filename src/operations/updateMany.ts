import type { UpdateMany } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { THINGS_COLLECTION, thingsFind, thingsResolveType } from './things.js'

export const updateMany: UpdateMany = async function updateMany(this: DoPayloadAdapter, args) {
  const { collection, data, where } = args

  const docs: Record<string, unknown>[] = []

  if (collection === THINGS_COLLECTION) {
    // Things: find across all types, resolve type per entity
    const updateData = documentToEntityData(data)
    delete updateData.type // can't change entity type
    const result = await thingsFind(this._service, this.namespace, { where, limit: 10000, pagination: false })

    for (const doc of result.docs) {
      const entityId = doc.id as string
      const type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (doc.type as string)
      if (!type) continue

      const updated = await this._service.update(this.namespace, type, entityId, updateData)
      if (updated) {
        const updatedDoc = entityToDocument(updated)
        updatedDoc.type = type
        docs.push(updatedDoc)

        try {
          await this._service.sendEvent(this.namespace, buildCdcEvent({
            id: entityId, ns: this.context, event: `${collection}.updated`,
            entityType: type, entityData: updateData,
          }))
        } catch (err) {
          console.error('[cdc] Event emission failed:', err)
        }
      }
    }
  } else {
    // Standard collections: compound find + loop compound updates
    const result = (await this._service.payloadFind(this.namespace, collection, where, undefined, 10000, 1, false)) as any

    for (const doc of result.docs ?? []) {
      try {
        const updated = await this._service.payloadUpdateOne(this.namespace, collection, { id: { equals: doc.id } }, undefined, data, this.context)
        if (updated) docs.push(updated)
      } catch (err) {
        console.error('[updateMany] Failed to update doc:', err)
      }
    }
  }

  return docs as any
}
