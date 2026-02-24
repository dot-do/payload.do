import type { UpdateMany } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData, slugToType } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { translateWhere } from '../queries/where.js'
import { THINGS_COLLECTION, thingsFind, thingsResolveType } from './things.js'

export const updateMany: UpdateMany = async function updateMany(this: DoPayloadAdapter, args) {
  const { collection, data, where } = args

  const updateData = documentToEntityData(data)
  const docs: Record<string, unknown>[] = []
  const ts = new Date().toISOString()

  if (collection === THINGS_COLLECTION) {
    // Things: find across all types, resolve type per entity
    const result = await thingsFind(this._service, this.namespace, { where, limit: 10000, pagination: false })
    delete updateData.type // can't change entity type

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
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const result = await this._service.find(this.namespace, type, filter, { limit: 10000 })

    for (const entity of result.items) {
      const entityId = entity.$id as string
      const updated = await this._service.update(this.namespace, type, entityId, updateData)
      if (updated) {
        docs.push(entityToDocument(updated))

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
  }

  return docs as any
}
