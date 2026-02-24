import type { DeleteMany } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { slugToType } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { translateWhere } from '../queries/where.js'
import { THINGS_COLLECTION, thingsFind, thingsResolveType } from './things.js'

export const deleteMany: DeleteMany = async function deleteMany(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  const ts = new Date().toISOString()

  if (collection === THINGS_COLLECTION) {
    // Things: find across all types, resolve type per entity
    const result = await thingsFind(this._service, this.namespace, { where, limit: 10000, pagination: false })

    for (const doc of result.docs) {
      const entityId = doc.id as string
      const type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (doc.type as string)
      if (!type) continue

      try {
        await this._service.delete(this.namespace, type, entityId)
        await this._service.sendEvent(this.namespace, buildCdcEvent({
          id: entityId, ns: this.context, event: `${collection}.deleted`,
          entityType: type, entityData: { $id: entityId },
        }))
      } catch {
        // fire-and-forget
      }
    }
  } else {
    const type = slugToType(collection)
    const filter = translateWhere(where)
    const result = await this._service.find(this.namespace, type, filter, { limit: 10000 })

    for (const entity of result.items) {
      const entityId = entity.$id as string
      try {
        await this._service.delete(this.namespace, type, entityId)
        await this._service.sendEvent(this.namespace, buildCdcEvent({
          id: entityId, ns: this.context, event: `${collection}.deleted`,
          entityType: type, entityData: { $id: entityId },
        }))
      } catch {
        // fire-and-forget
      }
    }
  }
}
