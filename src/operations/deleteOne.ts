import type { DeleteOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { THINGS_COLLECTION, thingsFindOne, thingsResolveType } from './things.js'

export const deleteOne: DeleteOne = async function deleteOne(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  // Things: find entity across all types
  if (collection === THINGS_COLLECTION) {
    const existing = await thingsFindOne(this._service, this.namespace, where)
    if (!existing) return {} as any
    const entityId = existing.id as string
    const type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (existing.type as string)
    if (!type) return {} as any

    await this._service.delete(this.namespace, type, entityId)

    try {
      await this._service.sendEvent(this.namespace, buildCdcEvent({
        id: entityId, ns: this.context, event: `${collection}.deleted`,
        entityType: type, entityData: { $id: entityId },
      }))
    } catch (err) {
      console.error('[cdc] Event emission failed:', err)
    }

    return existing as any
  }

  // Standard collections: single compound call (find + soft delete + CDC)
  return this._service.payloadDeleteOne(this.namespace, collection, where, this.context) as any
}
