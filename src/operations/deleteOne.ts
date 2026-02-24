import type { DeleteOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, slugToType } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { translateWhere } from '../queries/where.js'
import { THINGS_COLLECTION, thingsFindOne, thingsResolveType } from './things.js'

export const deleteOne: DeleteOne = async function deleteOne(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  let entityId: string
  let type: string
  let existingDoc: Record<string, unknown>

  if (collection === THINGS_COLLECTION) {
    // Things: find entity across all types
    const existing = await thingsFindOne(this._service, this.namespace, where)
    if (!existing) return {} as any
    entityId = existing.id as string
    type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (existing.type as string)
    if (!type) return {} as any
    existingDoc = existing
  } else {
    type = slugToType(collection)
    const filter = translateWhere(where)
    const existing = await this._service.findOne(this.namespace, type, filter)
    if (!existing) return {} as any
    entityId = existing.$id as string
    existingDoc = entityToDocument(existing)
  }

  // Soft delete via the collection API
  await this._service.delete(this.namespace, type, entityId)

  // CDC event emission
  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: entityId,
      ns: this.context,
      event: `${collection}.deleted`,
      entityType: type,
      entityData: { $id: entityId },
    }))
  } catch (err) {
    console.error('[cdc] Event emission failed:', err)
  }

  return existingDoc as any
}
