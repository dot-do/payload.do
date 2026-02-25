import type { UpdateOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { THINGS_COLLECTION, thingsFindOne, thingsResolveType } from './things.js'

export const updateOne: UpdateOne = async function updateOne(this: DoPayloadAdapter, args) {
  const { collection, data } = args

  // Payload may pass `id` instead of `where`
  const resolvedWhere: any = 'where' in args && args.where ? args.where : undefined
  const resolvedId: string | undefined = 'id' in args && (args as any).id != null ? String((args as any).id) : undefined

  if (!resolvedWhere && !resolvedId) throw new Error(`updateOne requires either 'id' or 'where' for ${collection}`)

  // Things: find entity across all types, resolve its type
  if (collection === THINGS_COLLECTION) {
    const effectiveWhere = resolvedWhere ?? { id: { equals: resolvedId } }
    const existing = await thingsFindOne(this._service, this.namespace, effectiveWhere)
    if (!existing) throw new Error(`Document not found in ${collection}`)
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

  // Standard collections: single compound call (find + noun stamping + update + CDC)
  return this._service.payloadUpdateOne(this.namespace, collection, resolvedWhere, resolvedId, data, this.context) as any
}
