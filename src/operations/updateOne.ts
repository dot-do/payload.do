import type { UpdateOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData, slugToType } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'
import { resolveNounSchema } from '../utilities/noun-cache.js'
import { translateWhere } from '../queries/where.js'
import { THINGS_COLLECTION, thingsFindOne, thingsResolveType } from './things.js'

export const updateOne: UpdateOne = async function updateOne(this: DoPayloadAdapter, args) {
  const { collection, data } = args

  // Payload may pass `id` instead of `where`
  const resolvedWhere: any = 'where' in args && args.where ? args.where : 'id' in args && (args as any).id != null ? { id: { equals: (args as any).id } } : undefined
  if (!resolvedWhere) throw new Error(`updateOne requires either 'id' or 'where' for ${collection}`)

  const updateData = documentToEntityData(data)
  let nounSchema: { schemaVersion: number; schemaHash: string } | null = null
  let entityId: string
  let type: string

  if (collection === THINGS_COLLECTION) {
    // Things: find entity across all types, resolve its type
    const existing = await thingsFindOne(this._service, this.namespace, resolvedWhere)
    if (!existing) throw new Error(`Document not found in ${collection}`)
    entityId = existing.id as string
    type = (await thingsResolveType(this._service, this.namespace, entityId)) ?? (existing.type as string)
    if (!type) throw new Error(`Cannot resolve type for entity ${entityId}`)
    // Remove type from update data (can't change the type column)
    delete updateData.type
  } else {
    type = slugToType(collection)
    const filter = translateWhere(resolvedWhere)
    const existing = await this._service.findOne(this.namespace, type, filter)
    if (!existing) throw new Error(`Document not found in ${collection}`)
    entityId = existing.$id as string
  }

  // Stamp latest schema version on update data
  if (collection !== THINGS_COLLECTION) {
    nounSchema = await resolveNounSchema(this._service, this.namespace, collection)
    if (nounSchema) {
      updateData._schemaVersion = nounSchema.schemaVersion
      updateData._schemaHash = nounSchema.schemaHash
    }
  }

  const updated = await this._service.update(this.namespace, type, entityId, updateData)
  if (!updated) throw new Error(`Failed to update document in ${collection}`)

  const doc = entityToDocument(updated)

  // For Things, add type to the document
  if (collection === THINGS_COLLECTION) {
    doc.type = type
  }

  // CDC event emission
  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: entityId,
      ns: this.context,
      event: `${collection}.updated`,
      entityType: type,
      entityData: updateData,
      meta: nounSchema ? { schemaHash: nounSchema.schemaHash, schemaVersion: nounSchema.schemaVersion } : undefined,
    }))
  } catch (err) {
    console.error('[cdc] Event emission failed:', err)
  }

  return doc as any
}
