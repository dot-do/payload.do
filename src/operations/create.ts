import type { Create } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData, slugToType } from '../utilities/transforms.js'
import { buildCdcEvent, ulid } from '../utilities/cdc.js'
import { resolveNounId, resolveNounSchema } from '../utilities/noun-cache.js'
import { CH_COLLECTIONS } from './analytics.js'
import { THINGS_COLLECTION } from './things.js'

export const create: Create = async function create(this: DoPayloadAdapter, { collection, data }: any) {
  const entityData = documentToEntityData(data)
  const ts = new Date().toISOString()

  // Events collection: send to Pipeline only, skip SQLite
  if (CH_COLLECTIONS.has(collection)) {
    const eventId = ulid()
    try {
      await this._service.sendEvent(this.namespace, {
        id: eventId,
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
  let type: string
  if (collection === THINGS_COLLECTION) {
    type = entityData.type as string
    if (!type) throw new Error('Things collection requires a "type" field')
    // Remove type from entity data (stored in the type column, not the JSON blob)
    delete entityData.type
  } else {
    type = slugToType(collection)
  }

  // Resolve the Noun ID for this collection's type and attach it to entity data
  let nounSchema: { schemaVersion: number; schemaHash: string } | null = null
  if (collection !== THINGS_COLLECTION && !entityData.noun) {
    const nounId = await resolveNounId(this._service, this.namespace, collection)
    if (nounId) {
      entityData.noun = nounId
    }
  }

  // Stamp schema version on entity data
  if (collection !== THINGS_COLLECTION) {
    nounSchema = await resolveNounSchema(this._service, this.namespace, collection)
    if (nounSchema) {
      entityData._schemaVersion = nounSchema.schemaVersion
      entityData._schemaHash = nounSchema.schemaHash
    }
  }

  const entity = (await this._service.create(this.namespace, type, entityData)) as Record<string, unknown>
  const doc = entityToDocument(entity)

  // For Things, add type back to the document
  if (collection === THINGS_COLLECTION) {
    doc.type = type
  }

  // CDC event emission
  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: entity.$id as string,
      ns: this.context,
      event: `${collection}.created`,
      entityType: type,
      entityData,
      meta: nounSchema ? { schemaHash: nounSchema.schemaHash, schemaVersion: nounSchema.schemaVersion } : undefined,
    }))
  } catch (err) {
    console.error('[cdc] Event emission failed:', err)
  }

  return doc as any
}
