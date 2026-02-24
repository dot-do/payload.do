import type { CreateGlobal, FindGlobal, UpdateGlobal } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, documentToEntityData } from '../utilities/transforms.js'
import { buildCdcEvent } from '../utilities/cdc.js'

/**
 * Globals are stored with type `_global_{slug}` in the entities table.
 * There's exactly one entity per global slug.
 */

export const createGlobal: CreateGlobal = async function createGlobal(this: DoPayloadAdapter, args) {
  const { slug, data } = args
  const type = `_global_${slug}`

  // Check if already exists
  const existing = await this._service.findOne(this.namespace, type)
  if (existing) {
    // Update existing global
    const entityId = existing.$id as string
    const entityData = documentToEntityData(data)
    const updated = await this._service.update(this.namespace, type, entityId, entityData)
    const doc = entityToDocument(updated ?? existing)

    try {
      await this._service.sendEvent(this.namespace, buildCdcEvent({
        id: entityId, ns: this.context, event: `_global_${slug}.updated`,
        entityType: type, entityData,
      }))
    } catch (err) { console.error('[cdc] Event emission failed:', err) }

    return doc as any
  }

  // Create new global
  const entityData = documentToEntityData(data)
  const created = (await this._service.create(this.namespace, type, entityData)) as Record<string, unknown>
  const doc = entityToDocument(created)

  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: created.$id as string, ns: this.context, event: `_global_${slug}.created`,
      entityType: type, entityData,
    }))
  } catch (err) { console.error('[cdc] Event emission failed:', err) }

  return doc as any
}

export const findGlobal: FindGlobal = async function findGlobal(this: DoPayloadAdapter, args) {
  const { slug } = args
  const type = `_global_${slug}`

  const entity = await this._service.findOne(this.namespace, type)
  if (!entity) return {} as any

  return entityToDocument(entity) as any
}

export const updateGlobal: UpdateGlobal = async function updateGlobal(this: DoPayloadAdapter, args) {
  const { slug, data } = args
  const type = `_global_${slug}`

  const existing = await this._service.findOne(this.namespace, type)

  if (existing) {
    const entityId = existing.$id as string
    const entityData = documentToEntityData(data)
    const updated = await this._service.update(this.namespace, type, entityId, entityData)
    const doc = entityToDocument(updated ?? existing)

    try {
      await this._service.sendEvent(this.namespace, buildCdcEvent({
        id: entityId, ns: this.context, event: `_global_${slug}.updated`,
        entityType: type, entityData,
      }))
    } catch (err) { console.error('[cdc] Event emission failed:', err) }

    return doc as any
  }

  // Create if not exists
  const entityData = documentToEntityData(data)
  const created = (await this._service.create(this.namespace, type, entityData)) as Record<string, unknown>
  const doc = entityToDocument(created)

  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: created.$id as string, ns: this.context, event: `_global_${slug}.created`,
      entityType: type, entityData,
    }))
  } catch (err) { console.error('[cdc] Event emission failed:', err) }

  return doc as any
}
