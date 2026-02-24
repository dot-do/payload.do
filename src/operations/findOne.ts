import type { FindOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { entityToDocument, slugToType } from '../utilities/transforms.js'
import { resolveNounContext } from '../utilities/noun-cache.js'
import { translateWhere } from '../queries/where.js'
import { CH_COLLECTIONS, chFindOne } from './analytics.js'
import { THINGS_COLLECTION, thingsFindOne } from './things.js'

export const findOne: FindOne = async function findOne(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  if (CH_COLLECTIONS.has(collection)) {
    return chFindOne(this._service, collection, this.context, where) as any
  }

  // Things = universal view (no type filter)
  if (collection === THINGS_COLLECTION) {
    return thingsFindOne(this._service, this.namespace, where) as any
  }

  const type = slugToType(collection)
  const filter = translateWhere(where)

  const entity = await this._service.findOne(this.namespace, type, filter)
  if (!entity) return null

  // Resolve NounContext for migration-on-read
  const nounCtx = await resolveNounContext(this._service, this.namespace, collection)
  return entityToDocument(entity, nounCtx ?? undefined) as any
}
