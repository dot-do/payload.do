import type { Count } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { slugToType } from '../utilities/transforms.js'
import { translateWhere } from '../queries/where.js'
import { CH_COLLECTIONS, chCount } from './analytics.js'
import { THINGS_COLLECTION, thingsCount } from './things.js'

export const count: Count = async function count(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  if (CH_COLLECTIONS.has(collection)) {
    const totalDocs = await chCount(this._service, collection, this.context, where)
    return { totalDocs }
  }

  // Things = universal view (no type filter)
  if (collection === THINGS_COLLECTION) {
    const totalDocs = await thingsCount(this._service, this.namespace, where)
    return { totalDocs }
  }

  const type = slugToType(collection)
  const filter = translateWhere(where)

  const totalDocs = await this._service.count(this.namespace, type, filter)
  return { totalDocs }
}
