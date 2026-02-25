import type { FindOne } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { CH_COLLECTIONS, chFindOne, VERSIONS_COLLECTIONS, versionFindOne } from './analytics.js'
import { THINGS_COLLECTION, thingsFindOne } from './things.js'

export const findOne: FindOne = async function findOne(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  if (CH_COLLECTIONS.has(collection)) {
    return chFindOne(this._service, collection, this.context, where) as any
  }

  const versionConfig = VERSIONS_COLLECTIONS.get(collection)
  if (versionConfig) {
    return versionFindOne(this._service, versionConfig, where) as any
  }

  // Things = universal view (no type filter) — keep existing SQL path
  if (collection === THINGS_COLLECTION) {
    return thingsFindOne(this._service, this.namespace, where) as any
  }

  // Standard collections: single compound call (slug→type + query + migration-on-read)
  return this._service.payloadFindOne(this.namespace, collection, where) as any
}
