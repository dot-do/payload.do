import type { Count } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { CH_COLLECTIONS, chCount, VERSIONS_COLLECTIONS, versionCount } from './analytics.js'
import { THINGS_COLLECTION } from './things.js'

export const count: Count = async function count(this: DoPayloadAdapter, args) {
  const { collection, where } = args

  if (CH_COLLECTIONS.has(collection)) {
    const totalDocs = await chCount(this._service, collection, this.context, where)
    return { totalDocs }
  }

  const versionConfig = VERSIONS_COLLECTIONS.get(collection)
  if (versionConfig) {
    const totalDocs = await versionCount(this._service, versionConfig, where)
    return { totalDocs }
  }

  // Things = universal view (no type filter)
  if (collection === THINGS_COLLECTION) {
    return this._service.payloadThingsCount(this.namespace, where)
  }

  // Standard collections: single compound call
  return this._service.payloadCount(this.namespace, collection, where)
}
