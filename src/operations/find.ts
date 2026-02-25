import type { Find, PaginatedDocs } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { buildPagination } from '../utilities/pagination.js'
import { CH_COLLECTIONS, chFind, VERSIONS_COLLECTIONS, versionFind } from './analytics.js'
import { THINGS_COLLECTION } from './things.js'

export const find: Find = async function find(this: DoPayloadAdapter, args) {
  const { collection, where, limit: rawLimit = 10, page = 1, pagination = true, sort } = args

  if (CH_COLLECTIONS.has(collection)) {
    const { docs, totalDocs } = await chFind(this._service, collection, this.context, { where, sort: sort as string | undefined, limit: rawLimit, page, pagination })
    return { docs, ...buildPagination(totalDocs, rawLimit, page) } as PaginatedDocs<any>
  }

  const versionConfig = VERSIONS_COLLECTIONS.get(collection)
  if (versionConfig) {
    const { docs, totalDocs } = await versionFind(this._service, versionConfig, { where, sort: sort as string | undefined, limit: rawLimit, page, pagination })
    return { docs, ...buildPagination(totalDocs, rawLimit, page) } as PaginatedDocs<any>
  }

  // Things = universal view of ALL entities (no type filter)
  if (collection === THINGS_COLLECTION) {
    return this._service.payloadThingsFind(this.namespace, where, sort as string | undefined, rawLimit, page, pagination) as Promise<PaginatedDocs<any>>
  }

  // Standard collections: single compound call (slug→type + query + migration-on-read + pagination)
  return this._service.payloadFind(this.namespace, collection, where, sort as string | undefined, rawLimit, page, pagination) as Promise<PaginatedDocs<any>>
}
