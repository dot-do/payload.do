import type { Find, PaginatedDocs } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { buildPagination } from '../utilities/pagination.js'
import { entityToDocument, slugToType } from '../utilities/transforms.js'
import { resolveNounContext } from '../utilities/noun-cache.js'
import { translateWhere } from '../queries/where.js'
import { translateSort } from '../queries/sort.js'
import { CH_COLLECTIONS, chFind, VERSIONS_COLLECTIONS, versionFind } from './analytics.js'
import { THINGS_COLLECTION, thingsFind } from './things.js'

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
    const { docs, totalDocs } = await thingsFind(this._service, this.namespace, { where, sort: sort as string | string[] | undefined, limit: rawLimit, page, pagination })
    return { docs, ...buildPagination(totalDocs, rawLimit, page) } as PaginatedDocs<any>
  }

  const type = slugToType(collection)
  const filter = translateWhere(where)
  const sortObj = translateSort(sort as string | undefined)
  const limit = pagination ? rawLimit : 0
  const offset = limit > 0 ? (page - 1) * limit : 0

  const result = await this._service.find(this.namespace, type, filter, {
    limit: limit > 0 ? limit : 10000,
    offset,
    sort: sortObj,
  })

  // Resolve NounContext for migration-on-read
  const nounCtx = await resolveNounContext(this._service, this.namespace, collection)
  const docs = result.items.map((entity) => entityToDocument(entity, nounCtx ?? undefined))
  const totalDocs = result.total

  return { docs, ...buildPagination(totalDocs, limit, page) } as PaginatedDocs<any>
}
