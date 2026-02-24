import type { QueryDrafts } from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { find } from './find.js'

export const queryDrafts = async function queryDrafts(this: DoPayloadAdapter, args: any) {
  const { collection, limit = 10, page = 1, pagination = true, sort, where } = args

  return find.call(this, {
    collection,
    limit,
    page,
    pagination,
    sort,
    where: {
      ...where,
      _status: { equals: 'draft' },
    },
    req: args.req,
  })
} as QueryDrafts
