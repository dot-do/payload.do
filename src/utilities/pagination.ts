export interface PaginationMeta {
  totalDocs: number
  limit: number
  totalPages: number
  page: number
  pagingCounter: number
  hasPrevPage: boolean
  hasNextPage: boolean
  prevPage: number | null
  nextPage: number | null
}

/**
 * Build pagination metadata from totalDocs, limit, and current page.
 * When limit is 0 (pagination disabled), all docs on one page.
 */
export function buildPagination(totalDocs: number, limit: number, page: number): PaginationMeta {
  const effectiveLimit = limit > 0 ? limit : totalDocs
  const totalPages = limit > 0 ? Math.ceil(totalDocs / limit) : totalDocs > 0 ? 1 : 0
  const hasNextPage = limit > 0 && page < totalPages
  const hasPrevPage = page > 1

  return {
    totalDocs,
    limit: effectiveLimit,
    totalPages,
    page,
    pagingCounter: (page - 1) * effectiveLimit + 1,
    hasPrevPage,
    hasNextPage,
    prevPage: hasPrevPage ? page - 1 : null,
    nextPage: hasNextPage ? page + 1 : null,
  }
}
