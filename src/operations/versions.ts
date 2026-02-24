import type {
  CountVersions,
  CreateVersion,
  DeleteVersions,
  FindVersions,
  UpdateVersion,
  CreateGlobalVersion,
  FindGlobalVersions,
  UpdateGlobalVersion,
  CountGlobalVersions,
  PaginatedDocs,
} from 'payload'
import type { DoPayloadAdapter } from '../types.js'
import { buildPagination } from '../utilities/pagination.js'
import { buildCdcEvent } from '../utilities/cdc.js'

function emptyPaginated(): PaginatedDocs<any> {
  return { docs: [], ...buildPagination(0, 10, 1) } as PaginatedDocs<any>
}

export const createVersion: CreateVersion = async function createVersion(this: DoPayloadAdapter, args) {
  const { collectionSlug, versionData, parent } = args as any
  const now = new Date().toISOString()
  const version = (versionData?._version ?? 1) as number

  // Version events are stored as CDC events in the events table
  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: parent ?? '',
      ns: this.context,
      event: `${collectionSlug}.versioned`,
      entityType: collectionSlug,
      entityData: versionData ?? {},
    }))
  } catch {
    // fire-and-forget
  }

  return {
    id: `${parent ?? ''}_v${version}`,
    parent: parent ?? '',
    version: versionData ?? {},
    createdAt: now,
    updatedAt: now,
  } as any
}

export const findVersions: FindVersions = async function findVersions(this: DoPayloadAdapter, args) {
  if (!this.versionStore) return emptyPaginated()

  const { collection: collectionSlug, where, limit = 10, page = 1, sort } = args as any
  const id = where?.parent?.equals ?? undefined

  const { docs, totalDocs } = await this.versionStore.findVersions({
    context: this.context,
    type: collectionSlug,
    id,
    limit,
    page,
    sort: typeof sort === 'string' ? sort : '-createdAt',
  })

  return { docs, ...buildPagination(totalDocs, limit, page) } as PaginatedDocs<any>
}

export const updateVersion: UpdateVersion = async function updateVersion(this: DoPayloadAdapter) {
  // Versions are immutable events — no-op
  return {} as any
}

export const deleteVersions: DeleteVersions = async function deleteVersions(this: DoPayloadAdapter) {
  // Events are immutable — no-op
}

export const countVersions: CountVersions = async function countVersions(this: DoPayloadAdapter, args) {
  if (!this.versionStore) return { totalDocs: 0 }

  const { collection: collectionSlug, where } = args as any
  const id = where?.parent?.equals ?? undefined

  const totalDocs = await this.versionStore.countVersions({
    context: this.context,
    type: collectionSlug,
    id,
  })

  return { totalDocs }
}

export const createGlobalVersion: CreateGlobalVersion = async function createGlobalVersion(this: DoPayloadAdapter, args) {
  const { globalSlug, versionData } = args as any
  const now = new Date().toISOString()
  const version = (versionData?._version ?? 1) as number

  try {
    await this._service.sendEvent(this.namespace, buildCdcEvent({
      id: globalSlug,
      ns: this.context,
      event: `_global_${globalSlug}.versioned`,
      entityType: `_global_${globalSlug}`,
      entityData: versionData ?? {},
    }))
  } catch {
    // fire-and-forget
  }

  return {
    id: `${globalSlug}_v${version}`,
    parent: globalSlug,
    version: versionData ?? {},
    createdAt: now,
    updatedAt: now,
  } as any
}

export const findGlobalVersions: FindGlobalVersions = async function findGlobalVersions(this: DoPayloadAdapter, args) {
  if (!this.versionStore) return emptyPaginated()

  const { slug, limit = 10, page = 1, sort } = args as any

  const { docs, totalDocs } = await this.versionStore.findVersions({
    context: this.context,
    type: `_global_${slug}`,
    limit,
    page,
    sort: typeof sort === 'string' ? sort : '-createdAt',
  })

  return { docs, ...buildPagination(totalDocs, limit, page) } as PaginatedDocs<any>
}

export const updateGlobalVersion: UpdateGlobalVersion = async function updateGlobalVersion(this: DoPayloadAdapter) {
  return {} as any
}

export const countGlobalVersions: CountGlobalVersions = async function countGlobalVersions(this: DoPayloadAdapter, args) {
  if (!this.versionStore) return { totalDocs: 0 }

  const { slug } = args as any

  const totalDocs = await this.versionStore.countVersions({
    context: this.context,
    type: `_global_${slug}`,
  })

  return { totalDocs }
}
