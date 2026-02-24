import type { PayloadDatabaseService } from './types.js'

export interface VersionDoc {
  id: string
  parent: string
  version: Record<string, unknown>
  createdAt: string
  updatedAt: string
}

export interface VersionStore {
  findVersions(args: {
    context: string
    type: string
    id?: string
    limit?: number
    page?: number
    sort?: string
  }): Promise<{ docs: VersionDoc[]; totalDocs: number }>
  countVersions(args: { context: string; type: string; id?: string }): Promise<number>
}

/** No-op implementation for when ClickHouse is not configured. */
export class NullVersionStore implements VersionStore {
  async findVersions() {
    return { docs: [] as VersionDoc[], totalDocs: 0 }
  }
  async countVersions() {
    return 0
  }
}

/** ClickHouse-backed implementation that reads version events from the events table. */
export class ClickHouseVersionStore implements VersionStore {
  private service: PayloadDatabaseService

  constructor(service: PayloadDatabaseService) {
    this.service = service
  }

  async findVersions(args: { context: string; type: string; id?: string; limit?: number; page?: number; sort?: string }) {
    const { context, type, id, limit = 10, page = 1, sort } = args
    const offset = (page - 1) * limit
    const dir = sort?.startsWith?.('-') ? 'DESC' : 'ASC'

    let sql = `SELECT id, ts, data FROM events WHERE ns = {ctx:String} AND event = {evt:String}`
    const params: Record<string, string | number> = { ctx: context, evt: `${type}.versioned` }

    if (id) {
      sql += ` AND JSONExtractString(data, 'id') = {parentId:String}`
      params.parentId = id
    }

    sql += ` ORDER BY ts ${dir} LIMIT {lim:UInt32} OFFSET {off:UInt32}`
    params.lim = limit
    params.off = offset

    try {
      const result = await this.service.chQuery(sql, params)

      const docs: VersionDoc[] = result.data.map((row) => {
        const rawData = row.data
        const versionData = typeof rawData === 'string' ? JSON.parse(rawData) : (rawData ?? {})
        const version = (versionData._version ?? 1) as number
        return {
          id: `${row.id}_v${version}`,
          parent: row.id as string,
          version: versionData,
          createdAt: row.ts as string,
          updatedAt: row.ts as string,
        }
      })

      // Get total count
      let countSql = `SELECT count() as total FROM events WHERE ns = {ctx:String} AND event = {evt:String}`
      if (id) countSql += ` AND JSONExtractString(data, 'id') = {parentId:String}`
      const countResult = (await this.service.chQuery(countSql, params)) as { data: { total: number }[] }
      const totalDocs = countResult.data[0]?.total ?? 0

      return { docs, totalDocs }
    } catch (err) {
      console.error('[versions] findVersions failed:', err)
      return { docs: [], totalDocs: 0 }
    }
  }

  async countVersions(args: { context: string; type: string; id?: string }) {
    const { context, type, id } = args
    let sql = `SELECT count() as total FROM events WHERE ns = {ctx:String} AND event = {evt:String}`
    const params: Record<string, string | number> = { ctx: context, evt: `${type}.versioned` }
    if (id) {
      sql += ` AND JSONExtractString(data, 'id') = {parentId:String}`
      params.parentId = id
    }
    try {
      const result = (await this.service.chQuery(sql, params)) as { data: { total: number }[] }
      return result.data[0]?.total ?? 0
    } catch (err) {
      console.error('[versions] countVersions failed:', err)
      return 0
    }
  }
}
