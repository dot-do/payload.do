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

const ULID_ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

/**
 * Generate a ULID lower bound for a given timestamp.
 * Uses the timestamp portion (10 Crockford Base32 chars) + 16 zeros.
 * This produces the lexicographically smallest ULID for that millisecond,
 * allowing ClickHouse to use the primary key (ORDER BY id) for time range queries.
 */
function ulidLowerBound(timestampMs: number): string {
  let str = ''
  let ts = timestampMs
  for (let i = 9; i >= 0; i--) {
    str = ULID_ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  return str + '0000000000000000'
}

/**
 * Extract the timestamp from a ULID string (first 10 Crockford Base32 chars → epoch ms).
 * Returns an ISO string.
 */
function ulidToTimestamp(ulid: string): string {
  let ts = 0
  for (let i = 0; i < 10; i++) {
    ts = ts * 32 + ULID_ENCODING.indexOf(ulid[i].toUpperCase())
  }
  return new Date(ts).toISOString()
}

/** Default lookback for version queries (90 days). Keeps partition scans bounded. */
const DEFAULT_VERSION_LOOKBACK_DAYS = 90

/** ClickHouse-backed implementation that reads version events from the events table. */
export class ClickHouseVersionStore implements VersionStore {
  private service: PayloadDatabaseService

  constructor(service: PayloadDatabaseService) {
    this.service = service
  }

  async findVersions(args: { context: string; type: string; id?: string; limit?: number; page?: number; sort?: string }) {
    const { context, type, id, limit = 10, page = 1, sort } = args
    const offset = (page - 1) * limit
    // Use id (ULID, time-sorted) for ordering — it's the table's ORDER BY key
    const dir = sort?.startsWith?.('-') ? 'DESC' : 'ASC'

    // Use ULID lower bound instead of ts column to leverage primary key (ORDER BY id)
    const timeLowerBound = ulidLowerBound(Date.now() - DEFAULT_VERSION_LOOKBACK_DAYS * 86_400_000)

    const params: Record<string, string | number> = { ctx: context, evt: `${type}.versioned`, time_lb: timeLowerBound }
    let whereClauses = `ns = {ctx:String} AND event = {evt:String} AND id >= {time_lb:String}`

    if (id) {
      whereClauses += ` AND data.id.:String = {parentId:String}`
      params.parentId = id
    }

    // Run data + count queries in parallel (single round trip)
    const dataSql = `SELECT id, data FROM events WHERE ${whereClauses} ORDER BY id ${dir} LIMIT {lim:UInt32} OFFSET {off:UInt32}`
    const countSql = `SELECT count() as total FROM events WHERE ${whereClauses}`
    params.lim = limit
    params.off = offset

    try {
      const [result, countResult] = await Promise.all([
        this.service.chQuery(dataSql, params),
        this.service.chQuery(countSql, params) as Promise<{ data: { total: number }[] }>,
      ])

      const docs: VersionDoc[] = result.data.map((row) => {
        const rawData = row.data
        const versionData = typeof rawData === 'string' ? JSON.parse(rawData) : (rawData ?? {})
        const version = (versionData._version ?? 1) as number
        const ts = ulidToTimestamp(row.id as string)
        return {
          id: `${row.id}_v${version}`,
          parent: row.id as string,
          version: versionData,
          createdAt: ts,
          updatedAt: ts,
        }
      })

      const totalDocs = countResult.data[0]?.total ?? 0

      return { docs, totalDocs }
    } catch (err) {
      console.error('[versions] findVersions failed:', err)
      return { docs: [], totalDocs: 0 }
    }
  }

  async countVersions(args: { context: string; type: string; id?: string }) {
    const { context, type, id } = args

    // Use ULID lower bound instead of ts column to leverage primary key (ORDER BY id)
    const timeLowerBound = ulidLowerBound(Date.now() - DEFAULT_VERSION_LOOKBACK_DAYS * 86_400_000)

    let sql = `SELECT count() as total FROM events WHERE ns = {ctx:String} AND event = {evt:String} AND id >= {time_lb:String}`
    const params: Record<string, string | number> = { ctx: context, evt: `${type}.versioned`, time_lb: timeLowerBound }
    if (id) {
      sql += ` AND data.id.:String = {parentId:String}`
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
