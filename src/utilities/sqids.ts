import Sqids from 'sqids'

const BLOCKED_WORDS = ['ass', 'damn', 'fuck', 'shit', 'hell', 'crap', 'dick', 'porn', 'sex', 'cum']

const sqids = new Sqids({
  minLength: 5,
  blocklist: new Set(BLOCKED_WORDS),
})

export function encodeSqid(tenantNum: number, rowid: number, c: number): string {
  return sqids.encode([tenantNum, rowid, c])
}

export function decodeSqid(sqid: string): { tenantNum: number; rowid: number; c: number } {
  const [tenantNum, rowid, c] = sqids.decode(sqid)
  return { tenantNum, rowid, c }
}

export function generateSid(prefix: string, tenantNum: number, rowid: number, c: number): string {
  const sqid = encodeSqid(tenantNum, rowid, c)
  return `${prefix}_${sqid}`
}

export function parseSid(sid: string): { collection: string; tenantNum: number; rowid: number; c: number } {
  const underscoreIndex = sid.lastIndexOf('_')
  const collection = sid.slice(0, underscoreIndex)
  const sqid = sid.slice(underscoreIndex + 1)
  return { collection, ...decodeSqid(sqid) }
}
