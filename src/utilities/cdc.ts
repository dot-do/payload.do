/**
 * CDC event builder — ensures event shape matches the ClickHouse `events` table
 * and its materialized views (streams.versions, streams.data, etc.)
 *
 * streams.versions extracts from the `data` JSON column:
 *   ev.data.type, ev.data.id, ev.data.name, ev.data.content, ev.data.code, ev.data.visibility
 *
 * The events table expects: actor (JSON), data (JSON), meta (JSON)
 */

import { UAParser } from 'ua-parser-js'
import { isbot } from 'isbot'

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

export function ulid(): string {
  let str = ''
  let ts = Date.now()
  for (let i = 9; i >= 0; i--) {
    str = ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  for (let i = 0; i < 16; i++) {
    str += ENCODING[Math.floor(Math.random() * 32)]
  }
  return str
}

/** Rich actor identity — columnar JSON for easy filtering by org, user, ip, asn, etc. */
export interface CdcActor {
  // Authenticated identity
  id?: string
  name?: string
  email?: string
  orgId?: string
  // Network
  ip?: string
  asn?: number
  asOrganization?: string
  clientTcpRtt?: number
  // Geo
  city?: string
  region?: string
  regionCode?: string
  country?: string
  continent?: string
  postalCode?: string
  metroCode?: string
  latitude?: number
  longitude?: number
  timezone?: string
  isEU?: boolean
  // UA (parsed)
  ua?: string
  browser?: string
  browserVersion?: string
  os?: string
  osVersion?: string
  device?: string
  deviceVendor?: string
  deviceModel?: string
  engine?: string
  // Bot detection (isbot + CF Bot Management enterprise)
  bot?: boolean
  botScore?: number
  botVerified?: boolean
  botCorporateProxy?: boolean
  botStaticResource?: boolean
  botDetectionIds?: number[]
  // TLS fingerprint
  ja3?: string
  ja4?: string
  tlsVersion?: string
  // CF edge
  colo?: string
  httpProtocol?: string
  requestPriority?: string
  clientAcceptEncoding?: string
  [key: string]: unknown
}

export interface CdcEventOptions {
  /** Entity ID (e.g. "contact_abc123") */
  id: string
  /** Namespace context URL (e.g. "https://headless.ly/~platform") */
  ns: string
  /** CDC event name (e.g. "contacts.created") */
  event: string
  /** Entity type (e.g. "Contact") */
  entityType: string
  /** Entity data (the actual fields — name, email, etc.) */
  entityData: Record<string, unknown>
  /** Actor identity — full object with id, name, email, orgId, ip, asn, etc. */
  actor?: CdcActor
  /** Source identifier (defaults to "platform") */
  source?: string
  /** URL context */
  url?: string
  /** Extra metadata (schema version, commit SHA, etc.) */
  meta?: Record<string, unknown>
}

/**
 * Build actor from a Request — extracts all available edge context.
 *
 * Identity (id, name, email, orgId) comes from the `id-org-ai` header if present.
 * Geo/network (ip, asn, city, country, etc.) comes from request.cf.
 * UA (browser, os, device) comes from ua-parser-js.
 * Bot detection from isbot + CF botManagement (enterprise: score, verifiedBot, ja3/ja4, detectionIds).
 */
export function actorFromRequest(request: Request): CdcActor {
  const cf = (request.cf ?? {}) as Record<string, unknown>
  const actor: CdcActor = {}

  // --- Network ---
  const ip = request.headers.get('cf-connecting-ip')
  if (ip) actor.ip = ip
  if (cf.asn) actor.asn = cf.asn as number
  if (cf.asOrganization) actor.asOrganization = cf.asOrganization as string
  if (typeof cf.clientTcpRtt === 'number') actor.clientTcpRtt = cf.clientTcpRtt

  // --- Geo (full CF enterprise set) ---
  if (cf.city) actor.city = cf.city as string
  if (cf.region) actor.region = cf.region as string
  if (cf.regionCode) actor.regionCode = cf.regionCode as string
  if (cf.country) actor.country = cf.country as string
  if (cf.continent) actor.continent = cf.continent as string
  if (cf.postalCode) actor.postalCode = cf.postalCode as string
  if (cf.metroCode) actor.metroCode = cf.metroCode as string
  if (cf.latitude) actor.latitude = parseFloat(cf.latitude as string)
  if (cf.longitude) actor.longitude = parseFloat(cf.longitude as string)
  if (cf.timezone) actor.timezone = cf.timezone as string
  if (cf.isEUCountry) actor.isEU = cf.isEUCountry === '1'

  // --- UA parsing (ua-parser-js) ---
  const rawUA = request.headers.get('user-agent')
  if (rawUA) {
    actor.ua = rawUA
    const parsed = new UAParser(rawUA).getResult()
    if (parsed.browser.name) actor.browser = parsed.browser.name
    if (parsed.browser.version) actor.browserVersion = parsed.browser.version
    if (parsed.os.name) actor.os = parsed.os.name
    if (parsed.os.version) actor.osVersion = parsed.os.version
    if (parsed.device.type) actor.device = parsed.device.type
    if (parsed.device.vendor) actor.deviceVendor = parsed.device.vendor
    if (parsed.device.model) actor.deviceModel = parsed.device.model
    if (parsed.engine.name) actor.engine = parsed.engine.name

    // Bot detection (isbot UA regex + CF botManagement)
    actor.bot = isbot(rawUA)
  }

  const botMgmt = cf.botManagement as Record<string, unknown> | undefined
  if (botMgmt) {
    if (typeof botMgmt.score === 'number') actor.botScore = botMgmt.score
    if (typeof botMgmt.verifiedBot === 'boolean') actor.botVerified = botMgmt.verifiedBot
    if (typeof botMgmt.corporateProxy === 'boolean') actor.botCorporateProxy = botMgmt.corporateProxy
    if (typeof botMgmt.staticResource === 'boolean') actor.botStaticResource = botMgmt.staticResource
    if (Array.isArray(botMgmt.detectionIds) && botMgmt.detectionIds.length > 0) actor.botDetectionIds = botMgmt.detectionIds as number[]
    if (botMgmt.ja3Hash) actor.ja3 = botMgmt.ja3Hash as string
    if (botMgmt.ja4) actor.ja4 = botMgmt.ja4 as string
  }

  // --- TLS ---
  if (cf.tlsVersion) actor.tlsVersion = cf.tlsVersion as string

  // --- CF edge ---
  if (cf.colo) actor.colo = cf.colo as string
  if (cf.httpProtocol) actor.httpProtocol = cf.httpProtocol as string
  if (cf.requestPriority) actor.requestPriority = cf.requestPriority as string
  if (cf.clientAcceptEncoding) actor.clientAcceptEncoding = cf.clientAcceptEncoding as string

  // --- Authenticated identity (from id-org-ai header, set by auth snippet) ---
  const identity = request.headers.get('id-org-ai')
  if (identity) {
    try {
      const bin = atob(identity)
      const bytes = new Uint8Array(bin.length)
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i)
      const claims = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>
      if (claims.id) actor.id = claims.id as string
      if (claims.name) actor.name = claims.name as string
      if (claims.email) actor.email = claims.email as string
      if (claims.orgId) actor.orgId = claims.orgId as string
    } catch {
      // Invalid header — skip identity fields
    }
  }

  return actor
}

export function buildCdcEvent(opts: CdcEventOptions): Record<string, unknown> {
  return {
    id: ulid(),
    ns: opts.ns,
    ts: new Date().toISOString(),
    type: 'cdc',
    event: opts.event,
    source: opts.source ?? 'platform',
    url: opts.url ?? opts.ns,
    actor: opts.actor ?? {},
    data: {
      type: opts.entityType,
      id: opts.id,
      ...opts.entityData,
    },
    meta: opts.meta ?? {},
  }
}
