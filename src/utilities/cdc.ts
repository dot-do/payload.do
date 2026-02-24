/**
 * CDC event builder — ensures event shape matches the ClickHouse `events` table
 * and its materialized views (streams.versions, streams.data, etc.)
 *
 * streams.versions extracts from the `data` JSON column:
 *   ev.data.type, ev.data.id, ev.data.name, ev.data.content, ev.data.code, ev.data.visibility
 *
 * The events table expects: actor (JSON), data (JSON), meta (JSON)
 *
 * Actor is the raw request.cf object spread with ip/ua from headers, plus
 * authenticated identity from the id-org-ai header. All cf fields (geo,
 * network, botManagement, TLS, colo, etc.) are preserved as-is for
 * ClickHouse JSON column queries.
 */

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

/** Actor identity — raw cf object + ip/ua + authenticated identity */
export interface CdcActor {
  id?: string
  name?: string
  email?: string
  orgId?: string
  ip?: string
  ua?: string
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
 * Build actor from a Request — spreads raw request.cf for full edge context.
 *
 * All CF fields (geo, network, botManagement, TLS, colo, etc.) are preserved
 * as-is in the raw cf object. Identity from the `id-org-ai` header is merged in.
 */
export function actorFromRequest(request: Request): CdcActor {
  const actor: CdcActor = {
    ...((request.cf ?? {}) as Record<string, unknown>),
    ip: request.headers.get('cf-connecting-ip') ?? undefined,
    ua: request.headers.get('user-agent') ?? undefined,
  }

  // Authenticated identity (from id-org-ai header, set by auth snippet)
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
    ray: '',
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
