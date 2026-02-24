/**
 * Auxiliary worker that stubs the EVENTS_PIPELINE binding.
 *
 * Pipeline bindings expose a send() method, but in tests we receive
 * fetch requests from service binding calls. Stores events for inspection.
 *
 * Endpoints:
 *   POST /send   — Accept pipeline events
 *   GET  /events — Return stored events
 *   DELETE /events — Reset stored events (test isolation)
 */

let storedEvents = []

export default {
  async fetch(request) {
    const url = new URL(request.url)

    if (request.method === 'POST') {
      try {
        const body = await request.json()
        const events = Array.isArray(body) ? body : [body]
        storedEvents.push(...events)
        return Response.json({ ok: true, received: events.length })
      } catch {
        return Response.json({ ok: false, error: 'invalid body' }, { status: 400 })
      }
    }

    if (request.method === 'GET' && url.pathname === '/events') {
      return Response.json({ events: storedEvents, total: storedEvents.length })
    }

    if (request.method === 'DELETE' && url.pathname === '/events') {
      const count = storedEvents.length
      storedEvents = []
      return Response.json({ ok: true, cleared: count })
    }

    return Response.json({ ok: true })
  },
}
