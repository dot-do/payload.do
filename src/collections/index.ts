import type { CollectionConfig } from 'payload'
import { Thing } from './Thing.js'
import { Event } from './Event.js'
import { Noun } from './Noun.js'
import { Verb } from './Verb.js'
import { Action } from './Action.js'

export { Thing, Event, Noun, Verb, Action }

/**
 * Built-in meta-collections for the .do platform.
 *
 * @param options.hidden - Hide collections from the Payload admin sidebar (default: false)
 */
export function doPayloadCollections(options?: { hidden?: boolean }): CollectionConfig[] {
  const collections = [Thing, Event, Noun, Verb, Action]

  if (options?.hidden) {
    return collections.map((c) => ({
      ...c,
      admin: { ...c.admin, hidden: true },
    }))
  }

  return collections
}
