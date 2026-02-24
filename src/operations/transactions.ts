import type { DoPayloadAdapter } from '../types.js'

// Durable Objects are single-threaded — transactions are inherently atomic.
// These are no-ops since the DO handles concurrency via its write mutex.

export async function beginTransaction(this: DoPayloadAdapter): Promise<string | number | null> {
  return null
}

export async function commitTransaction(this: DoPayloadAdapter): Promise<void> {}

export async function rollbackTransaction(this: DoPayloadAdapter): Promise<void> {}
