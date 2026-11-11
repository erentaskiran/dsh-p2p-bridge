/**
 * The retry ladder, hammered: 200 fresh instances, four passes each, no timer and no network.
 *
 * `test/flush.test.js` asserts this once per test, and once in fifty full-suite runs one of those runs
 * reported "wrote 0 time(s)" - a path with no socket, no timer and no environment, so a one-in-fifty
 * failure there is a real puzzle rather than a flaky network. Repeating the same work 200 times in one
 * process is the cheapest way to make a rare state show up while the trace in the assertion says what it
 * was: per round, how many writes happened, the attempt count and the undelivered flag.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { apply } from '../index.js'
import { createIdentity, fingerprint } from '../lib/identity.js'

const ROUNDS = 200
const PASSES = 4
const RETRY_ATTEMPTS = 2

function freshInstance() {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-stress-'))
  const tools = new Map()
  const api = apply({
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: { get: () => undefined },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: () => {},
    session: { id: 'session-stress' },
  }, { dir, enabled: false, flushIntervalMs: 10_000_000, retryAttempts: RETRY_ATTEMPTS })
  const peer = createIdentity('stress-peer')
  const print = fingerprint(peer.publicKey)
  api.store.peers.observe({ publicKey: peer.publicKey, fingerprint: print })
  api.store.peers.trust(print)
  return { api, peer, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

test(`${ROUNDS} fresh instances each retry the same way`, { timeout: 120_000 }, () => {
  for (let round = 1; round <= ROUNDS; round += 1) {
    const h = freshInstance()
    try {
      h.api.store.outbox.append({
        id: `m-${round}`,
        kind: 'msg',
        envelope: { id: `m-${round}`, kind: 'msg', to: h.peer.publicKey, body: { text: 'x' } },
        attempts: 0,
        undelivered: false,
      })
      const writes = []
      h.api.transport.sendTo = () => { writes.push(round); return true }
      const trace = []
      for (let pass = 0; pass < PASSES; pass += 1) {
        h.api.retryPass()
        const row = h.api.store.outbox.all()[0]
        trace.push({ pass, writes: writes.length, attempts: row?.attempts, undelivered: row?.undelivered })
      }
      assert.equal(writes.length, RETRY_ATTEMPTS + 1, `round ${round} wrote ${writes.length} time(s); trace=${JSON.stringify(trace)}`)
      const rows = h.api.store.outbox.all()
      assert.equal(rows.length, 1, `round ${round} kept the row`)
      assert.equal(rows[0].undelivered, true, `round ${round} marked it undelivered; trace=${JSON.stringify(trace)}`)
    } finally {
      h.cleanup()
    }
  }
})
