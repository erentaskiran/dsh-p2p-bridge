/**
 * The retry ladder, driven directly instead of raced against a timer.
 *
 * The earlier version of this check slept and hoped: it passed three runs in four because the assertion
 * depended on where a 2 s interval happened to land. `apply()` now takes `flushIntervalMs` and returns
 * `flushOutbox`, so these tests call the exact function the timer calls and assert what it did. No sleeps,
 * no race, same answer every run.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { apply } from '../index.js'
import { createIdentity, fingerprint } from '../lib/identity.js'

/** A plugin instance with the timer pushed out of the way, so the test owns the only flush. */
function harness({ retryAttempts = 3 } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-flush-'))
  const tools = new Map()
  const ctx = {
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: { get: () => undefined },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: () => {},
    session: { id: 'session-flush' },
  }
  const api = apply(ctx, { dir, enabled: false, flushIntervalMs: 10_000_000, retryAttempts })
  const identity = JSON.parse(readFileSync(join(dir, 'identity.json'), 'utf8'))
  const peer = createIdentity('peer')
  const peerFingerprint = fingerprint(peer.publicKey)
  api.store.peers.observe({ publicKey: peer.publicKey, fingerprint: peerFingerprint })
  api.store.peers.trust(peerFingerprint)
  return { api, identity, peer, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

test('one flush writes a queued message and counts the attempt', () => {
  const h = harness()
  try {
    h.api.store.outbox.append({
      id: 'm1',
      kind: 'msg',
      envelope: { id: 'm1', kind: 'msg', to: h.peer.publicKey, body: { text: 'x' } },
      attempts: 0,
      undelivered: false,
    })
    const wrote = []
    h.api.transport.sendTo = (_publicKey, envelope) => { wrote.push(envelope.id); return true }
    h.api.flushOutbox()
    const rows = h.api.store.outbox.all()
    assert.deepEqual(wrote, ['m1'], 'the queued message was written once')
    assert.equal(rows.length, 1, 'a message stays queued until its ack')
    assert.equal(rows[0].attempts, 1, 'and its attempt count moved')
  } finally {
    h.cleanup()
  }
})

test('one flush clears an ack that was written', () => {
  const h = harness()
  try {
    h.api.store.outbox.append({
      id: 'a1',
      kind: 'ack',
      envelope: { id: 'a1', kind: 'ack', to: h.peer.publicKey, body: { replyTo: 'm0' } },
      attempts: 0,
      undelivered: false,
    })
    h.api.transport.sendTo = () => true
    h.api.flushOutbox()
    assert.equal(h.api.store.outbox.size, 0, 'an ack leaves the queue once written')
  } finally {
    h.cleanup()
  }
})

test('a message that never gets its ack is marked undelivered, not dropped', () => {
  const h = harness({ retryAttempts: 2 })
  try {
    h.api.store.outbox.append({
      id: 'm2',
      kind: 'msg',
      envelope: { id: 'm2', kind: 'msg', to: h.peer.publicKey, body: { text: 'x' } },
      attempts: 0,
      undelivered: false,
    })
    const writes = []
    h.api.transport.sendTo = (_publicKey, envelope) => { writes.push(envelope.id); return true }
    // One pass both writes the row and applies the ladder; the writes are the proof that the pass did its
    // flush half, so deleting that half makes this fail rather than pass by luck.
    for (let round = 0; round < 4; round += 1) h.api.retryPass()
    assert.ok(writes.length >= 3, `the flush kept retrying (wrote ${writes.length} time(s))`)
    const rows = h.api.store.outbox.all()
    assert.equal(rows.length, 1, 'the row is kept, not silently dropped')
    assert.equal(rows[0].undelivered, true, 'and it is marked undelivered so status counts it')
    assert.equal(h.api.store.outbox.all().filter((row) => row.undelivered === true).length, 1)
  } finally {
    h.cleanup()
  }
})

test('a flush with no socket leaves the row alone and does not count a failed attempt as progress', () => {
  const h = harness()
  try {
    h.api.store.outbox.append({
      id: 'm3',
      kind: 'msg',
      envelope: { id: 'm3', kind: 'msg', to: h.peer.publicKey, body: { text: 'x' } },
      attempts: 0,
      undelivered: false,
    })
    h.api.transport.sendTo = () => false
    h.api.flushOutbox()
    const rows = h.api.store.outbox.all()
    assert.equal(rows.length, 1)
    assert.equal(rows[0].attempts, 0, 'a write that did not happen is not an attempt')
  } finally {
    h.cleanup()
  }
})
