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

test('an unverified socket is dropped after the stale window', async () => {
  // `dropStaleSockets` earns its place by measurement: with it the 50-run demo was 50/50, and without it
  // one run in fifty never got a live peer. This pins the behaviour, so deleting the call fails here
  // instead of showing up as a rare flake.
  const h = harness()
  try {
    const destroyed = []
    const fakeSocket = { destroyed: false, writable: true, destroy() { this.destroyed = true; destroyed.push(1) }, on() {}, setNoDelay() {} }
    h.api.transport.swarm = { leave() {}, join: () => ({ flushed: async () => {} }) }
    h.api.transport.topic = Buffer.alloc(32)
    const peer = h.api.transport.accept(fakeSocket, { publicKey: Buffer.alloc(32, 7) })
    assert.equal(h.api.transport.connections.size, 1)

    // Not stale yet.
    assert.equal(h.api.transport.dropStaleSockets(6000), 0)
    assert.equal(h.api.transport.connections.size, 1)

    // Older than the window and still unverified: it goes, and the topic is refreshed so a clean dial can
    // happen. Driven through the keep-alive body, so deleting the call site fails here too.
    peer.createdAt = Date.now() - 10_000
    h.api.transport.keepAliveTick()
    assert.equal(h.api.transport.connections.size, 0)
    assert.equal(destroyed.length, 1, 'the socket was destroyed by the keep-alive pass')
  } finally {
    h.cleanup()
  }
})

test('a verified socket is never treated as stale', () => {
  const h = harness()
  try {
    const fakeSocket = { destroyed: false, writable: true, destroy() { this.destroyed = true }, on() {}, setNoDelay() {} }
    h.api.transport.accept(fakeSocket, { publicKey: Buffer.alloc(32, 8) })
    const peer = [...h.api.transport.connections.values()][0]
    peer.publicKey = h.peer.publicKey
    peer.createdAt = Date.now() - 60_000
    assert.equal(h.api.transport.dropStaleSockets(6000), 0, 'a peer that answered is left alone')
    assert.equal(h.api.transport.connections.size, 1)
  } finally {
    h.cleanup()
  }
})

test('a peer that stops answering pings is dropped, and one that answers is kept', () => {
  // `pingQuietPeers` asks a different question from "is the socket open": a socket can report writable
  // while the far side has already reset its half, so liveness is "did anybody answer". Fake both peers.
  const h = harness()
  try {
    const silent = { destroyed: false, writable: true, destroy() { this.destroyed = true }, on() {}, setNoDelay() {} }
    const chatty = { destroyed: false, writable: true, destroy() { this.destroyed = true }, on() {}, setNoDelay() {} }
    h.api.transport.swarm = { leave() {}, join: () => ({ flushed: async () => {} }) }
    h.api.transport.topic = Buffer.alloc(32)
    h.api.transport.accept(silent, { publicKey: Buffer.alloc(32, 1) })
    h.api.transport.accept(chatty, { publicKey: Buffer.alloc(32, 2) })
    const [silentPeer, chattyPeer] = [...h.api.transport.connections.values()]
    silentPeer.publicKey = h.peer.publicKey
    chattyPeer.publicKey = 'someone-else'
    h.api.transport.onPing = () => ({ kind: 'ping' })
    h.api.transport.sendOn = () => true

    // Nobody has been heard from, both are quiet: the first pass pings, the third drops.
    h.api.transport.lastHeard.set(h.peer.publicKey, 0)
    h.api.transport.lastHeard.set('someone-else', Date.now())
    // Driven through the keep-alive pass, so deleting the call site fails this test as well.
    const pass = () => h.api.transport.keepAliveTick()
    pass()
    assert.equal(silent.destroyed, false, 'the first quiet pass only pings')
    pass()
    assert.equal(silent.destroyed, false, 'the second still tolerates it')
    pass()
    assert.equal(silent.destroyed, true, 'the silent socket was destroyed')
    assert.equal(chatty.destroyed, false, 'the peer that was heard from is untouched')
  } finally {
    h.cleanup()
  }
})
