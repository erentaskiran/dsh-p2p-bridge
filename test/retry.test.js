/**
 * The two paths that keep a delivered message from getting stuck, tested where they actually run.
 *
 * `reacknowledge` and the ack queue were both added on the strength of a 50-run measurement, and then
 * went unproven: disabling either left every suite green. These tests drive the real plugin and assert
 * the behaviour, so removing the guard makes one of them fail.
 *
 *   1. a duplicate of a *delivered* message is acked again, so the sender's retry can terminate;
 *   2. a duplicate of a message this node never delivered is not acked, because there is nothing to
 *      confirm and acking an unknown id would confirm a message that does not exist here;
 *   3. an ack that could not be written is queued, not dropped, and is visible to the status.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'

import { apply } from '../index.js'
import { createIdentity, fingerprint as fingerprintOf, sign } from '../lib/identity.js'
import { seal, signingBytes } from '../lib/protocol.js'

const SECRET = randomBytes(16).toString('hex')
const TURN_MS = 60_000

/** One fleet on a fake Host context; `swarm` decides whether it reaches the network. */
function fleet(tag, { offline = false } = {}) {
  const dir = mkdtempSync(join(tmpdir(), `p2p-retry-${tag}-`))
  const tools = new Map()
  const ctx = {
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: { get: () => undefined },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: (install) => {
      ctx._cleanup = install()
    },
    session: { id: 'session-retry' },
  }
  // An offline fleet gets a swarm that never emits a connection: the plugin still completes `apply`,
  // registers its tools and runs its delivery path, which is exactly the state a message arrives in
  // when the socket has already dropped.
  const config = {
    dir,
    enabled: !offline,
    fleetSecretEnv: 'TEST_SECRET',
    wakeOnMessage: false,
    autoStart: !offline,
    ...(offline
      ? { swarmFactory: () => ({ join: () => ({}), leave: () => {}, on: () => {}, destroy: async () => {} }) }
      : {}),
  }
  const api = apply(ctx, config)
  const identity = JSON.parse(readFileSync(join(dir, 'identity.json'), 'utf8'))
  return { dir, tools, api, identity, cleanup: () => { ctx._cleanup?.(); rmSync(dir, { recursive: true, force: true }) } }
}

async function until(check, { timeout = 20_000, label = 'condition' } = {}) {
  const started = Date.now()
  for (;;) {
    if (await check()) return
    if (Date.now() - started > timeout) throw new Error(`timed out waiting for ${label} after ${timeout}ms`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Trusts each fleet in the other's peer table, the rows `bin/p2p-trust.js` would write. */
function trustEachOther(a, b) {
  for (const [left, right] of [[a, b], [b, a]]) {
    const print = fingerprintOf(right.identity.publicKey)
    left.api.store.peers.observe({ publicKey: right.identity.publicKey, fingerprint: print })
    left.api.store.peers.trust(print)
    left.api.store.peers.save()
  }
}

/** A signed `msg` with a chosen id, so a test can replay it. */
function signedMessage(senderIdentity, toPublicKey, id, text) {
  const envelope = { v: 1, id, kind: 'msg', from: senderIdentity.publicKey, to: toPublicKey, ts: new Date().toISOString(), body: { text, replyTo: null } }
  envelope.sig = sign(senderIdentity, signingBytes(envelope))
  return envelope
}

test('a duplicate of a delivered message is acked again', { timeout: TURN_MS }, async () => {
  process.env.TEST_SECRET = SECRET
  const a = fleet('dup-a')
  const b = fleet('dup-b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'both connected' })

    const sent = await a.tools.get('p2p_send').execute({ to: fingerprintOf(b.identity.publicKey), text: 'once' })
    await until(() => b.api.store.inbox.all().length === 1, { label: 'B stores the message' })
    await until(() => a.api.store.outbox.size === 0, { label: "A's outbox drains" })
    // Clear B's queue too: the ack row asserted below has to come from the replay, not the first delivery.
    b.api.store.outbox.clear()

    // The sender's retry goes the same way the original did: A re-sends the identical envelope to B.
    // B already stored this id, so it hits the replay rule - and must answer with an ack anyway.
    const duplicate = signedMessage(a.identity, b.identity.publicKey, sent.messageId, 'once')
    assert.equal(a.api.transport.sendTo(b.identity.publicKey, duplicate), true, 'the duplicate must reach B')

    await until(() => b.api.store.outbox.all().some((entry) => entry.kind === 'ack'), {
      label: 'the duplicate is re-acked',
      timeout: 15_000,
    })
    assert.equal(b.api.store.inbox.all().length, 1, 'a duplicate must not be stored twice')
  } finally {
    a.cleanup()
    b.cleanup()
  }
})

test('a duplicate of a message that was never delivered is not acked', { timeout: TURN_MS }, async () => {
  process.env.TEST_SECRET = SECRET
  const a = fleet('nodup-a')
  const b = fleet('nodup-b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'both connected' })

    // A well-formed id that A never stored. A's replay window rejects the duplicate; there is nothing
    // to confirm, so no ack may appear.
    const unknownId = '00000000-0000-4000-8000-000000000000'
    const duplicate = signedMessage(a.identity, b.identity.publicKey, unknownId, 'never delivered')
    a.api.store.seen.add(unknownId)
    a.api.store.seen.save()

    assert.equal(b.api.transport.sendTo(a.identity.publicKey, duplicate), true)
    await new Promise((resolve) => setTimeout(resolve, 2000))
    assert.equal(a.api.store.outbox.all().some((entry) => entry.kind === 'ack'), false, 'nothing was delivered, so no ack')
  } finally {
    a.cleanup()
    b.cleanup()
  }
})

test('an ack that cannot be written is queued and counted, not lost', () => {
  const b = fleet('ackq', { offline: true })
  try {
    const sender = createIdentity('ackq-sender')
    const print = fingerprintOf(sender.publicKey)
    b.api.store.peers.observe({ publicKey: sender.publicKey, fingerprint: print })
    b.api.store.peers.trust(print)

    // Deliver one message through the same entry point the transport uses; with no socket the ack
    // cannot be written, so it has to land in the outbox.
    const envelope = seal(sender, { kind: 'msg', to: b.identity.publicKey, body: { text: 'please ack', replyTo: null } })
    b.api.transport.onEnvelope(envelope, { uid: 0, hex: 'offline', socket: { destroyed: true } })

    const rows = b.api.store.outbox.all()
    assert.equal(b.api.store.inbox.all().length, 1, 'the message itself is stored')
    assert.ok(rows.some((row) => row.kind === 'ack'), 'the unsendable ack is queued')
    assert.equal(rows.every((row) => row.undelivered !== true), true, 'it is not given up on yet')
  } finally {
    b.cleanup()
  }
})

test('a queued ack is retried on the periodic flush when the socket recovers', { timeout: TURN_MS }, async () => {
  // The case the timer exists for: the ack could not be written (the socket died at that instant), the
  // peer stays connected, and nothing else would move that row - `confirmAcksTo` only fires when the peer
  // talks first. Without the periodic flush the row sits in the outbox forever and "drained" is
  // unreachable, which is one of the two ways the demo timed out.
  process.env.TEST_SECRET = `${SECRET}-flush`
  const a = fleet('flush-a')
  const b = fleet('flush-b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'both connected' })

    // Swallow exactly the ack B tries to write, then let the socket work again.
    const realSend = b.api.transport.sendTo
    const written = []
    b.api.transport.sendTo = (publicKey, envelope) => {
      if (envelope?.kind === 'ack') return false
      return realSend.call(b.api.transport, publicKey, envelope)
    }

    await a.tools.get('p2p_send').execute({ to: fingerprintOf(b.identity.publicKey), text: 'flush me' })
    await until(() => b.api.store.inbox.all().length === 1, { label: 'B stores the message' })
    assert.ok(b.api.store.outbox.all().some((row) => row.kind === 'ack'), 'the ack is queued while the write fails')

    // Socket healthy again; nobody speaks, so only the timer can retry the queued ack.
    b.api.transport.sendTo = (publicKey, envelope) => {
      written.push(envelope?.kind)
      return realSend.call(b.api.transport, publicKey, envelope)
    }
    await until(() => b.api.store.outbox.all().every((row) => row.kind !== 'ack'), {
      label: 'the periodic flush clears the queued ack',
      timeout: 20_000,
    })
    assert.ok(written.includes('ack'), `the flush wrote the ack (saw: ${written.join(',') || 'nothing'})`)
    // And the sender agrees: its message is confirmed.
    await until(() => a.api.store.outbox.size === 0, { label: "A's message is confirmed" })
  } finally {
    a.cleanup()
    b.cleanup()
  }
})
