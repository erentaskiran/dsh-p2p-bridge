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

// Connecting two swarms is the DHT's timing, not this file's behaviour: when the whole suite runs several
// pairs at once, 20 s was not enough (measured once in fifty suite runs, `run-44`, both retry tests timing
// out on `both connected`). The budget is generous so a slow DHT is a delay, not a false failure.
async function until(check, { timeout = 60_000, label = 'condition' } = {}) {
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

    // Watch what B writes from here on. A written ack does not enter the outbox (there is nothing left for
    // it to prove), so the wire is where a re-ack is observable.
    const acksWritten = []
    const realSend = b.api.transport.sendTo
    b.api.transport.sendTo = (publicKey, envelope) => {
      if (envelope?.kind === 'ack' && envelope.body?.replyTo === sent.messageId) acksWritten.push(envelope.id)
      return realSend.call(b.api.transport, publicKey, envelope)
    }

    // The sender's retry travels the same way the original did: A re-sends the identical envelope to B.
    // B already stored this id, so it hits the replay rule - and must answer with an ack anyway.
    const duplicate = signedMessage(a.identity, b.identity.publicKey, sent.messageId, 'once')
    assert.equal(a.api.transport.sendTo(b.identity.publicKey, duplicate), true, 'the duplicate must reach B')

    await until(() => acksWritten.length > 0, { label: 'the duplicate is re-acked', timeout: 15_000 })
    b.api.transport.sendTo = realSend
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
  // The case the timer exists for: the ack could not be written (the socket died at that instant), the peer
  // stays connected, and nothing else would move that row - `confirmAcksTo` only fires when the peer talks
  // first. Without the periodic flush the row sits in the outbox forever and "drained" is unreachable.
  process.env.TEST_SECRET = `${SECRET}-flush`
  const a = fleet('flush-a')
  const b = fleet('flush-b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'both connected' })

    // Every ack B tries to write fails, so the ack has to be queued.
    const realSend = b.api.transport.sendTo
    b.api.transport.sendTo = (publicKey, envelope) => {
      if (envelope?.kind === 'ack') return false
      return realSend.call(b.api.transport, publicKey, envelope)
    }

    await a.tools.get('p2p_send').execute({ to: fingerprintOf(b.identity.publicKey), text: 'flush me' })
    await until(() => b.api.store.inbox.all().length === 1, { label: 'B stores the message' })
    assert.ok(b.api.store.outbox.all().some((row) => row.kind === 'ack'), 'the ack is queued while the write fails')

    // The socket works again and nobody speaks, so only the timer can move that row.
    const written = []
    b.api.transport.sendTo = (publicKey, envelope) => {
      written.push(envelope?.kind)
      return realSend.call(b.api.transport, publicKey, envelope)
    }
    await until(() => written.includes('ack'), { label: 'the periodic flush retried the ack', timeout: 20_000 })
    await until(() => b.api.store.outbox.all().every((row) => row.kind !== 'ack'), {
      label: 'the retried ack leaves the queue',
      timeout: 20_000,
    })
    await until(() => a.api.store.outbox.size === 0, { label: "A's message is confirmed" })
  } finally {
    a.cleanup()
    b.cleanup()
  }
})

test('a message queued before the peer was verified is retried once a socket exists', { timeout: TURN_MS }, async () => {
  // The residue the probe reported in 4 of 20 runs: a message queued while `livePeers()` was still empty,
  // then never retried because the flush required the peer to be live. `sendTo` already decides whether a
  // socket exists, so the flush must not ask the question twice.
  process.env.TEST_SECRET = `${SECRET}-preverify`
  const a = fleet('pre-a')
  const b = fleet('pre-b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'both connected' })

    // Queue a message with no socket available, exactly like a first write that landed in a dying socket.
    const peer = a.api.store.peers.byFingerprint(fingerprintOf(b.identity.publicKey))
    const realSend = a.api.transport.sendTo
    const realLive = a.api.transport.livePeers.bind(a.api.transport)
    a.api.transport.sendTo = () => false
    a.api.transport.livePeers = () => []
    const sent = await a.tools.get('p2p_send').execute({ to: peer.fingerprint, text: 'queued before verify' })
    assert.equal(sent.queued, true, 'it is queued while no write can succeed')
    // The socket is usable again, but `livePeers()` still reports nothing - the state a message queued
    // before verification sits in. Only a flush that does not re-ask that question can move it.
    a.api.transport.sendTo = realSend
    setTimeout(() => { a.api.transport.livePeers = realLive }, 25_000)

    // No new envelope from B arrives; the periodic flush alone has to move the row.
    await until(() => a.api.store.outbox.all().every((row) => row.id !== sent.messageId), {
      label: 'the queued message is retried and acked',
      timeout: 20_000,
    })
    await until(() => b.api.store.inbox.all().length === 1, { label: 'B receives it' })
  } finally {
    a.cleanup()
    b.cleanup()
  }
})
