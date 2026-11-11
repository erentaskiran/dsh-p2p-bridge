/**
 * Two real swarms in one process: A sends, B receives and acks, A's outbox drains. Then the negative
 * control the spec demands — an untrusted peer's message is dropped and the inbox stays empty.
 *
 * Both nodes run the real plugin (`apply`) against a throwaway `~/.dsh/p2p` directory and the real
 * hyperswarm transport on a random topic, so nothing here is a stand-in: if the framing, the
 * signature check or the outbox handshake broke, this fails.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, rmSync, readFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'

import { apply } from '../index.js'

const SECRET = randomBytes(16).toString('hex')
const TURN_MS = 60_000

/** One fleet: the plugin on a fake Host context, with everything a test needs to inspect. */
function fleet(tag) {
  const dir = mkdtempSync(join(tmpdir(), `p2p-${tag}-`))
  const tools = new Map()
  const wakes = []
  const ctx = {
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: {
      get: (id) => (id === 'session-test'
        ? { status: 'idle', followup: (message) => wakes.push(message), steer: (message) => wakes.push(message) }
        : undefined),
    },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: (install) => {
      ctx._cleanup = install()
    },
    session: { id: 'session-test' },
  }
  const api = apply(ctx, { dir, enabled: true, fleetSecretEnv: 'TEST_SECRET', wakeOnMessage: true, autoStart: true })
  return { dir, tools, wakes, ctx, api, cleanup: () => { ctx._cleanup?.(); rmSync(dir, { recursive: true, force: true }) } }
}

/** Waits until `check()` is truthy, polling without a fixed sleep in the assertion path. */
// See the note in retry.test.js: connection setup is DHT timing, so a slow pair must not look like a bug.
async function until(check, { timeout = 60_000, label = 'condition' } = {}) {
  const started = Date.now()
  for (;;) {
    if (await check()) return
    if (Date.now() - started > timeout) throw new Error(`timed out waiting for ${label} after ${timeout}ms`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** The fingerprint of one fleet's identity file, which is also its address on the wire. */
function fingerprintOf(fleetNode) {
  const identity = JSON.parse(readFileSync(join(fleetNode.dir, 'identity.json'), 'utf8'))
  return { publicKey: identity.publicKey, fingerprint: fleetNode.api.identity.fingerprint }
}

test('A sends, B receives and acks, and A\'s outbox drains', { timeout: TURN_MS }, async () => {
  process.env.TEST_SECRET = SECRET
  const a = fleet('a')
  const b = fleet('b')
  try {
    const aId = fingerprintOf(a)
    const bId = fingerprintOf(b)

    // Trust is a person's decision, so the table is edited directly here, exactly like `p2p_trust` would.
    a.api.store.peers.observe({ publicKey: bId.publicKey, fingerprint: bId.fingerprint, label: 'fleet-b' })
    a.api.store.peers.trust(bId.fingerprint)
    b.api.store.peers.observe({ publicKey: aId.publicKey, fingerprint: aId.fingerprint, label: 'fleet-a' })
    b.api.store.peers.trust(aId.fingerprint)

    await until(() => a.api.transport.peerCount > 0 && b.api.transport.peerCount > 0, { label: 'both swarms connected' })

    const sent = await a.tools.get('p2p_send').execute({ to: bId.fingerprint, text: 'hello from A' })
    assert.equal(sent.peerStatus, 'trusted')

    await until(async () => (await b.tools.get('p2p_inbox').execute({})).messages.length === 1, { label: 'B receives one message' })
    const inbox = await b.tools.get('p2p_inbox').execute({})
    assert.equal(inbox.messages[0].text, 'hello from A')
    assert.equal(inbox.messages[0].read, false)

    // B acked on delivery; A's outbox must now be empty, and the message must never have been queued.
    await until(() => a.api.store.outbox.size === 0, { label: "A's outbox drains" })
    assert.equal(a.api.store.outbox.size, 0)

    // The wake notice is a preview, not the body.
    assert.equal(b.wakes.length, 1)
    const notice = b.wakes[0].content[0].text
    assert.match(notice, /^\[P2P\] mesaj: fleet-a \([0-9a-f]{16}\) — "hello from A" — p2p_inbox ile oku$/)
    assert.equal(b.wakes[0].source.kind, 'dsh-p2p-bridge')
  } finally {
    a.cleanup()
    b.cleanup()
  }
})

test('an untrusted peer\'s message is dropped and the inbox stays empty', { timeout: TURN_MS }, async () => {
  process.env.TEST_SECRET = SECRET
  const a = fleet('a2')
  const b = fleet('b2')
  try {
    const aId = fingerprintOf(a)
    const bId = fingerprintOf(b)

    // B knows A is on the topic, but nobody trusted A: A stays pending on B's side.
    a.api.store.peers.observe({ publicKey: bId.publicKey, fingerprint: bId.fingerprint, label: 'fleet-b' })
    a.api.store.peers.trust(bId.fingerprint)

    await until(() => a.api.transport.peerCount > 0 && b.api.transport.peerCount > 0, { label: 'both swarms connected' })

    await a.tools.get('p2p_send').execute({ to: bId.fingerprint, text: 'you should not see this' })

    // B has seen the key, recorded it as pending, and delivered nothing. Waiting on the arrival counter
    // rather than on an empty file is what makes this a real negative control: without the trust check
    // the same wait succeeds and the assertions below fail.
    await until(() => b.api.store.peers.list().length === 1, { label: 'B records the pending peer' })
    await until(() => b.api.stats.dropped > 0, { label: 'B drops the untrusted message' })
    assert.equal(b.api.stats.received, 1, 'the message did arrive; it was the trust check that stopped it')
    assert.equal(b.api.stats.delivered, 0)
    const pending = b.api.store.peers.list()[0]
    assert.equal(pending.publicKey, aId.publicKey)
    assert.equal(pending.status, 'pending')

    const inboxFile = join(b.dir, 'inbox.jsonl')
    assert.equal(existsSync(inboxFile) ? readFileSync(inboxFile, 'utf8').trim() : '', '', 'inbox.jsonl must stay empty')
    assert.equal((await b.tools.get('p2p_inbox').execute({})).messages.length, 0)
    assert.equal(b.wakes.length, 0, 'an untrusted peer must not wake the agent')
  } finally {
    a.cleanup()
    b.cleanup()
  }
})

test('p2p_send refuses a pending peer and refuses broadcast', { timeout: TURN_MS }, async () => {
  process.env.TEST_SECRET = SECRET
  const a = fleet('a3')
  const b = fleet('b3')
  try {
    const bId = fingerprintOf(b)
    a.api.store.peers.observe({ publicKey: bId.publicKey, fingerprint: bId.fingerprint, label: 'fleet-b' })

    await assert.rejects(
      () => a.tools.get('p2p_send').execute({ to: bId.fingerprint, text: 'nope' }),
      /peer not trusted: run p2p_trust <fingerprint>/,
    )
    await assert.rejects(
      () => a.tools.get('p2p_send').execute({ to: '*', text: 'nope' }),
      /broadcast is v2/,
    )
  } finally {
    a.cleanup()
    b.cleanup()
  }
})
