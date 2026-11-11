/**
 * Fifty messages over one established connection: the reliability question that is ours to own.
 *
 * The two-process demo measured two different things at once - can two fleets find each other (the DHT's
 * answer, slow and racy), and does a delivered message get delivered exactly once with a working ack
 * (our answer). Conflating them hid the second behind the first. This file asks only the second: connect
 * once, then send fifty messages down that one connection and require all fifty to arrive, be acked, and
 * wake the agent exactly once each.
 *
 * If this fails, the bug is ours. If it passes and the two-process demo still loses runs, the loss is in
 * connection setup, which is a different report.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'

import { apply } from '../index.js'
import { fingerprint as fingerprintOf } from '../lib/identity.js'

const SECRET = randomBytes(16).toString('hex')
const MESSAGES = 50
const TURN_MS = 180_000

function fleet(tag) {
  const dir = mkdtempSync(join(tmpdir(), `p2p-steady-${tag}-`))
  const tools = new Map()
  const wakes = []
  const ctx = {
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: {
      get: (id) => (id === 'session-steady'
        ? { status: 'idle', followup: (message) => wakes.push(message), steer: (message) => wakes.push(message) }
        : undefined),
    },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: (install) => {
      ctx._cleanup = install()
    },
    session: { id: 'session-steady' },
  }
  const api = apply(ctx, { dir, enabled: true, fleetSecretEnv: 'STEADY_SECRET', wakeOnMessage: true, autoStart: true })
  const identity = JSON.parse(readFileSync(join(dir, 'identity.json'), 'utf8'))
  return { dir, tools, wakes, api, identity, cleanup: () => { ctx._cleanup?.(); rmSync(dir, { recursive: true, force: true }) } }
}

async function until(check, { timeout = 30_000, label = 'condition' } = {}) {
  const started = Date.now()
  for (;;) {
    if (await check()) return
    if (Date.now() - started > timeout) throw new Error(`timed out waiting for ${label} after ${timeout}ms`)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

function trustEachOther(a, b) {
  for (const [left, right] of [[a, b], [b, a]]) {
    const print = fingerprintOf(right.identity.publicKey)
    left.api.store.peers.observe({ publicKey: right.identity.publicKey, fingerprint: print })
    left.api.store.peers.trust(print)
    left.api.store.peers.save()
  }
}

test('fifty messages over one connection all arrive, ack and wake exactly once', { timeout: TURN_MS }, async () => {
  process.env.STEADY_SECRET = SECRET
  const a = fleet('a')
  const b = fleet('b')
  try {
    trustEachOther(a, b)
    await until(() => a.api.transport.livePeers().length > 0 && b.api.transport.livePeers().length > 0, { label: 'one connection' })

    const to = fingerprintOf(b.identity.publicKey)
    for (let index = 1; index <= MESSAGES; index += 1) {
      const sent = await a.tools.get('p2p_send').execute({ to, text: `message ${index}` })
      assert.equal(sent.sent, true, `message ${index} should leave on the open connection`)
      // Each message is confirmed before the next one starts, so a stuck ack shows up as its own failure
      // rather than as a final count that is one short.
      await until(() => !a.api.store.outbox.all().some((row) => row.id === sent.messageId), {
        label: `message ${index} to be acked`,
        timeout: 15_000,
      })
    }

    await until(() => b.api.store.inbox.all().length === MESSAGES, { label: 'all fifty stored' })
    const inbox = b.api.store.inbox.all()
    assert.equal(inbox.length, MESSAGES, 'exactly one inbox row per message')
    assert.equal(new Set(inbox.map((row) => row.id)).size, MESSAGES, 'no duplicate ids')
    assert.deepEqual(
      inbox.map((row) => row.text).sort((x, y) => Number(x.split(' ')[1]) - Number(y.split(' ')[1])),
      Array.from({ length: MESSAGES }, (_, index) => `message ${index + 1}`),
      'every message body arrived, in order',
    )
    assert.equal(b.wakes.length, MESSAGES, 'exactly one wake per message')
    assert.equal(a.api.store.outbox.size, 0, 'the sender queued nothing that is still unconfirmed')
    assert.equal(a.api.store.outbox.all().filter((row) => row.undelivered === true).length, 0, 'nothing was given up on')

    const status = await a.tools.get('p2p_status').execute({})
    assert.equal(status.undelivered, 0)
    assert.equal(status.queued, 0)
  } finally {
    a.cleanup()
    b.cleanup()
  }
})
