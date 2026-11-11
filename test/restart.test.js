/**
 * The replay window across a restart: an id accepted by one process must be refused by the next.
 *
 * This is the one property that cannot be checked in a single process — a `SeenSet` that keeps its ids
 * in memory passes every in-process test and still forgets everything when the harness restarts, which
 * is exactly the gap this file closes.
 *
 * The negative control lives here too: with persistence off (`P2P_DISABLE_SEEN_SAVE=1` on the child, or
 * `--no-save` below) the second process must accept the same id, and the test is written so that the
 * failure is the assertion node naming the replay, not a timeout.
 */
import assert from 'node:assert/strict'
import test from 'node:test'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createIdentity } from '../lib/identity.js'
import { open as openEnvelope, seal } from '../lib/protocol.js'
import { SeenSet } from '../lib/inbox.js'

const PLUGIN_ROOT = dirname(dirname(fileURLToPath(import.meta.url)))

/**
 * One short-lived process: loads the window from `dir`, tries to accept `envelope`, prints what happened.
 *
 * `--no-save` turns persistence off inside the child, which is the negative control: the same id must
 * then be accepted a second time.
 */
const CHILD = `
import { open } from ${JSON.stringify(join(PLUGIN_ROOT, 'lib/protocol.js'))}
import { SeenSet } from ${JSON.stringify(join(PLUGIN_ROOT, 'lib/inbox.js'))}
import { loadIdentity } from ${JSON.stringify(join(PLUGIN_ROOT, 'lib/identity.js'))}

const dir = process.argv[1]
const identity = loadIdentity({ dir, label: 'restart-receiver' })
const seen = new SeenSet({ dir })
if (process.env.P2P_DISABLE_SEEN_SAVE === '1') seen.save = () => {}
const envelope = JSON.parse(process.env.P2P_ENVELOPE)
try {
  open(envelope, { selfPublicKey: identity.publicKey, seen })
  console.log(JSON.stringify({ accepted: true, size: seen.size }))
} catch (error) {
  console.log(JSON.stringify({ accepted: false, code: error.code ?? 'unknown', message: error.message, size: seen.size }))
}
`

function runChild(dir, envelope) {
  const done = spawnSync(process.execPath, ['--input-type=module', '-e', CHILD, dir], {
    env: { ...process.env, P2P_ENVELOPE: JSON.stringify(envelope) },
    encoding: 'utf8',
  })
  const line = (done.stdout || '').trim().split('\n').pop()
  assert.ok(line, `child printed nothing (stderr: ${done.stderr?.slice(0, 300)})`)
  return JSON.parse(line)
}

/** An envelope signed for the receiver that owns `dir`, exactly as the wire would carry it. */
async function receiverIn(dir) {
  const { loadIdentity } = await import(join(PLUGIN_ROOT, 'lib/identity.js'))
  return loadIdentity({ dir, label: 'restart-receiver' })
}

function envelopeFor(receiver) {
  const sender = createIdentity('restart-sender')
  return seal(sender, { kind: 'msg', to: receiver.publicKey, body: { text: 'once only', replyTo: null } })
}

test('an id accepted before a restart is refused after it', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-restart-'))
  try {
    const receiver = await receiverIn(dir)
    const envelope = envelopeFor(receiver)

    const first = runChild(dir, envelope)
    assert.equal(first.accepted, true, 'the first process should accept a fresh id')
    assert.ok(existsSync(join(dir, 'seen.json')), 'the accepted id must be written to seen.json')

    const stored = JSON.parse(readFileSync(join(dir, 'seen.json'), 'utf8'))
    assert.equal(Object.keys(stored.ids).length, 1)
    assert.equal(Object.keys(stored.ids)[0], envelope.id)

    const second = runChild(dir, envelope)
    assert.equal(second.accepted, false, 'a second process must refuse the same id')
    assert.equal(second.code, 'replay')
    assert.match(second.message, /already seen inside the replay window/)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('negative control: with persistence off, the second process accepts it again', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-restart-neg-'))
  try {
    const receiver = await receiverIn(dir)
    const envelope = envelopeFor(receiver)

    // Save is off in BOTH processes here, which is the pre-fix world: nothing is written, so the second
    // process starts with an empty window. If `seen.save()` were still missing, the main test above
    // would pass on a coincidence, and this one proves the difference rather than assuming it.
    process.env.P2P_DISABLE_SEEN_SAVE = '1'
    const first = runChild(dir, envelope)
    assert.equal(first.accepted, true)
    assert.equal(existsSync(join(dir, 'seen.json')), false, 'with save off nothing is persisted')

    const second = runChild(dir, envelope)
    assert.equal(second.accepted, true, 'without persistence the window is empty again, which is the bug')
  } finally {
    delete process.env.P2P_DISABLE_SEEN_SAVE
    rmSync(dir, { recursive: true, force: true })
  }
})

test('a replay inside one process is still refused', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-inproc-'))
  try {
    const receiver = await receiverIn(dir)
    const seen = new SeenSet({ dir })
    const envelope = envelopeFor(receiver)
    openEnvelope(envelope, { selfPublicKey: receiver.publicKey, seen })
    assert.throws(
      () => openEnvelope(envelope, { selfPublicKey: receiver.publicKey, seen }),
      (error) => error.code === 'replay',
    )
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('the window forgets an id after 24 hours', () => {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-ttl-'))
  try {
    const seen = new SeenSet({ dir, ttlHours: 24 })
    seen.add('old-id', Date.now())
    assert.equal(seen.has('old-id'), true)
    assert.equal(seen.has('old-id', Date.now() + 25 * 60 * 60 * 1000), false)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})
