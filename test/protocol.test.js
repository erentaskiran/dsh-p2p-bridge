/**
 * The envelope rules, one test per rejection.
 *
 * Every case here is a message the plugin must refuse before an agent ever sees it: a wrong version,
 * a missing or forged signature, a body over the size cap, an id replayed inside the window, and a
 * message addressed to another node. The positive case at the top exists so a rejection cannot pass
 * just because everything is rejected.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { createIdentity, sign, verify } from '../lib/identity.js'
import {
  canonical,
  createFramer,
  MAX_MESSAGE_BYTES,
  open,
  PROTOCOL_VERSION,
  ProtocolError,
  seal,
  signingBytes,
  topicFor,
} from '../lib/protocol.js'
import { SeenSet } from '../lib/inbox.js'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const sender = createIdentity('sender')
const receiver = createIdentity('receiver')

function sealed(overrides = {}) {
  const envelope = seal(sender, { kind: 'msg', to: receiver.publicKey, body: { text: 'hello', replyTo: null } })
  return { ...envelope, ...overrides }
}

function freshSeen() {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-seen-'))
  return { seen: new SeenSet({ dir }), dir }
}

test('a well-formed envelope opens', () => {
  const { seen, dir } = freshSeen()
  try {
    const opened = open(sealed(), { selfPublicKey: receiver.publicKey, seen })
    assert.equal(opened.kind, 'msg')
    assert.equal(opened.body.text, 'hello')
    assert.equal(opened.from, sender.publicKey)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('a different protocol version is rejected', () => {
  assert.throws(
    () => open(sealed({ v: 2 }), { selfPublicKey: receiver.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'version' && /unsupported protocol version/.test(error.message),
  )
})

test('a missing signature is rejected', () => {
  const envelope = sealed()
  delete envelope.sig
  assert.throws(
    () => open(envelope, { selfPublicKey: receiver.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'signature' && /no sig/.test(error.message),
  )
})

test('a forged signature is rejected', () => {
  const envelope = sealed({ body: { text: 'tampered', replyTo: null } })
  assert.throws(
    () => open(envelope, { selfPublicKey: receiver.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'signature' && /does not verify/.test(error.message),
  )
})

test('a body over the 64 KB cap is rejected', () => {
  const big = sealed({ body: { text: 'x'.repeat(MAX_MESSAGE_BYTES), replyTo: null } })
  // Re-sign so the size is the only thing wrong with it.
  delete big.sig
  big.sig = sign(sender, signingBytes(big))
  assert.throws(
    () => open(big, { selfPublicKey: receiver.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'oversize',
  )
})

test('a replayed id inside the window is rejected', () => {
  const { seen, dir } = freshSeen()
  try {
    const envelope = sealed()
    open(envelope, { selfPublicKey: receiver.publicKey, seen })
    assert.throws(
      () => open(envelope, { selfPublicKey: receiver.publicKey, seen }),
      (error) => error instanceof ProtocolError && error.code === 'replay' && /already seen/.test(error.message),
    )
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('an id whose window expired is accepted again', () => {
  const { seen, dir } = freshSeen()
  try {
    const envelope = sealed()
    const later = Date.now() + 25 * 60 * 60 * 1000
    open(envelope, { selfPublicKey: receiver.publicKey, seen })
    assert.equal(open(envelope, { selfPublicKey: receiver.publicKey, seen, now: later }).id, envelope.id)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('a message addressed to another node is rejected', () => {
  const stranger = createIdentity('stranger')
  assert.throws(
    () => open(sealed(), { selfPublicKey: stranger.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'recipient' && /another node/.test(error.message),
  )
})

test('an unknown kind is rejected', () => {
  const envelope = sealed({ kind: 'shout' })
  delete envelope.sig
  envelope.sig = sign(sender, signingBytes(envelope))
  assert.throws(
    () => open(envelope, { selfPublicKey: receiver.publicKey }),
    (error) => error instanceof ProtocolError && error.code === 'kind',
  )
})

test('the canonical form sorts keys, so both sides sign the same bytes', () => {
  const a = { b: 1, a: { d: 2, c: [3, { f: 4, e: 5 }] } }
  const b = { a: { c: [3, { e: 5, f: 4 }], d: 2 }, b: 1 }
  assert.equal(canonical(a), canonical(b))
  assert.equal(signingBytes({ ...a, sig: 'ignored' }).toString(), canonical(a))
})

test('the topic is 32 bytes and depends on the whole secret', () => {
  assert.equal(topicFor('secret-a').length, 32)
  assert.notDeepEqual(topicFor('secret-a'), topicFor('secret-b'))
  assert.throws(() => topicFor(''), /fleet secret is empty/)
})

test('the framer splits NDJSON and rejects an oversize line before parsing', () => {
  const accepted = []
  const rejected = []
  const framer = createFramer({
    maxBytes: 128,
    onMessage: (value) => accepted.push(value),
    onError: (error) => rejected.push(error.code),
  })
  framer.push('{"a":1}\n{"b":')
  framer.push('2}\n')
  assert.deepEqual(accepted, [{ a: 1 }, { b: 2 }])
  assert.deepEqual(rejected, [])

  const big = createFramer({ maxBytes: 128, onMessage: () => rejected.push('accepted'), onError: (error) => rejected.push(error.code) })
  big.push(`${'x'.repeat(200)}\n`)
  assert.deepEqual(rejected, ['oversize'])
})

test('a signature verifies only against the matching key', () => {
  const bytes = Buffer.from('signed bytes')
  const signature = sign(sender, bytes)
  assert.equal(verify(sender.publicKey, bytes, signature), true)
  assert.equal(verify(receiver.publicKey, bytes, signature), false)
  assert.equal(verify(sender.publicKey, Buffer.from('other bytes'), signature), false)
})

test('the protocol version this suite signs for is 1', () => {
  assert.equal(PROTOCOL_VERSION, 1)
  assert.equal(sealed().v, 1)
})
