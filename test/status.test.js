/**
 * What a tool may tell the model, and what it may never tell it.
 *
 * The rule "the plugin never returns a key, only a fingerprint" was enforced by reading the code and by a
 * hand-written check in a shell; the acceptance table pointed at `test/` for it, which was not true. This
 * is that test: the payload shapes are asserted, so re-adding `publicKey` to either tool turns this red.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { apply } from '../index.js'
import { createIdentity, fingerprint } from '../lib/identity.js'

function harness() {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-status-'))
  const tools = new Map()
  const api = apply({
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: { get: () => undefined },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: () => {},
    session: { id: 'session-status' },
  }, { dir, enabled: false })
  return { api, tools, dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

test('p2p_status carries a fingerprint and never the public key', async () => {
  const h = harness()
  try {
    const status = await h.tools.get('p2p_status').execute({})
    assert.equal('publicKey' in status, false, 'the public key must not appear in a tool result')
    assert.equal(typeof status.fingerprint, 'string')
    assert.equal(status.fingerprint.length, 16)
    assert.equal(status.fingerprint, h.api.identity.fingerprint)
    // Nothing in the payload may contain the raw key, wherever it is nested.
    assert.equal(JSON.stringify(status).includes(h.api.identity.publicKey), false)
  } finally {
    h.cleanup()
  }
})

test('p2p_peers rows carry a fingerprint and never the public key', async () => {
  const h = harness()
  try {
    const peer = createIdentity('known-peer')
    const print = fingerprint(peer.publicKey)
    h.api.store.peers.observe({ publicKey: peer.publicKey, fingerprint: print, label: 'friend' })
    h.api.store.peers.trust(print)

    const { peers } = await h.tools.get('p2p_peers').execute({})
    assert.equal(peers.length, 1)
    assert.deepEqual(Object.keys(peers[0]).sort(), ['fingerprint', 'label', 'lastSeen', 'status'])
    assert.equal(JSON.stringify(peers).includes(peer.publicKey), false, 'the peer key must not appear')
  } finally {
    h.cleanup()
  }
})

test('p2p_inbox rows expose the text and the sender label, not the sender key', async () => {
  const h = harness()
  try {
    const peer = createIdentity('sender-peer')
    const print = fingerprint(peer.publicKey)
    h.api.store.peers.observe({ publicKey: peer.publicKey, fingerprint: print, label: 'friend' })
    h.api.store.inbox.append({ id: 'msg-1', from: peer.publicKey, fromLabel: 'friend', ts: new Date().toISOString(), text: 'hello', read: false })
    const { messages } = await h.tools.get('p2p_inbox').execute({})
    assert.equal(messages.length, 1)
    assert.equal(messages[0].text, 'hello')
    assert.equal(messages[0].fromLabel, 'friend')
    assert.equal(JSON.stringify(messages).includes(peer.publicKey), false, 'the sender key must not appear')
  } finally {
    h.cleanup()
  }
})
