/**
 * The guards must be in the source, not just in behaviour.
 *
 * A negative control means editing production code until a test goes red - and then putting it back. Twice
 * today that "putting it back" was missed, leaving `if (false)` where a signature check belongs in the
 * working tree. A test that cannot pass while a guard is disabled is the cheapest structural fix: it reads
 * the source and fails loudly, so a forgotten revert is caught by the suite rather than by a reviewer.
 *
 * The negative controls themselves belong in a throwaway copy of the tree (a separate worktree), never in
 * the one that gets committed. This test is the backstop for when that habit slips.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)))

function read(path) {
  return readFileSync(join(ROOT, path), 'utf8')
}

const SOURCES = ['index.js', 'lib/protocol.js', 'lib/transport.js', 'lib/inbox.js', 'lib/wake.js', 'lib/identity.js']

test('no source file disables a guard with `if (false)`', () => {
  for (const path of SOURCES) {
    const text = read(path)
    assert.equal(
      /if\s*\(\s*false\s*\)/.test(text),
      false,
      `${path} contains \`if (false)\`: a negative control was left enabled. Revert it before committing.`,
    )
  }
})

test('every guard the acceptance table names is present in the source', () => {
  const guards = [
    ['lib/protocol.js', 'if (!verifySignature(envelope.from, signingBytes(envelope), envelope.sig)) {', 'signature check'],
    ['lib/protocol.js', 'seen.save()', 'replay persistence'],
    ['index.js', "if (sender.status !== 'trusted') {", 'trust gate'],
    ['index.js', 'function acknowledge(envelope) {', 'ack handling'],
    ['index.js', 'flushOutbox()', 'flush call'],
    ['lib/transport.js', 'this.dropStaleSockets()', 'stale-socket drop'],
    ['lib/transport.js', 'this.pingQuietPeers()', 'liveness ping'],
  ]
  for (const [path, needle, what] of guards) {
    assert.ok(read(path).includes(needle), `${path} lost the ${what} (looked for \`${needle}\`)`)
  }
})

test('replay persistence has no environment switch', () => {
  // `if (process.env.NC_OFF !== '1') seen.save()` was a test hook that shipped in production code: one
  // variable switched the replay window off. A test replaces `seen.save` on its own copy instead.
  for (const path of SOURCES) {
    assert.equal(/process\.env\.NC_OFF/.test(read(path)), false, `${path} checks NC_OFF: no test hook belongs in production`)
  }
})
