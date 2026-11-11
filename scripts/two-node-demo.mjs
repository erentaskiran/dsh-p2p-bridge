#!/usr/bin/env node
/**
 * Two fleets, two processes, one topic: the end-to-end check that a single-process test cannot make.
 *
 * `test/e2e.test.js` runs both sides inside one process, which proves the framing, the trust gate and
 * the ack handshake but not that two *separate* DSH hosts, each with its own `DSH_HOME`, identity and
 * state folder, find each other and exchange a message. This script does that: it builds two throwaway
 * homes under a temp directory, trusts each side's key in the other's peer table, starts one child
 * process per fleet, sends from A, and prints what B received and how it was woken.
 *
 *   node scripts/two-node-demo.mjs                     # local, both fleets on this machine
 *   node scripts/two-node-demo.mjs --keep              # keep the temp homes for inspection
 *   P2P_REJECT_SENDER=1 node scripts/two-node-demo.mjs # negative control: receiver withholds trust
 *
 * Exit code 0 means B stored exactly one message and its agent was woken with a preview notice.
 * With `P2P_REJECT_SENDER=1` the receiver leaves the sender `pending` instead, and the run must fail:
 * the message arrives and is dropped, which is the trust gate doing its job on the real wire.
 */
import { spawn } from 'node:child_process'
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomBytes } from 'node:crypto'

const HERE = dirname(fileURLToPath(import.meta.url))
const ROOT = join(HERE, '..')
const PLUGIN = join(ROOT, 'index.js')
const SECRET = randomBytes(16).toString('hex')
const keep = process.argv.includes('--keep')
const base = mkdtempSync(join(tmpdir(), 'p2p-two-node-'))

/**
 * One fleet in its own child process, with its own DSH_HOME.
 *
 * The role decides what it does after connecting: `sender` sends one message and reports the tool
 * result plus the outbox drain; `receiver` reports what reached its inbox and how it was woken.
 */
const CHILD = `
import { apply } from ${JSON.stringify(PLUGIN)}

const role = process.env.P2P_ROLE
const dir = process.env.P2P_DIR
const wakes = []
const tools = new Map()
const ctx = {
  logger: () => ({ warn: (...args) => { if (process.env.P2P_DEBUG === '1') console.error('[warn]', role, ...args.map(String)) } }),
  tools: { register: (tool) => tools.set(tool.name, tool) },
  agents: { get: (id) => (id === 'session-demo' ? { status: 'idle', followup: (m) => wakes.push(m), steer: (m) => wakes.push(m) } : undefined) },
  systemPrompt: { getSectionOrder: () => 10, section: () => {} },
  effect: (install) => { ctx._cleanup = install() },
  session: { id: 'session-demo' },
}
const api = apply(ctx, { dir, enabled: true, fleetSecretEnv: 'P2P_SECRET', wakeOnMessage: true })

const status = await tools.get('p2p_status').execute({})
console.log(JSON.stringify({ event: 'ready', role, fingerprint: status.fingerprint }))

const wait = async (check, ms, what) => {
  const started = Date.now()
  while (!(await check())) {
    if (Date.now() - started > ms) throw new Error('timeout waiting for ' + what + ' (' + role + ', ' + ms + 'ms)')
    await new Promise((r) => setTimeout(r, 100))
  }
}

try {
  if (role === 'sender') {
    // Wait for a socket whose remote key we have learned, which is what makes addressing it possible.
    // The tool payload carries no keys by design, so this harness resolves the key from the store.
    const trustedLive = () => api.store.peers.list().filter((p) => p.status === 'trusted' && api.transport.livePeers().includes(p.publicKey))
    await wait(() => trustedLive().length > 0, 45000, 'a live trusted peer')
    const peer = trustedLive()[0]
    if (peer === undefined) throw new Error('no live trusted peer')
    const sent = await tools.get('p2p_send').execute({ to: peer.fingerprint, text: 'hello across two fleets' })
    console.log(JSON.stringify({ event: 'sent', to: peer.fingerprint, ...sent }))
    // An ack that was lost is retried on the refresh ladder, which can take a DHT round trip; give the
    // ladder room to finish rather than declaring failure while the queue is still making progress.
    await wait(() => api.store.outbox.size === 0, 90000, 'the ack drain')
    console.log(JSON.stringify({ event: 'acked', outbox: api.store.outbox.size }))
  } else {
    await wait(() => wakes.length > 0, 45000, 'the first message')
    const inbox = await tools.get('p2p_inbox').execute({ limit: 5 })
    console.log(JSON.stringify({
      event: 'received',
      messages: inbox.messages.map((m) => ({ text: m.text, fromLabel: m.fromLabel, read: m.read })),
      wakes: wakes.length,
      wakeText: wakes[0]?.content?.[0]?.text,
      wakeSourceKind: wakes[0]?.source?.kind,
      stats: api.stats,
    }))
  }
} catch (error) {
  // A failed wait must not look like a clean exit: print why, and exit non-zero.
  const outbox = api.store.outbox.all().map((row) => ({ id: row.id?.slice(0, 8), attempts: row.attempts, undelivered: row.undelivered, to: String(row.envelope?.to).slice(0, 8) }))
  console.log(JSON.stringify({ event: 'error', role, message: error.message, stats: api.stats, wakes: wakes.length, outboxStates: outbox, livePeers: api.transport.livePeers().length }))
  await api.transport.stop()
  process.exit(1)
}
await api.transport.stop()
process.exit(0)
`

/** Creates both homes, so each side's key is trusted in the other's table before they meet. */
async function buildHomes() {
  const { loadIdentity } = await import(join(ROOT, 'lib/identity.js'))
  const homes = {}
  for (const role of ['sender', 'receiver']) {
    const home = join(base, role)
    const dir = join(home, 'p2p')
    mkdirSync(dir, { recursive: true })
    homes[role] = { home, dir, identity: loadIdentity({ dir, label: `demo-${role}` }) }
  }
  // Trust each side's key in the other's peer table: the same shape `bin/p2p-trust.js` writes.
  for (const [role, home] of Object.entries(homes)) {
    const other = role === 'sender' ? homes.receiver : homes.sender
    writeFileSync(join(home.dir, 'peers.json'), `${JSON.stringify({
      version: 1,
      peers: [{
        publicKey: other.identity.publicKey,
        fingerprint: other.identity.fingerprint,
        label: `demo-${role === 'sender' ? 'receiver' : 'sender'}`,
        status: process.env.P2P_REJECT_SENDER === '1' && role === 'receiver' ? 'pending' : 'trusted',
        firstSeen: new Date().toISOString(),
        lastSeen: new Date().toISOString(),
      }],
    }, null, 2)}\n`, { mode: 0o600 })
  }
  return homes
}

function run(home, role) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', CHILD], {
    env: { ...process.env, DSH_HOME: home.home, P2P_DIR: home.dir, P2P_SECRET: SECRET, P2P_ROLE: role },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const events = []
  const errors = []
  child.stdout.on('data', (chunk) => {
    for (const line of chunk.toString().split('\n')) {
      if (line.trim().length === 0) continue
      try {
        events.push(JSON.parse(line))
      } catch {
        errors.push(line)
      }
    }
  })
  child.stderr.on('data', (chunk) => errors.push(chunk.toString()))
  const done = new Promise((resolve) => child.on('exit', (code) => resolve(code)))
  return { child, events, errors, done }
}

const homes = await buildHomes()
const receiver = run(homes.receiver, 'receiver')
const sender = run(homes.sender, 'sender')
const codes = await Promise.all([receiver.done, sender.done])

const report = {
  secret: `${SECRET.slice(0, 4)}… (not printed in full)`,
  homes: Object.fromEntries(Object.entries(homes).map(([role, h]) => [role, { home: h.home, fingerprint: h.identity.fingerprint }])),
  sender: sender.events,
  receiver: receiver.events,
  exitCodes: { sender: codes[0], receiver: codes[1] },
  stderr: [...sender.errors, ...receiver.errors].filter((line) => line.trim().length > 0).slice(0, 6),
}
console.log(JSON.stringify(report, null, 2))

const received = receiver.events.find((event) => event.event === 'received')
const acked = sender.events.some((event) => event.event === 'acked' && event.outbox === 0)
const woke = typeof received?.wakeText === 'string' && received.wakeText.startsWith('[P2P] mesaj:')
const ok = received?.messages?.length === 1
  && received.messages[0].text === 'hello across two fleets'
  && woke
  && acked
  && received.wakeSourceKind === 'dsh-p2p-bridge'
if (!keep) rmSync(base, { recursive: true, force: true })
console.log(ok ? '\nTWO-NODE E2E: PASS' : '\nTWO-NODE E2E: FAIL')
process.exit(ok ? 0 : 1)
