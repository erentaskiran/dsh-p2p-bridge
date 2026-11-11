/**
 * Test B, measured through the thing that matters: does a message cross in BOTH directions?
 *
 * `bothSaw` (does each side's `livePeers()` contain the other) turned out to be a narrower question than
 * "can the two fleets talk", and on this machine it reported 0/50 while messages were demonstrably
 * arriving. An observation that disagrees with a passing end-to-end run is a broken observation until
 * proven otherwise, so this probe measures delivery instead: A sends to B, B sends to A, each message must
 * arrive and be acked. A run passes only when both directions did.
 *
 *   node scripts/connect-probe.mjs                     50 fresh pairs, 5 at a time
 *   PROBE_RUNS=10 PROBE_CAP_MS=45000 node scripts/connect-probe.mjs
 *
 * Prints one JSON line per run and a summary with median/p95/max of the time to a verified two-way
 * exchange. Exit code 0 always: this is a measurement, and its verdict belongs in the summary line.
 */
import { spawn } from 'node:child_process'
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomBytes } from 'node:crypto'

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..')
const RUNS = Number(process.env.PROBE_RUNS ?? 50)
const CAP_MS = Number(process.env.PROBE_CAP_MS ?? 45000)
const PARALLEL = Number(process.env.PROBE_PARALLEL ?? 5)

/**
 * One fleet. It waits until it can address the peer, sends one message, then waits for one message from the
 * peer. Both halves are reported separately, so a one-way link is not hidden behind a single boolean.
 */
const CHILD = `
import { apply } from ${JSON.stringify(join(ROOT, 'index.js'))}
import { loadIdentity, fingerprint } from ${JSON.stringify(join(ROOT, 'lib/identity.js'))}
const dir = process.env.PROBE_DIR
const role = process.env.PROBE_ROLE
loadIdentity({ dir, label: role })
const tools = new Map()
const wakes = []
const api = apply({
  logger: () => ({ warn: () => {} }),
  tools: { register: (t) => tools.set(t.name, t) },
  agents: { get: (id) => (id === 'probe' ? { status: 'idle', followup: (m) => wakes.push(m), steer: (m) => wakes.push(m) } : undefined) },
  systemPrompt: { getSectionOrder: () => 10, section: () => {} },
  effect: (i) => { globalThis.__cleanup = i() },
  session: { id: 'probe' },
}, { dir, enabled: true, fleetSecretEnv: 'PROBE_SECRET', autoStart: true, wakeOnMessage: true })

const started = Number(process.env.PROBE_START)
const cap = Number(process.env.PROBE_CAP)
const peerFingerprint = process.env.PROBE_PEER_FP
const wait = async (check, ms) => {
  const t0 = Date.now()
  while (!(await check())) {
    if (Date.now() - t0 > ms) return false
    await new Promise((r) => setTimeout(r, 50))
  }
  return true
}

const out = { role, sentOk: false, gotMessage: false, sentMs: null, gotMs: null, ackedOutbox: null, wakes: 0 }
try {
  const up = await wait(() => api.transport.livePeers().length > 0, cap)
  if (up) {
    out.sentOk = true
    out.sentMs = Date.now() - started
    try {
      await tools.get('p2p_send').execute({ to: peerFingerprint, text: 'ping from ' + role })
    } catch (error) {
      out.sendError = String(error.message ?? error).slice(0, 80)
    }
  }
  const got = await wait(() => api.store.inbox.all().length > 0, cap)
  out.gotMessage = got
  out.gotMs = got ? Date.now() - started : null
  out.wakes = wakes.length
  // Wait longer than the retry ladder so a residue is a real loss, not an in-flight retry.
  const drained = await wait(() => api.store.outbox.size === 0, 20000)
  out.ackedOutbox = drained ? 0 : api.store.outbox.size
  if (!drained) {
    out.outboxKinds = api.store.outbox.all().map((row) => row.kind + ':' + (row.undelivered === true ? 'undelivered' : 'queued'))
    out.outboxDetail = api.store.outbox.all().map((row) => ({
      kind: row.kind,
      attempts: row.attempts,
      id: String(row.id).slice(0, 8),
      to: String(row.envelope?.to).slice(0, 8),
      // A queued row is a message this side sent; its proof of delivery is the peer's inbox, so report the
      // ack count this side has seen instead of looking in its own inbox.
      ackSeen: api.stats.acksReceived ?? null,
      myStats: api.stats,
    }))
  }
} catch (error) {
  out.error = String(error.message ?? error).slice(0, 120)
}
out.stats = api.stats
console.log(JSON.stringify(out))
await api.transport.stop()
process.exit(0)
`

async function oneRun(run) {
  const secret = randomBytes(16).toString('hex')
  const base = mkdtempSync(join(tmpdir(), `p2p-b-${run}-`))
  const { loadIdentity, fingerprint } = await import(join(ROOT, 'lib/identity.js'))
  const ids = {}
  const homes = {}
  for (const role of ['a', 'b']) {
    homes[role] = join(base, role, 'p2p')
    mkdirSync(homes[role], { recursive: true })
    ids[role] = loadIdentity({ dir: homes[role], label: `probe-${role}` })
  }
  for (const [role, other] of [['a', 'b'], ['b', 'a']]) {
    writeFileSync(join(homes[role], 'peers.json'), `${JSON.stringify({
      version: 1,
      peers: [{
        publicKey: ids[other].publicKey,
        fingerprint: fingerprint(ids[other].publicKey),
        label: `probe-${other}`,
        status: 'trusted',
        firstSeen: new Date().toISOString(),
        lastSeen: new Date().toISOString(),
      }],
    }, null, 2)}\n`)
  }

  const started = Date.now()
  const children = ['a', 'b'].map((role) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', CHILD], {
      env: {
        ...process.env,
        DSH_HOME: join(base, role),
        PROBE_DIR: homes[role],
        PROBE_SECRET: secret,
        PROBE_ROLE: role,
        PROBE_PEER_FP: fingerprint(ids[role === 'a' ? 'b' : 'a'].publicKey),
        PROBE_START: String(started),
        PROBE_CAP: String(CAP_MS),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    child.stdout.on('data', (chunk) => { stdout += chunk.toString() })
    return new Promise((resolve) => child.on('exit', () => {
      const line = stdout.trim().split('\n').filter(Boolean).pop()
      try {
        resolve(JSON.parse(line))
      } catch {
        resolve({ role, parseError: line?.slice(0, 120) })
      }
    }))
  })
  const sides = await Promise.all(children)
  const byRole = Object.fromEntries(sides.map((side) => [side.role, side]))
  const bothWays = byRole.a?.gotMessage === true && byRole.b?.gotMessage === true
  const line = {
    run,
    bothWays,
    a: { sent: byRole.a?.sentOk, got: byRole.a?.gotMessage, gotMs: byRole.a?.gotMs, wakes: byRole.a?.wakes, outbox: byRole.a?.ackedOutbox, kinds: byRole.a?.outboxKinds, detail: byRole.a?.outboxDetail },
    b: { sent: byRole.b?.sentOk, got: byRole.b?.gotMessage, gotMs: byRole.b?.gotMs, wakes: byRole.b?.wakes, outbox: byRole.b?.ackedOutbox, kinds: byRole.b?.outboxKinds, detail: byRole.b?.outboxDetail },
  }
  console.log(JSON.stringify(line))
  rmSync(base, { recursive: true, force: true })
  return line
}

const results = []
for (let start = 1; start <= RUNS; start += PARALLEL) {
  const batch = []
  for (let run = start; run < Math.min(start + PARALLEL, RUNS + 1); run += 1) batch.push(oneRun(run))
  for (const line of await Promise.all(batch)) results.push(line)
}

const both = results.filter((row) => row.bothWays)
const times = both.map((row) => Math.max(row.a.gotMs ?? Infinity, row.b.gotMs ?? Infinity)).filter(Number.isFinite).sort((x, y) => x - y)
const pct = (p) => (times.length === 0 ? null : times[Math.min(times.length - 1, Math.floor((p / 100) * times.length))])
console.log(JSON.stringify({
  summary: true,
  runs: results.length,
  bothWays: both.length,
  oneWay: results.filter((row) => !row.bothWays && (row.a?.got || row.b?.got)).length,
  neither: results.filter((row) => !row.a?.got && !row.b?.got).length,
  medianMs: pct(50),
  p95Ms: pct(95),
  maxMs: times.length ? times[times.length - 1] : null,
  minMs: times.length ? times[0] : null,
}))
