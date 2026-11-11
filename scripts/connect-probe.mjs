/**
 * Test B: connection setup, measured instead of assumed. N fresh pairs, one per run, each in its own processes; records the
 * milliseconds from start to the first verified peer, or a timeout. Prints JSON lines.
 */
import { spawn } from 'node:child_process'
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomBytes } from 'node:crypto'

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..')
const RUNS = Number(process.env.PROBE_RUNS ?? 50)
const CAP_MS = Number(process.env.PROBE_CAP_MS ?? 45000)
const PARALLEL = Number(process.env.PROBE_PARALLEL ?? 5)

const CHILD = `
import { apply } from ${JSON.stringify(join(ROOT, 'index.js'))}
import { loadIdentity } from ${JSON.stringify(join(ROOT, 'lib/identity.js'))}
const dir = process.env.PROBE_DIR
const role = process.env.PROBE_ROLE
const peerKey = process.env.PROBE_PEER
loadIdentity({ dir, label: role })
const tools = new Map()
const api = apply({
  logger: () => ({ warn: () => {} }),
  tools: { register: (t) => tools.set(t.name, t) },
  agents: { get: () => undefined },
  systemPrompt: { getSectionOrder: () => 10, section: () => {} },
  effect: (i) => { globalThis.__cleanup = i() },
  session: { id: 'probe' },
}, { dir, enabled: true, fleetSecretEnv: 'PROBE_SECRET', autoStart: true, wakeOnMessage: false })

const started = Number(process.env.PROBE_START)
const cap = Number(process.env.PROBE_CAP)
const poll = setInterval(async () => {
  const live = api.transport.livePeers()
  if (live.includes(peerKey)) {
    clearInterval(poll)
    console.log(JSON.stringify({ role, connectedMs: Date.now() - started }))
    await api.transport.stop()
    process.exit(0)
  }
  if (Date.now() - started > cap) {
    clearInterval(poll)
    console.log(JSON.stringify({ role, connectedMs: null, timedOut: true, livePeers: live.length }))
    await api.transport.stop()
    process.exit(0)
  }
}, 50)
`

const results = []

async function oneRun(run) {
  const secret = randomBytes(16).toString('hex')
  const base = mkdtempSync(join(tmpdir(), `p2p-c-${run}-`))
  const ids = {}
  const homes = {}
  for (const role of ['a', 'b']) {
    const dir = join(base, role, 'p2p')
    mkdirSync(dir, { recursive: true })
    homes[role] = dir
  }
  const { loadIdentity } = await import(join(ROOT, 'lib/identity.js'))
  for (const role of ['a', 'b']) ids[role] = loadIdentity({ dir: homes[role], label: role })
  for (const [role, other] of [['a', 'b'], ['b', 'a']]) {
    const { fingerprint } = await import(join(ROOT, 'lib/identity.js'))
    const print = fingerprint(ids[other].publicKey)
    writeFileSync(join(homes[role], 'peers.json'), `${JSON.stringify({ version: 1, peers: [{ publicKey: ids[other].publicKey, fingerprint: print, label: other, status: 'trusted', firstSeen: new Date().toISOString(), lastSeen: new Date().toISOString() }] }, null, 2)}\n`)
  }
  const started = Date.now()
  const children = ['a', 'b'].map((role) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', CHILD], {
      env: { ...process.env, DSH_HOME: join(base, role), PROBE_DIR: homes[role], PROBE_SECRET: secret, PROBE_ROLE: role, PROBE_PEER: ids[role === 'a' ? 'b' : 'a'].publicKey, PROBE_START: String(started), PROBE_CAP: String(CAP_MS) },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let out = ''
    child.stdout.on('data', (chunk) => { out += chunk.toString() })
    return new Promise((resolve) => child.on('exit', () => resolve(out.trim().split('\n').filter(Boolean).map((line) => { try { return JSON.parse(line) } catch { return { raw: line } } }))))
  })
  const events = (await Promise.all(children)).flat()
  const perSide = events.map((e) => e.connectedMs ?? null)
  const both = perSide.every((ms) => typeof ms === 'number')
  const line = {
    run,
    bothSaw: both,
    bothSawMs: both ? Math.max(...perSide) : null,
    sides: perSide,
  }
  results.push(line)
  console.log(JSON.stringify(line))
  rmSync(base, { recursive: true, force: true })
  return line
}

for (let start = 1; start <= RUNS; start += PARALLEL) {
  const batch = []
  for (let run = start; run < Math.min(start + PARALLEL, RUNS + 1); run += 1) batch.push(oneRun(run))
  await Promise.all(batch)
}

const times = results.filter((r) => r.bothSawMs !== null).map((r) => r.bothSawMs).sort((x, y) => x - y)
const pct = (p) => (times.length === 0 ? null : times[Math.min(times.length - 1, Math.floor((p / 100) * times.length))])
console.log(JSON.stringify({
  summary: true,
  runs: results.length,
  bothSaw: times.length,
  onlyOneSide: results.filter((r) => !r.bothSaw).length,
  medianMs: pct(50), p95Ms: pct(95), maxMs: times.length ? times[times.length - 1] : null, minMs: times.length ? times[0] : null,
}))
