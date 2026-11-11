/**
 * Local storage under `~/.dsh/p2p/`.
 *
 * Append-only JSONL, because the volume here is tens of messages a day and a file a person can open
 * is worth more than a database. Two rules hold everywhere in this module:
 *
 * 1. A write lands whole or not at all (temp file + rename). A half-written line would make the next
 *    read of that file fail, and losing the whole inbox to one bad append is not acceptable.
 * 2. A file that cannot be parsed is reset, not repaired by guessing. These files hold a derived
 *    record of what happened, never the only copy of a key.
 *
 * The seen set and the peer table are the two pieces of state that decide whether a message is
 * delivered, so they live here next to the files that store them rather than inside the transport.
 */
import { chmodSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const FILE_MODE = 0o600
const DAY_MS = 24 * 60 * 60 * 1000

function writeAtomic(path, text, mode = FILE_MODE) {
  const tmp = `${path}.${process.pid}.tmp`
  writeFileSync(tmp, text, { mode })
  chmodSync(tmp, mode)
  renameSync(tmp, path)
}

function readLines(path) {
  let text
  try {
    text = readFileSync(path, 'utf8')
  } catch {
    return []
  }
  const rows = []
  for (const line of text.split('\n')) {
    if (line.trim().length === 0) continue
    try {
      const row = JSON.parse(line)
      if (row !== null && typeof row === 'object' && !Array.isArray(row)) rows.push(row)
    } catch {
      /* one unreadable line does not cost the rest of the file */
    }
  }
  return rows
}

function readJson(path) {
  try {
    const value = JSON.parse(readFileSync(path, 'utf8'))
    return value !== null && typeof value === 'object' && !Array.isArray(value) ? value : undefined
  } catch {
    return undefined
  }
}

/** Replay window: ids seen inside the TTL are refused. Persisted, so a restart does not reopen it. */
export class SeenSet {
  constructor({ dir, ttlHours = 24, now = Date.now() }) {
    this.path = join(dir, 'seen.json')
    this.ttlMs = ttlHours * 60 * 60 * 1000
    this.ids = new Map()
    const stored = readJson(this.path)
    const entries = stored?.ids
    if (entries !== null && typeof entries === 'object' && !Array.isArray(entries)) {
      for (const [id, at] of Object.entries(entries)) {
        if (typeof at === 'number' && now - at <= this.ttlMs) this.ids.set(id, at)
      }
    }
  }

  has(id, now = Date.now()) {
    const at = this.ids.get(id)
    if (at === undefined) return false
    if (now - at > this.ttlMs) {
      this.ids.delete(id)
      return false
    }
    return true
  }

  add(id, now = Date.now()) {
    this.ids.set(id, now)
  }

  /** Drops expired ids; returns how many went away. */
  prune(now = Date.now()) {
    let dropped = 0
    for (const [id, at] of this.ids) {
      if (now - at > this.ttlMs) {
        this.ids.delete(id)
        dropped += 1
      }
    }
    return dropped
  }

  get size() {
    return this.ids.size
  }

  /** Writes the window, dropping expired ids first: the file stays proportional to the last 24 h. */
  save(now = Date.now()) {
    this.prune(now)
    mkdirSync(join(this.path, '..'), { recursive: true })
    writeAtomic(this.path, `${JSON.stringify({ version: 1, ids: Object.fromEntries(this.ids) })}\n`)
  }
}

/**
 * The peer table: `status` is `pending` until a person approves the fingerprint, and nothing in the
 * plugin upgrades it on its own. `trusted` is the only status that lets a message wake an agent.
 */
export class PeerTable {
  constructor({ dir }) {
    this.path = join(dir, 'peers.json')
    this.peers = new Map()
    const stored = readJson(this.path)
    for (const peer of Array.isArray(stored?.peers) ? stored.peers : []) {
      if (peer !== null && typeof peer === 'object' && typeof peer.publicKey === 'string') {
        this.peers.set(peer.publicKey, {
          publicKey: peer.publicKey,
          fingerprint: String(peer.fingerprint ?? ''),
          label: String(peer.label ?? ''),
          status: peer.status === 'trusted' ? 'trusted' : 'pending',
          firstSeen: String(peer.firstSeen ?? ''),
          lastSeen: String(peer.lastSeen ?? ''),
        })
      }
    }
  }

  list() {
    return [...this.peers.values()]
  }

  get(publicKey) {
    return this.peers.get(publicKey)
  }

  byFingerprint(wanted) {
    const needle = String(wanted).toLowerCase()
    for (const peer of this.peers.values()) {
      if (peer.fingerprint.toLowerCase() === needle) return peer
    }
    return undefined
  }

  /**
   * First sighting of a key makes it pending; a known peer only gets its lastSeen refreshed.
   *
   * The label a peer sends about itself is a hint, never an override: a person's local name for that
   * peer (`p2p_trust`, `setLabel`) is what stays, or a remote could rename itself into something
   * confusing in our own peer table.
   */
  observe({ publicKey, fingerprint, label, now = new Date().toISOString() }) {
    const existing = this.peers.get(publicKey)
    if (existing !== undefined) {
      existing.lastSeen = now
      if (label && !existing.label) existing.label = label
      return existing
    }
    const peer = {
      publicKey,
      fingerprint: String(fingerprint ?? ''),
      label: String(label ?? '').slice(0, 64),
      status: 'pending',
      firstSeen: now,
      lastSeen: now,
    }
    this.peers.set(publicKey, peer)
    return peer
  }

  /** Approves a peer by fingerprint. Returns the peer, or undefined when no such fingerprint is known. */
  trust(fingerprint, now = new Date().toISOString()) {
    const peer = this.byFingerprint(fingerprint)
    if (peer === undefined) return undefined
    peer.status = 'trusted'
    peer.lastSeen = now
    return peer
  }

  setLabel(publicKey, label) {
    const peer = this.peers.get(publicKey)
    if (peer !== undefined) peer.label = String(label).slice(0, 64)
  }

  save() {
    mkdirSync(join(this.path, '..'), { recursive: true })
    writeAtomic(this.path, `${JSON.stringify({ version: 1, peers: this.list() }, null, 2)}\n`)
  }
}

/** One append-only message file: the inbox a person can read with `tail -f`. */
export class MessageLog {
  constructor({ dir, name }) {
    this.path = join(dir, name)
    this.name = name
  }

  all() {
    return readLines(this.path)
  }

  append(row) {
    mkdirSync(join(this.path, '..'), { recursive: true })
    const existing = (() => {
      try {
        const text = readFileSync(this.path, 'utf8')
        return text.length > 0 && !text.endsWith('\n') ? `${text}\n` : text
      } catch {
        return ''
      }
    })()
    writeAtomic(this.path, `${existing}${JSON.stringify(row)}\n`)
    return row
  }

  /** Inbox read: newest last, `limit` applied from the end so a big file stays cheap to read. */
  recent({ limit = 20, unreadOnly = false } = {}) {
    const rows = this.all().filter((row) => (unreadOnly ? row.read !== true : true))
    return rows.slice(Math.max(0, rows.length - limit))
  }

  /** Flips `read` on the given ids and rewrites the file whole; a no-op returns false. */
  markRead(ids) {
    const wanted = new Set(ids)
    let changed = false
    const rows = this.all().map((row) => {
      if (wanted.has(row.id) && row.read !== true) {
        changed = true
        return { ...row, read: true }
      }
      return row
    })
    if (!changed) return false
    writeAtomic(this.path, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`)
    return true
  }

  clear() {
    writeAtomic(this.path, '')
  }

  get size() {
    return this.all().length
  }
}

/** Everything the plugin stores for one fleet, created on demand. */
export function openStore({ dir, seenTtlHours = 24, now = Date.now() }) {
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  return {
    dir,
    inbox: new MessageLog({ dir, name: 'inbox.jsonl' }),
    outbox: new MessageLog({ dir, name: 'outbox.jsonl' }),
    seen: new SeenSet({ dir, ttlHours: seenTtlHours, now }),
    peers: new PeerTable({ dir }),
  }
}

export { DAY_MS }
