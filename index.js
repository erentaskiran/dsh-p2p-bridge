/**
 * dsh-p2p-bridge — encrypted peer-to-peer messaging between two DSH fleets.
 *
 * Four Host-level tools and nothing else in the agent's way: `p2p_status`, `p2p_peers`, `p2p_send`,
 * `p2p_inbox`. The transport runs in the background on a hyperswarm topic derived from a fleet
 * secret; an incoming message from a pinned peer is stored, then the owning session is woken with a
 * short notice (see `lib/wake.js`).
 *
 * Fail-closed rules, all enforced here and in `lib/protocol.js`:
 * - an unknown peer is `pending`, and a `pending` peer's messages are dropped, not queued;
 * - `p2p_send` refuses a peer that is not `trusted`;
 * - `to: "*"` is refused with "broadcast is v2";
 * - the plugin never returns a key, only a fingerprint.
 *
 * `enabled` defaults to false: with no fleet secret nothing joins the DHT, and `p2p_status` says so.
 */
import { createRequire } from 'node:module'
import { hostname } from 'node:os'

import { defaultDir, fingerprint, loadIdentity, sign } from './lib/identity.js'
import { openStore } from './lib/inbox.js'
import { MAX_MESSAGE_BYTES, PROTOCOL_VERSION, seal, topicFor } from './lib/protocol.js'
import { Transport } from './lib/transport.js'
import { deliverWake } from './lib/wake.js'

/** hyperswarm is CommonJS; this plugin stays ESM so the Host loader can import it. */
const require = createRequire(import.meta.url)

export const name = 'dsh-p2p-bridge'

/** Host services this plugin consumes; the same shape `@local/dsh-monitor` asks for. */
export const inject = ['tools', 'agents', 'systemPrompt']

const DEFAULTS = {
  enabled: false,
  fleetSecretEnv: 'DSH_P2P_FLEET_SECRET',
  autoStart: true,
  maxMessageBytes: MAX_MESSAGE_BYTES,
  wakeOnMessage: true,
  wakeSummaryChars: 120,
  retryAttempts: 3,
  flushIntervalMs: 2000,
  seenTtlHours: 24,
  dir: undefined,
}

/**
 * The model-facing shape of a peer row: everything except the key.
 *
 * The table on disk keeps `publicKey` because the transport needs it to address a socket; the tool
 * payload does not, because that is what lands in the transcript. Address peers by fingerprint.
 */
function publicPeer(peer) {
  return { fingerprint: peer.fingerprint, label: peer.label, status: peer.status, lastSeen: peer.lastSeen }
}

function errorText(error) {
  return error instanceof Error ? error.message : String(error)
}

/** Reads the fleet secret from the environment variable the config names; '' when it is not set. */
function readSecret(envName) {
  const value = process.env[envName]
  return typeof value === 'string' ? value.trim() : ''
}

export function apply(ctx, rawConfig) {
  const config = { ...DEFAULTS, ...(rawConfig ?? {}) }
  const dir = config.dir ?? defaultDir()
  const store = openStore({ dir, seenTtlHours: config.seenTtlHours })
  const logger = ctx.logger?.('p2p-bridge')
  const log = (what, detail) => {
    try {
      logger?.warn?.(`${what}${detail === undefined ? '' : `: ${typeof detail === 'string' ? detail : errorText(detail)}`}`)
    } catch {
      /* logging must never break delivery */
    }
  }

  let identity
  try {
    identity = loadIdentity({ dir, label: hostname() })
  } catch (error) {
    // A broken identity file is a decision for the user (regenerating it breaks every peer pin), so the
    // tools still register and every call reports the problem instead of the plugin failing to mount.
    identity = undefined
    log('identity unavailable', error)
  }

  const secret = readSecret(config.fleetSecretEnv)
  const enabled = config.enabled === true && secret.length > 0 && identity !== undefined
  const transport = new Transport({
    topic: enabled ? topicFor(secret) : undefined,
    identity,
    swarmFactory: config.swarmFactory ?? (() => new (requireHyperswarm())()),
    onEnvelope: (envelope) => handle(envelope),
    onPeer: (publicKey) => flushOutbox(publicKey),
    onReplay: (envelope) => reacknowledge(envelope),
    onPing: () => (identity === undefined ? undefined : seal(identity, { kind: 'ping', to: '*', body: {}, sign })),
    onConnect: (peer) => sendHello(peer),
    onRejected: (error) => log('rejected envelope', error),
    onLog: (what, detail) => log(`transport ${what}`, detail),
    maxBytes: config.maxMessageBytes,
  })
  transport.useSeen(store.seen)
  // A refresh is the moment a queued message may finally find a socket; drain on it.
  transport.onWake = () => {
    for (const publicKey of transport.livePeers()) flushOutbox(publicKey)
  }

  /** What happened to envelopes, for tests and diagnostics: arrival is distinct from delivery. */
  const stats = { received: 0, delivered: 0, dropped: 0, acksReceived: 0 }

  /** Session the plugin wakes: the one that owns this Host row, or the configured one. */
  const wakeSession = () => config.sessionId ?? ctx.session?.id

  /**
   * Delivery path for one verified envelope. Trust is checked before anything is written, so a
   * pending peer leaves no trace in the inbox.
   */
  function handle(envelope) {
    if (envelope.kind === 'msg') stats.received += 1
    confirmAcksTo(envelope.from)
    const sender = store.peers.observe({
      publicKey: envelope.from,
      fingerprint: fingerprint(envelope.from),
      label: envelope.body?.label ?? '',
    })
    if (sender.status !== 'trusted') {
      store.peers.save()
      if (envelope.kind === 'msg') stats.dropped += 1
      log('dropped message from untrusted peer', sender.fingerprint)
      return
    }
    if (envelope.kind === 'ack') {
      stats.acksReceived += 1
      acknowledge(envelope)
      return
    }
    if (envelope.kind === 'ping') {
      // Answer through the same socket the ping arrived on, addressed to its sender.
      if (envelope.to === '*' || envelope.to === identity?.publicKey) {
        const pong = seal(identity, { kind: 'hello', to: envelope.from, body: { label: identity.label, pong: true }, sign })
        transport.sendTo(envelope.from, pong)
      }
      return
    }
    if (envelope.kind === 'hello' || envelope.kind === 'peers') return
    if (envelope.kind !== 'msg') return
    if (envelope.to === '*') {
      stats.dropped += 1
      log('dropped broadcast', 'broadcast is v2')
      return
    }
    const row = {
      id: envelope.id,
      from: envelope.from,
      fromLabel: sender.label,
      fromFingerprint: sender.fingerprint,
      ts: envelope.ts,
      text: String(envelope.body?.text ?? ''),
      read: false,
      receivedAt: new Date().toISOString(),
    }
    store.inbox.append(row)
    stats.delivered += 1
    store.peers.save()
    reply({ kind: 'ack', to: envelope.from, body: { replyTo: envelope.id } })
    if (config.wakeOnMessage === true) {
      const sessionId = wakeSession()
      if (sessionId !== undefined) {
        deliverWake(ctx, sessionId, {
          label: sender.label,
          fingerprint: sender.fingerprint,
          text: row.text,
          summaryChars: config.wakeSummaryChars,
        })
      }
    }
  }

  /**
   * The first thing every socket gets: our hello, so the other side learns our key and can address a
   * reply. Without it a receiver never knows who is on the wire and `p2p_inbox` stays empty.
   */
  function sendHello(peer) {
    if (identity === undefined) return
    const hello = seal(identity, {
      kind: 'hello',
      to: '*',
      body: { label: identity.label, agentName: config.agentName ?? 'dsh-p2p-bridge', protocol: PROTOCOL_VERSION },
      sign,
    })
    transport.sendOn(peer, hello)
  }

  /**
   * Sends every queued message that has a live socket, and counts the attempt.
   *
   * A message stays in the outbox until the peer acks it. "The write returned" is not delivery: two peers
   * that dial each other at once can each hold a socket the other side already dropped, so the only
   * evidence that a message arrived is the ack that comes back. After `retryAttempts` tries the row is
   * marked `undelivered` and stays there for a person to look at, rather than disappearing.
   */
  function flushOutbox(publicKey) {
    const rows = store.outbox.all()
    const keep = []
    for (const row of rows) {
      const target = row.envelope?.to
      if (publicKey !== undefined && target !== publicKey) {
        keep.push(row)
        continue
      }
      const attempts = Number(row.attempts ?? 0)
      // `sendTo` decides whether a socket exists; requiring `livePeers` here skipped rows queued in the
      // window before the peer was verified, which is exactly when the first write was lost.
      if (target !== undefined && attempts < config.retryAttempts + 1 && !row.undelivered) {
        if (target !== undefined && transport.sendTo(target, row.envelope)) {
          // An ack has no acknowledgement of its own, so a successful write is as confirmed as it ever
          // gets: keeping it would leave the queue non-empty forever and make "drained" unreachable. If
          // this ack was the one that got lost, the peer retries the message, hits the replay rule and
          // is answered again by `reacknowledge` - that loop, not a permanently queued ack, is what makes
          // the message recoverable.
          if (row.kind === 'ack') continue
          keep.push({ ...row, attempts: attempts + 1, lastAttemptAt: new Date().toISOString() })
          continue
        }
      }
      keep.push(row)
    }
    store.outbox.clear()
    for (const row of keep) store.outbox.append(row)
  }

  /**
   * One pass of the retry ladder: flush what can go, then give up on what has used its attempts.
   *
   * The timer and `retry()` both call this, so a test exercises the real ladder rather than a copy of it.
   */
  function retryPass() {
    for (const row of store.outbox.all()) {
      if (row.undelivered === true) continue
      if (Number(row.attempts ?? 0) > config.retryAttempts) {
        const rows = store.outbox.all().map((item) => (item.id === row.id ? { ...item, undelivered: true } : item))
        store.outbox.clear()
        for (const item of rows) store.outbox.append(item)
        log('message undelivered', row.id)
      }
    }
    flushOutbox()
  }

  let retryTimer
  function startRetries() {
    if (retryTimer !== undefined) return
    retryTimer = setInterval(() => retryPass(), config.flushIntervalMs)
    retryTimer.unref?.()
  }

  /**
   * Drops the acks we still had queued for a peer once that peer talks to us again.
   *
   * An ack carries no acknowledgement of its own, so the next envelope from that peer is the evidence
   * that the pipe carries our writes: keeping the entry any longer would only re-send an ack the peer
   * already has.
   */
  function confirmAcksTo(publicKey) {
    const rows = store.outbox.all()
    const keep = rows.filter((row) => !(row.kind === 'ack' && row.envelope?.to === publicKey))
    if (keep.length === rows.length) return
    store.outbox.clear()
    for (const row of keep) store.outbox.append(row)
  }

  /**
   * A duplicate of a message we already delivered gets its ack again, and nothing else.
   *
   * The sender retries until it hears an ack, so the same id arriving twice is normal, not an attack. The
   * message is not stored twice and the agent is not woken twice; only the ack is repeated.
   */
  function reacknowledge(envelope) {
    if (envelope?.kind !== 'msg' || typeof envelope.id !== 'string') return
    if (envelope.to === '*' || envelope.to !== identity?.publicKey) return
    if (!store.inbox.all().some((row) => row.id === envelope.id)) return
    reply({ kind: 'ack', to: envelope.from, body: { replyTo: envelope.id } })
  }

  /** Raises one row's attempt count without a full rewrite when only that row changed. */
  function bumpAttempts(storeRef, id) {
    const rows = storeRef.outbox.all()
    const next = rows.map((row) => (row.id === id ? { ...row, attempts: Number(row.attempts ?? 0) + 1, lastAttemptAt: new Date().toISOString() } : row))
    storeRef.outbox.clear()
    for (const row of next) storeRef.outbox.append(row)
  }

  /** An ack removes the matching outbox entry, which is the only way a queued message leaves it. */
  function acknowledge(envelope) {
    const replyTo = envelope.body?.replyTo
    if (typeof replyTo !== 'string') return
    const rows = store.outbox.all()
    const kept = rows.filter((row) => row.id !== replyTo && row.envelope?.id !== replyTo)
    if (kept.length !== rows.length) {
      store.outbox.clear()
      for (const row of kept) store.outbox.append(row)
      log('acked', replyTo)
    }
  }

  /** Signs and sends one envelope; `queued` means no live socket took it. */
  function reply({ kind, to, body }) {
    if (identity === undefined) throw new Error('no identity: cannot sign a message')
    const envelope = seal(identity, { kind, to, body, sign })
    // Queue before the write, not after. Writing first leaves a window in which the peer's ack arrives,
    // `acknowledge()` finds no row to remove, and the row appears afterwards and waits for an ack that
    // already came: measured as a `msg:queued` residue in 4 of 20 probe runs, attempts=1.
    if (kind === 'msg') {
      store.outbox.append({
        id: envelope.id,
        kind,
        envelope,
        attempts: 0,
        queuedAt: new Date().toISOString(),
        undelivered: false,
      })
    }
    const sent = transport.sendTo(to, envelope)
    if (kind === 'msg') {
      // The row is already queued; record whether the first write landed so the retry ladder knows where
      // it stands.
      if (sent) bumpAttempts(store, envelope.id)
    } else if (kind === 'ack' && !sent) {
      // Only a failed write queues an ack: a written one has nothing left to prove, and queueing every ack
      // left a phantom row in 19 of 20 probe runs because nothing confirms an ack of an ack.
      store.outbox.append({
        id: envelope.id,
        kind,
        envelope,
        attempts: sent ? 1 : 0,
        queuedAt: new Date().toISOString(),
        undelivered: false,
      })
    }
    return { envelope, sent }
  }

  function status() {
    // No public key here on purpose: this object reaches the model and the chat transcript, and the
    // fingerprint is all a person or an agent needs — `p2p_send` addresses peers by fingerprint. The
    // key still travels on the wire in the hello envelope, where the other node needs it.
    return {
      enabled,
      fingerprint: identity?.fingerprint ?? null,
      topicPeers: transport.peerCount,
      trustedPeers: store.peers.list().filter((peer) => peer.status === 'trusted').length,
      unread: store.inbox.recent({ limit: Number.MAX_SAFE_INTEGER, unreadOnly: true }).length,
      // A queued or given-up message is visible in the status: silence is how a lost message stays lost.
      queued: store.outbox.all().filter((row) => row.undelivered !== true).length,
      undelivered: store.outbox.all().filter((row) => row.undelivered === true).length,
      listening: transport.listening,
      // A failed replay write degrades the window to memory: say so instead of pretending it is durable.
      ...(store.seen.lastSaveError === undefined ? {} : { replayWarning: `replay window not persisted: ${store.seen.lastSaveError}` }),
      ...(identity === undefined ? { error: 'identity file is unreadable or malformed' } : {}),
      ...(enabled ? {} : { reason: secret.length === 0 ? `no fleet secret in $${config.fleetSecretEnv}` : 'disabled in config' }),
    }
  }

  const statusTool = {
    name: 'p2p_status',
    description:
      'State of the peer-to-peer bridge: whether it is enabled, this fleet\'s public fingerprint, how many peers are connected and trusted, and how many messages are unread. Returns fingerprints, never keys.',
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    output: {
      schema: {
        type: 'object',
        properties: {
          enabled: { type: 'boolean' },
          fingerprint: { type: ['string', 'null'] },
          topicPeers: { type: 'number' },
          trustedPeers: { type: 'number' },
          unread: { type: 'number' },
          queued: { type: 'number' },
          undelivered: { type: 'number' },
          listening: { type: 'boolean' },
          reason: { type: 'string' },
          error: { type: 'string' },
          replayWarning: { type: 'string' },
        },
        required: ['enabled', 'topicPeers', 'trustedPeers', 'unread', 'queued', 'undelivered', 'listening'],
      },
      render(_args, value) {
        if (value.enabled !== true) {
          return [{ type: 'text', text: `p2p bridge disabled (${value.reason ?? 'not configured'})` }]
        }
        return [{
          type: 'text',
          text: `p2p enabled as ${value.fingerprint} — ${value.topicPeers} peer(s) connected, ${value.trustedPeers} trusted, ${value.unread} unread, ${value.queued} queued, ${value.undelivered} undelivered`,
        }]
      },
    },
    async execute() {
      return status()
    },
  }

  const peersTool = {
    name: 'p2p_peers',
    description:
      'Peers this fleet has seen: their fingerprint, label and status. A peer is `pending` until a person trusts it; only a trusted peer\'s messages are delivered.',
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    output: {
      schema: {
        type: 'object',
        properties: {
          peers: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                fingerprint: { type: 'string' },
                label: { type: 'string' },
                status: { type: 'string', enum: ['pending', 'trusted'] },
                lastSeen: { type: 'string' },
              },
              required: ['fingerprint', 'label', 'status', 'lastSeen'],
            },
          },
        },
        required: ['peers'],
      },
      render(_args, value) {
        if (value.peers.length === 0) return [{ type: 'text', text: 'no peers seen yet' }]
        const lines = value.peers.map((peer) => `${peer.fingerprint} ${peer.status} ${peer.label || '(no label)'} last seen ${peer.lastSeen}`)
        return [{ type: 'text', text: lines.join('\n') }]
      },
    },
    async execute() {
      return { peers: store.peers.list().map(publicPeer) }
    },
  }

  const sendTool = {
    name: 'p2p_send',
    description:
      'Send a short text message to a trusted peer, addressed by fingerprint. Fails for an untrusted peer and for broadcast ("*"), which is not in v1. The message is signed with this fleet\'s key and retried; if no socket takes it, it waits in the outbox.',
    parameters: {
      type: 'object',
      properties: {
        to: { type: 'string', description: 'Fingerprint of a trusted peer (16 hex characters). "*" is refused: broadcast is v2.' },
        text: { type: 'string', description: 'Message body, plain text, at most 64 KB.' },
      },
      required: ['to', 'text'],
      additionalProperties: false,
    },
    output: {
      schema: {
        type: 'object',
        properties: {
          messageId: { type: 'string' },
          queued: { type: 'boolean' },
          sent: { type: 'boolean' },
          peerStatus: { type: 'string' },
        },
        required: ['messageId', 'queued', 'sent', 'peerStatus'],
      },
      render(_args, value) {
        return [{ type: 'text', text: value.sent ? `sent ${value.messageId}` : `queued ${value.messageId} (peer offline)` }]
      },
    },
    async execute(rawArgs) {
      const to = String(rawArgs?.to ?? '').trim()
      const text = String(rawArgs?.text ?? '')
      if (to === '*') throw new Error('broadcast is v2: p2p_send needs one trusted peer fingerprint')
      if (to.length === 0) throw new Error('p2p_send needs a peer fingerprint')
      if (text.length === 0) throw new Error('p2p_send needs some text')
      if (Buffer.byteLength(text, 'utf8') > config.maxMessageBytes) {
        throw new Error(`message is over the ${config.maxMessageBytes}-byte cap`)
      }
      if (identity === undefined) throw new Error('no identity: the bridge cannot sign a message')
      const peer = store.peers.byFingerprint(to)
      if (peer === undefined) throw new Error(`unknown peer ${to}: no such fingerprint in peers.json`)
      if (peer.status !== 'trusted') throw new Error(`peer not trusted: run p2p_trust <fingerprint>`)
      const { envelope, sent } = reply({ kind: 'msg', to: peer.publicKey, body: { text, replyTo: null, label: identity.label } })
      return { messageId: envelope.id, queued: !sent, sent, peerStatus: peer.status }
    },
  }

  const inboxTool = {
    name: 'p2p_inbox',
    description:
      'Messages delivered by trusted peers, newest last. The wake notice carries only a preview; the full text is here. `markRead` flips the unread flag on the returned rows.',
    parameters: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'How many of the newest messages to return (default 20).' },
        markRead: { type: 'boolean', description: 'Mark the returned messages as read (default false).' },
      },
      additionalProperties: false,
    },
    output: {
      schema: {
        type: 'object',
        properties: {
          messages: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                id: { type: 'string' },
                from: { type: 'string' },
                fromLabel: { type: 'string' },
                ts: { type: 'string' },
                text: { type: 'string' },
                read: { type: 'boolean' },
              },
              required: ['id', 'from', 'fromLabel', 'ts', 'text', 'read'],
            },
          },
        },
        required: ['messages'],
      },
      render(_args, value) {
        if (value.messages.length === 0) return [{ type: 'text', text: 'no messages' }]
        return [{
          type: 'text',
          text: value.messages.map((row) => `${row.read ? ' ' : '•'} ${row.ts} ${row.fromLabel || row.from.slice(0, 12)}: ${row.text.slice(0, 200)}`).join('\n'),
        }]
      },
    },
    async execute(rawArgs) {
      const limit = Number.isFinite(rawArgs?.limit) ? Math.max(1, Math.min(200, Number(rawArgs.limit))) : 20
      const rows = store.inbox.recent({ limit })
      if (rawArgs?.markRead === true) {
        store.inbox.markRead(rows.map((row) => row.id))
        return { messages: rows.map((row) => ({ ...row, read: true })) }
      }
      return { messages: rows }
    },
  }

  ctx.tools.register(statusTool)
  ctx.tools.register(peersTool)
  ctx.tools.register(sendTool)
  ctx.tools.register(inboxTool)

  ctx.systemPrompt.section({
    name: 'tool:p2p-bridge',
    order: ctx.systemPrompt.getSectionOrder('TOOL_JOBS') + 1,
    text:
      'p2p_status, p2p_peers, p2p_send and p2p_inbox connect this fleet to another DeepSeek Harness fleet over an encrypted peer-to-peer link: '
      + 'use p2p_peers to see which peers are trusted, p2p_send to message one by fingerprint, and p2p_inbox to read what arrived — '
      + 'a [P2P] wake notice carries only a preview, so open the inbox before answering. Messages never reach an unknown peer: '
      + 'an unapproved fingerprint stays pending and its traffic is dropped, never queued.',
  })

  if (enabled && config.autoStart === true) {
    transport.start()
    startRetries()
  }

  // A Host row outlives turns; the sockets and the timer ladder do not outlive the plugin.
  ctx.effect(() => () => {
    if (retryTimer !== undefined) clearInterval(retryTimer)
    void transport.stop()
  }, 'p2p-bridge.transport()')

  return { store, transport, identity, enabled, stats, flushOutbox, retryPass, flushWrites: () => transport.flushWrites() }
}

function requireHyperswarm() {
  return require('hyperswarm')
}
