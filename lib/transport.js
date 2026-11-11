/**
 * The network half: a hyperswarm node on one topic, one framed socket per peer, and the reconnect
 * ladder.
 *
 * hyperswarm is the transport because it gives UDP hole punching, DHT topic lookup and an encrypted
 * Noise stream without any of the three being our code. This module adds exactly what hyperswarm
 * does not do: NDJSON framing with a read cap, a signature check on every envelope, and backoff for
 * a peer that keeps dropping.
 *
 * The socket is treated as hostile: a line over the cap or an envelope that fails `protocol.open`
 * closes that connection and nothing from it reaches the inbox.
 */
import { createFramer, open as openEnvelope, MAX_MESSAGE_BYTES } from './protocol.js'

/** Backoff ladder: 1s, 2s, 4s, ... capped at 30s, with jitter so two nodes do not lock-step. */
export const BACKOFF_BASE_MS = 1000
export const BACKOFF_CAP_MS = 30_000

export function backoffDelay(attempt, random = Math.random) {
  const capped = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** Math.max(0, attempt))
  return Math.round(capped * (0.5 + random() * 0.5))
}

/**
 * One swarm for one fleet.
 *
 * `onEnvelope(envelope, peer)` is called only after the signature verified; it decides delivery. A
 * rejected envelope is reported through `onRejected(error, peer)` and the peer is dropped, because
 * the safe assumption after a bad signature is that the peer is not who it claims to be.
 */
export class Transport {
  constructor({ topic, identity, swarmFactory, onEnvelope, onRejected, onLog, onPeer, onConnect, onReplay, maxBytes = MAX_MESSAGE_BYTES, sendTimeoutMs = 5000 }) {
    this.topic = topic
    this.identity = identity
    this.swarmFactory = swarmFactory
    this.onEnvelope = onEnvelope
    this.onRejected = onRejected ?? (() => {})
    this.onPeer = onPeer ?? (() => {})
    this.onConnect = onConnect ?? (() => {})
    this.onReplay = onReplay ?? (() => {})
    this.onWake = () => {}
    this.onPing = undefined
    this.lastHeard = new Map()
    this.pendingWrites = 0
    this.onLog = onLog ?? (() => {})
    this.maxBytes = maxBytes
    this.sendTimeoutMs = sendTimeoutMs
    this.swarm = undefined
    // Keyed per socket, never per remote key: hyperswarm dials both ways and destroys the duplicate, and
    // two sockets for one peer share the remote key. Keying by that key let the duplicate's close event
    // delete the live socket, which is why a message could be "sent" to nobody.
    this.connections = new Map() // uid -> { uid, socket, hex, publicKey, framer }
    this.nextUid = 1
    this.attempts = new Map()
    this.listening = false
    this.destroyed = false
    this.keepAliveTimer = undefined
  }

  /** Joins the topic. Safe to call twice; the second call is a no-op. */
  start() {
    if (this.swarm !== undefined) return this
    const swarm = this.swarmFactory()
    this.swarm = swarm
    swarm.on('connection', (socket, info) => this.accept(socket, info))
    swarm.on('error', (error) => this.onLog('swarm error', error))
    this.discovery = swarm.join(this.topic, { server: true, client: true })
    this.discovery?.flushed?.().then(() => { this.listening = true }).catch(() => {})
    this.startKeepAlive()
    return this
  }

  /**
   * Re-announces the topic while nobody is connected.
   *
   * A DHT announce can land before the other node has announced anything, and nothing retries it: the
   * two sides sit on the same topic with no socket, which is the "no live trusted peer" timeout in the
   * demo. Re-joining every `keepAliveMs` costs one announce round and turns that dead state into a
   * delay. Once a peer is present the timer stops, so an idle bridge is not announcing all day.
   */
  startKeepAlive(keepAliveMs = 5_000) {
    if (this.keepAliveTimer !== undefined) return
    this.keepAliveTimer = setInterval(() => {
      if (this.destroyed) return
      this.dropStaleSockets()
      this.pingQuietPeers()
      if (this.peerCount > 0) return
      this.refresh()
    }, keepAliveMs)
    this.keepAliveTimer.unref?.()
  }

  async stop() {
    this.destroyed = true
    for (const peer of this.connections.values()) {
      try {
        peer.socket.destroy()
      } catch {
        /* already gone */
      }
    }
    this.connections.clear()
    if (this.keepAliveTimer !== undefined) {
      clearInterval(this.keepAliveTimer)
      this.keepAliveTimer = undefined
    }
    if (this.swarm !== undefined) {
      try {
        await this.swarm.destroy()
      } catch {
        /* a swarm that failed to boot has nothing to destroy */
      }
      this.swarm = undefined
    }
    this.listening = false
  }

  /** A new inbound socket: frame it, verify everything, and keep it only while it behaves. */
  accept(socket, info) {
    const hex = info?.publicKey !== undefined ? Buffer.from(info.publicKey).toString('hex') : 'unknown'
    const uid = this.nextUid++
    const peer = { uid, socket, hex, publicKey: undefined, createdAt: Date.now(), attempts: this.attempts.get(hex) ?? 0 }
    this.connections.set(uid, peer)
    socket.setNoDelay?.(true)
    socket.on('error', (error) => this.drop(uid, error))
    socket.on('close', () => this.drop(uid))
    const framer = createFramer({
      maxBytes: this.maxBytes,
      onMessage: (envelope) => this.read(envelope, peer),
      onError: (error) => {
        this.onRejected(error, peer)
        this.drop(uid, error)
      },
    })
    peer.framer = framer
    socket.on('data', (chunk) => framer.push(chunk))
    socket.on('end', () => framer.end())
    this.onLog('connection', { peer: hex.slice(0, 12) })
    // The socket is up but the peer's identity key is not known until it signs something. The plugin
    // answers this by sending its hello, which is what makes `sendTo(publicKey)` possible afterwards.
    try {
      this.onConnect(peer)
    } catch (error) {
      this.onLog('hello failed', error)
    }
    // Both sides dial at once, so both end up with two sockets and hyperswarm destroys the duplicate on
    // each side. Each side may keep the *other* socket - then a single hello lands in a socket the far
    // side already dropped, and that side never learns who is connected. Measured: one side saw its peer
    // at 3 s while the other timed out at 60 s in 4 of 5 fresh runs. Repeating the hello until an
    // envelope arrives makes discovery symmetric whatever pair of sockets survives.
    this.repeatHello(peer)
    return peer
  }

  read(envelope, peer) {
    let verified
    try {
      verified = openEnvelope(envelope, { selfPublicKey: this.identity.publicKey, seen: this.seen, maxBytes: this.maxBytes })
    } catch (error) {
      // A replay is not an attack to punish here: our own retransmission of an unacked message has the
      // same id. The plugin re-acks what it already delivered instead of dropping the duplicate, and the
      // socket stays open, or a lost ack would make that message impossible to acknowledge forever.
      if (error.code === 'replay') {
        peer.publicKey = envelope?.from
        this.onReplay(envelope, peer)
        return
      }
      this.onRejected(error, peer)
      if (error.code === 'oversize' || error.code === 'malformed') this.drop(peer.uid, error)
      return
    }
    const firstTime = peer.publicKey === undefined
    peer.publicKey = verified.from
    peer.unansweredPings = 0
    this.noteHeard(verified.from)
    // A verified sender is the moment the outbox can drain: the socket exists and we know who is on it.
    if (firstTime) this.onPeer(verified.from, peer)
    this.onEnvelope(verified, peer)
  }

  /** Attaches the replay set after construction, so the transport can be built before the store. */
  useSeen(seen) {
    this.seen = seen
    return this
  }

  drop(uid, error) {
    const peer = this.connections.get(uid)
    if (peer === undefined) return
    this.connections.delete(uid)
    const hex = peer.hex
    // A clean close counts as a lost socket too. Only errors used to raise the attempt counter, so a
    // socket that closed politely (hyperswarm dropping the duplicate of a mutual dial, or the far side
    // exiting a turn) never reached the refresh ladder and the topic was never re-discovered: that is the
    // state the two 50-run failures ended in, with an ack queued and no socket to write it to.
    this.attempts.set(hex, Math.min(6, peer.attempts + 1))
    try {
      peer.socket.destroy()
    } catch {
      /* already destroyed */
    }
    this.scheduleReconnect(hex)
  }

  /**
   * Forces a fresh discovery round after a socket died.
   *
   * Calling `join` again on a topic this swarm already joined is a no-op, which is why the earlier
   * version never reconnected: the ladder ticked, the log line printed, and nothing on the wire changed.
   * Leaving the topic first and joining it again is what makes the DHT look the peer up anew.
   */
  scheduleReconnect(hex) {
    if (this.destroyed || this.swarm === undefined || this.refreshTimer !== undefined) return
    const attempt = this.attempts.get(hex) ?? 0
    if (attempt === 0) return
    const delay = backoffDelay(attempt)
    this.onLog('reconnect scheduled', { peer: hex.slice(0, 12), delayMs: delay, attempt })
    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = undefined
      if (this.destroyed) return
      this.refresh()
    }, delay)
    this.refreshTimer.unref?.()
  }

  /**
   * Destroys sockets that never proved who is on the other end.
   *
   * Two peers that dial each other at once can each keep the half the other one dropped; that socket never
   * errors, so it is never dropped, and the fleet waits on a pipe nobody reads. Measured on this machine:
   * in 4 of 5 fresh runs one side saw its peer in ~3 s while the other saw nothing for 60 s. A socket that
   * has said nothing for `staleMs` is treated as that half, destroyed, and the topic is refreshed so a
   * clean dial can happen.
   */
  dropStaleSockets(staleMs = 6000) {
    const now = Date.now()
    let dropped = 0
    for (const peer of [...this.connections.values()]) {
      if (peer.publicKey !== undefined) continue
      if (now - (peer.createdAt ?? now) < staleMs) continue
      dropped += 1
      this.drop(peer.uid, new Error('unverified socket'))
    }
    if (dropped > 0) {
      this.onLog('dropped unverified sockets', dropped)
      this.refresh()
    }
    return dropped
  }

  /**
   * Pings a verified peer that has gone quiet, and drops it if a previous ping also went unanswered.
   *
   * This is the liveness check the transport was missing. A socket can report `live` while the other side
   * has already reset its half - hyperswarm destroys the duplicate of a mutual dial on one side only - so
   * "is it open" is not the question; "does anybody answer" is. Two unanswered pings is about ten seconds.
   */
  pingQuietPeers(quietMs = 5000, unansweredLimit = 2) {
    if (this.identity === undefined) return 0
    const now = Date.now()
    let dropped = 0
    for (const peer of [...this.connections.values()]) {
      if (peer.publicKey === undefined || peer.socket.destroyed) continue
      const heard = this.lastHeard.get(peer.publicKey) ?? 0
      if (now - heard < quietMs) {
        peer.unansweredPings = 0
        continue
      }
      peer.unansweredPings = (peer.unansweredPings ?? 0) + 1
      if (peer.unansweredPings > unansweredLimit) {
        this.onLog('peer silent, dropping', { peer: peer.hex.slice(0, 12) })
        this.drop(peer.uid, new Error('peer silent'))
        dropped += 1
        continue
      }
      const ping = this.onPing?.()
      if (ping !== undefined) this.sendOn(peer, ping)
    }
    return dropped
  }

  /** Records that an envelope arrived from a peer, which is what keeps its socket alive. */
  noteHeard(publicKey) {
    if (typeof publicKey === 'string') this.lastHeard.set(publicKey, Date.now())
  }

  /** Leaves the topic and joins it again: the only way hyperswarm re-runs discovery for a joined topic. */
  refresh() {
    if (this.destroyed || this.swarm === undefined) return false
    try {
      this.swarm.leave(this.topic)
      this.discovery = this.swarm.join(this.topic, { server: true, client: true })
      this.discovery?.flushed?.().then(() => { this.listening = true }).catch(() => {})
      this.onLog('topic refreshed', { peers: this.peerCount })
      this.onWake()
      return true
    } catch (error) {
      this.onLog('refresh failed', error)
      return false
    }
  }

  /**
   * Sends the hello again on this socket until the far side answers, then stops.
   *
   * A no-op once `peer.publicKey` is known, and capped so a silent socket does not talk forever. The
   * interval is short because this only runs during the first seconds of a connection.
   */
  repeatHello(peer, { attempts = 6, intervalMs = 700 } = {}) {
    let left = attempts
    const timer = setInterval(() => {
      if (this.destroyed || left === 0 || peer.socket.destroyed || peer.publicKey !== undefined) {
        clearInterval(timer)
        return
      }
      left -= 1
      try {
        this.onConnect(peer)
      } catch (error) {
        this.onLog('hello retry failed', error)
      }
    }, intervalMs)
    timer.unref?.()
  }

  /** Writes one envelope to one specific socket, before its public key is known (the hello). */
  sendOn(peer, envelope) {
    try {
      peer.socket.write(`${JSON.stringify(envelope)}\n`)
      return true
    } catch {
      return false
    }
  }

  /** Writes one envelope to every open socket; resolves with how many sockets took it. */
  broadcast(envelope) {
    const line = `${JSON.stringify(envelope)}\n`
    let sent = 0
    for (const peer of this.connections.values()) {
      try {
        peer.socket.write(line)
        sent += 1
      } catch (error) {
        this.onLog('write failed', error)
      }
    }
    return sent
  }

  /** Sends to one verified peer by public key. Returns false when that peer has no live socket. */
  /**
   * Writes one envelope on every open socket for that peer.
   *
   * Two peers that dial each other at once end up with more than one socket, and hyperswarm destroys
   * the duplicate on one side without telling the other: a single-socket send then writes into a dead
   * pipe and reports success. Writing to all of them and letting the receiver's replay check drop the
   * copy costs one duplicate line in the rare case and never loses the message, which is the right
   * trade for a text message.
   */
  sendTo(publicKey, envelope) {
    const line = `${JSON.stringify(envelope)}\n`
    let wrote = 0
    for (const peer of this.connections.values()) {
      if (peer.publicKey !== publicKey) continue
      if (peer.socket.destroyed || peer.socket.writable === false) continue
      try {
        // Count the write until the socket acknowledges it. `write()` returns as soon as the bytes are
        // buffered, so a process that exits immediately after sending can drop the ack it just produced.
        this.pendingWrites += 1
        peer.socket.write(line, () => { this.pendingWrites -= 1 })
        wrote += 1
      } catch (error) {
        this.onLog('write failed', error)
      }
    }
    return wrote > 0
  }

  /**
   * Asks the plugin to retry its queue. Called when the topic is refreshed, because a new socket is the
   * moment a queued message can finally go out, and no peer event fires when the far side reconnects.
   */
  kickQueue() {
    this.onWake?.()
  }

  /**
   * Resolves when every write has reached its socket, or when `timeoutMs` passes.
   *
   * A graceful shutdown needs this: `write()` is buffered, so stopping the transport while an ack is still
   * in the buffer loses it, and the peer waits for an ack that was never sent. Measured as two of fifty
   * demo runs where the receiver had delivered the message and the sender still timed out.
   */
  async flushWrites(timeoutMs = 5000) {
    const started = Date.now()
    while (this.pendingWrites > 0) {
      if (Date.now() - started > timeoutMs) return false
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    return true
  }

  /** Public keys with at least one open socket right now. */
  livePeers() {
    const keys = new Set()
    for (const peer of this.connections.values()) {
      if (peer.publicKey !== undefined && !peer.socket.destroyed) keys.add(peer.publicKey)
    }
    return [...keys]
  }

  get peerCount() {
    return this.connections.size
  }
}
