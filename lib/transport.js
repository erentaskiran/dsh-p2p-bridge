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
  startKeepAlive(keepAliveMs = 12_000) {
    if (this.keepAliveTimer !== undefined) return
    this.keepAliveTimer = setInterval(() => {
      if (this.destroyed) return
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
    const peer = { uid, socket, hex, publicKey: undefined, attempts: this.attempts.get(hex) ?? 0 }
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
        peer.socket.write(line)
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
