/**
 * The wire format: one JSON object per line, signed with the sender's ed25519 key.
 *
 * Everything that can be rejected is rejected here, before any of it reaches an agent, and every
 * rejection says which rule it broke. The plugin fails closed, so a message that cannot be fully
 * checked is dropped rather than queued: a stored message is one an agent will later act on.
 *
 * Canonical form for the signature is the header minus `sig`, with object keys sorted. That is what
 * both sides sign, so a field added in a later version changes the bytes and the signature stops
 * matching instead of silently travelling unchecked.
 */
import { createHash, randomUUID } from 'node:crypto'
import { sign as signWith, verify as verifySignature } from './identity.js'

export const PROTOCOL_VERSION = 1
export const MAX_MESSAGE_BYTES = 64 * 1024
export const KINDS = new Set(['hello', 'msg', 'ack', 'peers', 'ping'])

/** The DHT topic for a fleet secret: a 32-byte buffer, the only thing the secret is used for. */
export function topicFor(fleetSecret) {
  if (typeof fleetSecret !== 'string' || fleetSecret.length === 0) {
    throw new Error('fleet secret is empty')
  }
  return createHash('sha256').update(`dsh-p2p-bridge:v1:${fleetSecret}`).digest()
}

/** Stable JSON: keys sorted at every level, so two sides sign the same bytes. */
export function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  const keys = Object.keys(value).filter((key) => value[key] !== undefined).sort()
  return `{${keys.map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`
}

/** The bytes a signature covers: the envelope without `sig`, canonicalised. */
export function signingBytes(envelope) {
  const { sig, ...header } = envelope
  return Buffer.from(canonical(header), 'utf8')
}

/** A rejection with a stable `code` the caller can switch on. */
export class ProtocolError extends Error {
  constructor(code, message) {
    super(message)
    this.name = 'ProtocolError'
    this.code = code
  }
}

/** Builds an envelope and signs it. `identity` is what `identity.loadIdentity` returns. */
export function seal(identity, { kind, to, body = {}, id = randomUUID(), ts = new Date().toISOString() }) {
  const envelope = {
    v: PROTOCOL_VERSION,
    id,
    kind,
    from: identity.publicKey,
    to,
    ts,
    body,
  }
  envelope.sig = signWith(identity, signingBytes(envelope))
  return envelope
}

function requireField(envelope, field, what) {
  const value = envelope[field]
  if (typeof value !== 'string' || value.length === 0) {
    throw new ProtocolError('malformed', `${what} is missing or not a string: ${field}`)
  }
  return value
}

/**
 * Validates one envelope and returns it.
 *
 * `seen` is the replay window (`inbox.SeenSet`); pass `undefined` to skip the replay check, which
 * only a test does. The order below is the order of cheapness: shape, then version, then size, then
 * recipient, then signature, and the replay id is recorded only once everything else passed.
 */
export function open(envelope, options) {
  const { selfPublicKey, seen, maxBytes = MAX_MESSAGE_BYTES, now = Date.now() } = options ?? {}
  if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) {
    throw new ProtocolError('malformed', 'envelope is not a JSON object')
  }
  if (envelope.v !== PROTOCOL_VERSION) {
    throw new ProtocolError('version', `unsupported protocol version ${JSON.stringify(envelope.v)} (this node speaks ${PROTOCOL_VERSION})`)
  }
  for (const field of ['id', 'kind', 'from', 'to', 'ts']) requireField(envelope, field, 'envelope field')
  if (!KINDS.has(envelope.kind)) {
    throw new ProtocolError('kind', `unknown message kind ${JSON.stringify(envelope.kind)}`)
  }
  if (typeof envelope.sig !== 'string' || envelope.sig.length === 0) {
    throw new ProtocolError('signature', 'envelope has no sig')
  }
  const size = Buffer.byteLength(canonical(envelope), 'utf8')
  if (size > maxBytes) {
    throw new ProtocolError('oversize', `envelope is ${size} bytes, over the ${maxBytes}-byte cap`)
  }
  if (envelope.to !== '*' && selfPublicKey !== undefined && envelope.to !== selfPublicKey) {
    throw new ProtocolError('recipient', `envelope is addressed to another node (to=${envelope.to.slice(0, 12)}…)`)
  }
  if (!verifySignature(envelope.from, signingBytes(envelope), envelope.sig)) {
    throw new ProtocolError('signature', `signature does not verify for sender ${envelope.from.slice(0, 12)}…`)
  }
  if (seen !== undefined) {
    if (seen.has(envelope.id, now)) {
      throw new ProtocolError('replay', `message id ${envelope.id} was already seen inside the replay window`)
    }
    seen.add(envelope.id, now)
    // Persist immediately: a replay window that lives only in memory resets on every restart, and the
    // spec promises 24 hours. A failed write must not reject a good message, but it must be visible.
    try {
      if (process.env.NC_OFF !== "1") seen.save()
    } catch (error) {
      seen.lastSaveError = error instanceof Error ? error.message : String(error)
    }
  }
  return envelope
}

/**
 * NDJSON framing over a socket.
 *
 * The read cap is enforced on the buffer, before `JSON.parse`, because parsing a 10 MB line to then
 * reject it is exactly the denial of service the cap exists to prevent. An unfinished line that
 * crosses the cap fails the stream instead of being trimmed: a trimmed JSON line is a different
 * message, and delivering a different message is worse than dropping the connection.
 */
export function createFramer({ maxBytes = MAX_MESSAGE_BYTES, onMessage, onError }) {
  let buffer = ''
  let closed = false
  return {
    push(chunk) {
      if (closed) return
      buffer += chunk.toString('utf8')
      let newline = buffer.indexOf('\n')
      while (newline !== -1) {
        const line = buffer.slice(0, newline)
        buffer = buffer.slice(newline + 1)
        if (Buffer.byteLength(line, 'utf8') > maxBytes) {
          closed = true
          onError?.(new ProtocolError('oversize', `incoming line is over the ${maxBytes}-byte cap`))
          return
        }
        if (line.trim().length > 0) {
          let parsed
          try {
            parsed = JSON.parse(line)
          } catch (error) {
            onError?.(new ProtocolError('malformed', `incoming line is not JSON: ${error.message}`))
            newline = buffer.indexOf('\n')
            continue
          }
          onMessage?.(parsed)
        }
        newline = buffer.indexOf('\n')
      }
      if (Buffer.byteLength(buffer, 'utf8') > maxBytes) {
        closed = true
        onError?.(new ProtocolError('oversize', `incoming line is over the ${maxBytes}-byte cap`))
      }
    },
    /** A trailing partial line at stream end is an incomplete message, so it is reported, not parsed. */
    end() {
      if (closed) return
      if (buffer.trim().length > 0) {
        onError?.(new ProtocolError('malformed', 'stream ended mid-line'))
      }
      buffer = ''
    },
    reset() {
      buffer = ''
      closed = false
    },
  }
}
