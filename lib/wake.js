/**
 * Waking an agent with an incoming message.
 *
 * Mirrors the monitor plugin's delivery exactly (`monitor-bundle/index.js:354-381`): a live owner
 * gets a `followup` when idle and a `steer` when it is mid-turn, and an owner that is not there is
 * not woken at all. The difference is what the message says: only a short notice with the peer and a
 * preview, never the body, so a chatty peer cannot spend this fleet's context.
 */
import { randomUUID } from 'node:crypto'

/** `[P2P] mesaj: <label> (<fingerprint>) — "<preview>" — p2p_inbox ile oku` */
export function wakeText({ label, fingerprint, text, summaryChars = 120 }) {
  const preview = String(text ?? '').replace(/\s+/g, ' ').trim().slice(0, summaryChars)
  const who = label && label.length > 0 ? label : 'unknown peer'
  const print = fingerprint && fingerprint.length > 0 ? fingerprint : 'no fingerprint'
  return `[P2P] mesaj: ${who} (${print}) — "${preview}" — p2p_inbox ile oku`
}

/** One steerable message, shaped like every other injected notice. */
export function wakeMessage(payload) {
  const text = typeof payload === 'string' ? payload : wakeText(payload)
  return {
    id: randomUUID(),
    role: 'user',
    content: [{ type: 'text', text }],
    source: { kind: 'dsh-p2p-bridge', form: 'notice', summary: 'P2P message received' },
  }
}

/**
 * Delivers the notice to `sessionId`, if that session has a live agent.
 *
 * Returns `false` when nobody is listening: the message is already in the inbox, so a session that
 * is not running loses nothing, and starting it just to announce a message would wake an agent for
 * a task it did not ask for.
 */
export function deliverWake(ctx, sessionId, payload) {
  const owner = ctx.agents.get(sessionId)
  if (owner === undefined) return false
  const message = wakeMessage(payload)
  if (owner.status === 'idle') owner.followup(message)
  else owner.steer(message)
  return true
}
