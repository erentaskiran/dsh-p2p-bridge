# dsh-p2p-bridge — architecture spec

Status: v1 design, 2026-11-11. Decided by Chief; implementation by Engineer.

## What it is

A DeepSeek Harness plugin that lets bots on **two separate DSH fleets** (Eren's machine and a
friend's machine) talk to each other over an encrypted peer-to-peer network — no central server,
no port forwarding, no shared VPN.

The friend runs the same plugin on their machine. Two fleets, one shared topic, direct sockets.

## File layout

```
dsh-p2p-bridge/
├── package.json          # name, exports, dsh.bundle.patch
├── cordis.patch.yml      # Host row insert
├── index.js              # plugin: tools + wake wiring
├── lib/
│   ├── identity.js       # ed25519 keypair, load/create, mode 600
│   ├── transport.js      # hyperswarm join/leave, socket framing, reconnect
│   ├── protocol.js       # message envelope, validation, size caps
│   ├── inbox.js          # append-only jsonl store, unread cursor
│   └── wake.js           # agent.followup/steer delivery (mirrors monitor)
├── test/
│   ├── protocol.test.js  # envelope validation, oversize reject, bad signature
│   └── e2e.test.js       # two swarms in one process, real message round-trip
└── README.md
```

## Transport — hyperswarm

Verified on this machine 2026-11-11: `hyperswarm@4.x` installs (60 packages) and a two-node
discovery test passed — A and B found each other on a shared topic and exchanged a line of text.

Why hyperswarm over libp2p:
- UDP hole punching is built in and is the default path; no relay needed for most NATs.
- DHT topic lookup means neither side needs the other's IP.
- 60 packages vs libp2p's hundreds; the whole plugin stays auditable.

Topic: `sha256("dsh-p2p-bridge:v1:" + fleetSecret)`. The fleet secret is a 32-byte value both
sides type in once (or scan as a QR). Knowing the topic is not enough to talk — the Noise
handshake still needs both peers to be in, and every application message carries an ed25519
signature checked against a pinned peer key.

## Identity

`~/.dsh/p2p/identity.json`, mode 600:

```json
{ "version": 1, "privateKey": "<base64 ed25519 32B>", "publicKey": "<base64 ed25519 32B>",
  "createdAt": "2026-11-11T10:00:00Z", "label": "eren-free-a1-12g" }
```

Public key is the peer's stable name. First connection to an unknown public key goes into
`~/.dsh/p2p/peers.json` as `status: "pending"`; the user approves with `p2p_trust <fingerprint>`
before any message from it is delivered to an agent. **Fail closed**: an untrusted peer's
messages are dropped, not queued.

## Protocol envelope

Newline-delimited JSON over the Noise stream, one message per line, max 64 KB each.
Anything larger is rejected before parse (read cap on the socket).

```json
{
  "v": 1,
  "id": "<uuid>",
  "kind": "hello" | "msg" | "ack" | "peers" | "ping",
  "from": "<sender publicKey b64>",
  "to": "<recipient publicKey b64>" | "*",
  "ts": "2026-11-11T10:00:00Z",
  "body": { "text": "...", "replyTo": "<id|null>" },
  "sig": "<ed25519 over canonical(header without sig)>"
}
```

Rules:
- `hello` on connect: `{v, id, kind:"hello", from, ts, body:{label, agentName, protocol:1}, sig}`.
  Reject mismatched `v` or missing sig.
- `msg` only delivered when `to` matches this node's public key (or `*` for broadcast, v2).
- `ack` returns the message id; unacked messages retry 3× with backoff, then land in `outbox.jsonl`
  as `undelivered` rather than being lost silently.
- Replay defence: ids seen in the last 24 h are rejected. The window is persisted to `seen.json`
  on every accepted envelope, so a harness restart does not reopen it.

## Tools (what the agent sees)

Four tools, registered at Host level like `monitor`:

| Tool | Args | Returns |
|---|---|---|
| `p2p_status` | `{}` | `{enabled, fingerprint, topicPeers, trustedPeers, unread, listening}` |
| `p2p_peers` | `{}` | `[{fingerprint, label, status, lastSeen}]` |
| `p2p_send` | `{to: fingerprint or "*", text}` | `{messageId, queued\|sent, peerStatus}` |
| `p2p_inbox` | `{limit?, markRead?}` | `[{id, from, fromLabel, ts, text, read}]` |

No tool returns a key. A tool result lands in the model's context and the chat transcript, so the
keypair stays in `identity.json` and the peer table; the wire carries keys in the hello envelope,
where the other node needs them, and nowhere else. Address peers by fingerprint.

No tool can send to a peer with `status: "pending"` — `p2p_send` fails with
`"peer not trusted: run p2p_trust <fingerprint>"`.

## Wake behaviour

Mirror of the monitor plugin (`/home/ubuntu/Documents/deepseek-harness/default-workspace/monitor-bundle/index.js:354-381`):

```js
const owner = ctx.agents.get(sessionId)
if (owner === undefined) return
if (owner.status === 'idle') owner.followup(message)
else owner.steer(message)
```

An incoming message wakes the agent in its own session with a short notice, not the whole body:

```
[P2P] mesaj: <fromLabel> (<fingerprint>) — "ilk 120 karakter…" — p2p_inbox ile oku
```

Full text stays in `p2p_inbox` so the wake note is cheap and the agent decides whether to spend
context on it.

## Storage

`~/.dsh/p2p/`:
- `identity.json` (600) — keypair
- `peers.json` (600) — trusted + pending peer table
- `inbox.jsonl` — append-only, one message per line, `read` flag rewritten on ack
- `outbox.jsonl` — undelivered after retries
- `seen.json` — replay-defence id set, 24 h window

Append-only jsonl, not sqlite: no native dep, trivially inspectable, and the volume here is
tens of messages a day, not thousands.

## Config (cordis.patch.yml)

```yaml
- insert:
    - id: p2p-bridge
      name: 'dsh-p2p-bridge'
      config:
        enabled: false            # off until the user sets a fleet secret
        fleetSecretEnv: DSH_P2P_FLEET_SECRET
        autoStart: true
        maxMessageBytes: 65536
        wakeOnMessage: true
        wakeSummaryChars: 120
        retryAttempts: 3
        seenTtlHours: 24
```

`enabled: false` by default: the plugin installs, registers tools, and answers `p2p_status` with
`{enabled: false}` — nothing joins the DHT until a fleet secret exists. That keeps the install
safe before the friend is involved.

## Security decisions, stated plainly

1. **Fail closed on unknown identity.** A peer that is not in `peers.json` as trusted cannot
   deliver a message to an agent. No auto-trust.
2. **The fleet secret is not a password into the bot.** It only produces the DHT topic. A peer
   still needs its ed25519 key approved.
3. **No file transfer in v1.** Text only, 64 KB cap. Attachments are a separate decision.
4. **No broadcast in v1.** `to: "*"` is accepted by the schema and refused by the handler with
   "broadcast is v2". The wake fan-out on the receiving side needs a policy the user has not set.
5. **The plugin never prints keys.** `p2p_status` shows a fingerprint (first 16 hex of sha256 of
   the public key), never the key itself.
6. **Every message is logged locally** for audit — path, timestamp, peer, size — in `inbox.jsonl`.

## Test plan

1. `protocol.test.js` — envelope validation: bad version, missing sig, oversize body, replay id,
   `to` mismatch. Each must reject.
2. `e2e.test.js` — two swarms in one process on a random topic; A sends, B receives, B acks, A's
   outbox drains. Then a negative control: an untrusted peer's message is dropped and `inbox.jsonl`
   stays empty.
3. Manual, two real machines: install on this box and on the friend's, swap fingerprints out of
   band, send one message each way. Record the transcript.

## Out of scope for v1

Broadcast, file transfer, group rooms, relay-only mode for symmetric NATs, desktop UI. Each is a
follow-up with its own decision.
