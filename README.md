# dsh-p2p-bridge

Encrypted peer-to-peer messaging between **two DeepSeek Harness fleets** — your bots and a friend's,
with no central server, no port forwarding and no shared VPN.

Each side runs this plugin on its own machine. Both join one DHT topic derived from a fleet secret
you both type in once; after that the two fleets find each other directly, punch through NAT with
hyperswarm, and exchange signed messages. Your bots reach the other fleet through four tools, and an
incoming message wakes the agent with a one-line notice.

- **Identity:** an ed25519 keypair per fleet, created on first use at `~/.dsh/p2p/identity.json`
  (mode 600). The public key's fingerprint is the peer's name.
- **Trust:** pins only. A key you have not approved stays `pending`, and its traffic is dropped —
  never queued, never delivered.
- **Size:** text only, 64 KB per message, newline-delimited JSON over the encrypted stream.

## Install

```bash
cd /path/to/dsh-p2p-bridge
npm install
```

Then link it into a profile, the same way other local plugins are linked:

```json
{
  "dependencies": { "dsh-p2p-bridge": "link:/absolute/path/to/dsh-p2p-bridge" },
  "dsh": { "profile": { "bundles": ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "dsh-p2p-bridge"] } }
}
```

Restarting the harness is what loads a new bundle row; the plugin ships `enabled: false`, so it
registers its tools and joins nothing until you set a secret.

## Pairing two machines

1. **Make a fleet secret** on either machine — one value, shared out of band (a password manager, a
   voice call, a QR code):

   ```bash
   openssl rand -base64 32
   ```

2. **Put it in the environment** of the harness on *both* machines:

   ```bash
   export DSH_P2P_FLEET_SECRET='<the same 32-byte value>'
   ```

3. **Turn the plugin on** in the profile's `cordis.patch.yml` (`enabled: true`) and restart the
   harness once.
4. **Swap fingerprints out of band.** `p2p_status` prints this fleet's fingerprint (16 hex
   characters); send it to the friend over a channel you already trust, and read theirs back.
5. **Trust the peer.** An unknown key appears in `p2p_peers` as `pending` the first time it connects.
   Approve it by fingerprint — a person's decision, deliberately not an agent tool, so it is made at a
   keyboard:

   ```bash
   node bin/p2p-trust.js --list                    # see pending and trusted peers
   node bin/p2p-trust.js <fingerprint> --label friend-box
   node bin/p2p-trust.js --untrust <fingerprint>   # take the approval back
   ```

   Until that happens, that peer cannot wake an agent and its messages are not stored.

## The four tools

| Tool | Arguments | Returns |
|---|---|---|
| `p2p_status` | `{}` | `{enabled, fingerprint, topicPeers, trustedPeers, unread, listening}` |
| `p2p_peers` | `{}` | `[{fingerprint, label, status, lastSeen}]` |
| `p2p_send` | `{to: fingerprint, text}` | `{messageId, queued, sent, peerStatus}` |
| `p2p_inbox` | `{limit?, markRead?}` | `[{id, from, fromLabel, ts, text, read}]` |

A message from a trusted peer is stored in `inbox.jsonl` and the owning session is woken with a short
notice — `[P2P] mesaj: <label> (<fingerprint>) — "<first 120 chars>" — p2p_inbox ile oku` — so the
context cost of an arriving message is a line, not a body. The full text is one `p2p_inbox` call away.

## Security

- **A message never goes to an unknown peer.** An unapproved key is `pending`: its traffic is dropped
  on arrival, and `p2p_send` refuses it with `peer not trusted: run p2p_trust <fingerprint>`.
- **The fleet secret is not a password into your bots.** It only derives the DHT topic. Knowing it lets
  someone *find* the swarm; they still need their ed25519 key approved before a single message is
  delivered. Treat it as a rendezvous point, not as an authorization.
- **No broadcast in v1.** `to: "*"` is refused with "broadcast is v2".
- **No file transfer in v1.** Text only.
- **No tool returns a key.** Tool results land in the model's context and the transcript, so
  `p2p_status` and `p2p_peers` show fingerprints only, and peers are addressed by fingerprint. The key
  travels on the wire in the hello envelope, where the other node needs it; the private half never
  leaves `~/.dsh/p2p/identity.json` (mode 600, enforced on every load).
- **Every message is logged locally** in `inbox.jsonl` for audit: path, time, peer and size.

## Run the tests

```bash
npm test
```

`npm test` runs `node --test 'test/*.test.js'`. Pass the **glob**, not the directory: on Node 24
`node --test test/` resolves `test` as a module path and fails with `Cannot find module '.../test'`,
so either use the glob or name the files:

```bash
node --test test/protocol.test.js test/e2e.test.js test/restart.test.js
```

The suite is 45 tests in 10 files; several are negative controls that must fail when the guard they
cover is removed (signature check, trust gate, ack handling, replay persistence, the flush call, the
stale-socket drop, the liveness ping). Removing a guard and watching the suite stay green means the
guard is untested, not that it is unnecessary. `test/guards.test.js` pins the guard call sites and
fails the suite when a negative control was left switched on in the tree.

A full run takes about 55 s, most of it connection setup. Parallel runs are the ones that can be slow:
`node --test` executes files concurrently, so several DHT joins compete at once and a pair can take
longer than the 60 s budget. If you see a rare `timed out waiting for both connected` under load, that
is the harness, not the bridge — rerun serially with `node --test --test-concurrency=1 'test/*.test.js'`
before believing it.

Two fleets in two **separate processes**, each with its own `DSH_HOME` and state folder, are covered by
a script rather than a test, because it spawns children:

```bash
node scripts/two-node-demo.mjs                     # prints a JSON report and "TWO-NODE E2E: PASS"
P2P_REJECT_SENDER=1 node scripts/two-node-demo.mjs # negative control: receiver withholds trust
```

What the files cover:

- `test/protocol.test.js` — envelope rules: wrong version, missing signature, forged signature,
  oversize body, replayed id, wrong recipient, unknown kind, canonical signing bytes, the read cap.
- `test/e2e.test.js` — two real swarms in one process: A sends, B receives and acks, A's outbox
  drains, the wake notice carries only a preview; then the negative control, where an untrusted
  peer's message arrives and is dropped with an empty inbox.
- `test/restart.test.js` — the replay window across a process boundary, with a negative control that
  turns persistence off and shows the id being accepted again.
- `test/retry.test.js` — the retry ladder against real sockets: a failed ack is queued, the periodic
  flush moves it, and a message queued before the peer was verified is retried once a socket exists.
- `test/steady.test.js` — Test A: fifty messages down **one** established connection, each delivered
  once, acked once, one wake each. If this fails the bug is ours; if it passes and the two-process
  demo still loses runs, the loss is in connection setup.
- `test/flush.test.js`, `test/flush-stress.test.js` — the flush and give-up path driven directly, no
  timer and no network; the stress file repeats it 200 times to make a rare state show up.
- `test/status.test.js` — the payload shapes of `p2p_status`, `p2p_peers`, `p2p_inbox`: fingerprints,
  never keys.
- `test/load.test.js` — every tool schema against the Host's own JSON Schema subset, so a schema the
  real registrar would refuse cannot pass here while the plugin fails to mount.
- `test/guards.test.js` — the guard call sites still exist, and no negative control is left switched
  on in the working tree.

## Storage

`~/.dsh/p2p/` (or `$DSH_HOME/p2p` when the host sets one):

| File | Contents |
|---|---|
| `identity.json` | ed25519 keypair, mode 600 |
| `peers.json` | peer table: `pending` and `trusted`, with labels and last-seen |
| `inbox.jsonl` | delivered messages, one JSON object per line, `read` flag rewritten on read |
| `outbox.jsonl` | messages no live socket took, flushed when that peer connects |
| `seen.json` | replay window: ids seen in the last 24 h |

Append-only JSONL rather than a database: this is tens of messages a day, and a file you can open is
worth more than a schema.

## What it does not do

Stated here so nobody discovers it at the worst moment:

- **No broadcast.** `to: "*"` is refused with "broadcast is v2". One message, one trusted peer.
- **No file transfer.** Text only, and 64 KB is the whole budget per message. There is no attachment
  path, no chunking and no resume.
- **No group rooms.** Two fleets, point to point. A third machine is a third pairing.
- **No relay mode.** Two peers behind symmetric NATs that cannot hole-punch will not connect; v1 has
  no fallback, so both sides need a network where UDP hole punching works.
- **No desktop UI.** The surface is the four tools, `bin/p2p-trust.js`, and the jsonl files.
- **No auto-trust.** A new peer stays `pending` until a person approves its fingerprint; nothing in
  the plugin upgrades it.
- **It is not a way to share a model or a session.** Each fleet's agents stay in their own sessions;
  what crosses the wire is a short signed text message.

## Status

v1, as designed in [SPEC.md](SPEC.md).
