#!/usr/bin/env node
/**
 * Approve a peer by fingerprint — the person's half of the trust rule.
 *
 * Trust is deliberately not an agent tool: `p2p_send` refuses an untrusted peer and an untrusted
 * peer's traffic is dropped, so the approval has to come from a human at a keyboard, outside the
 * model's reach. This is that keyboard.
 *
 *   node bin/p2p-trust.js <fingerprint> [--label "friend-box"] [--dir ~/.dsh/p2p]
 *   node bin/p2p-trust.js --list
 *   node bin/p2p-trust.js --untrust <fingerprint>
 *
 * Exit codes: 0 done, 1 no such fingerprint, 2 usage.
 */
import { PeerTable } from '../lib/inbox.js'
import { defaultDir } from '../lib/identity.js'

function usage() {
  console.error(`usage: p2p-trust <fingerprint> [--label <name>] [--dir <path>]
       p2p-trust --list [--dir <path>]
       p2p-trust --untrust <fingerprint> [--dir <path>]

Approve a peer so its messages may reach an agent. The fingerprint is the 16 hex characters
p2p_status / p2p_peers shows. Get it from the other person over a channel you already trust.`)
}

function parseArgs(argv) {
  const out = { dir: defaultDir(), list: false, untrust: undefined, label: undefined, fingerprint: undefined }
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (arg === '--list') out.list = true
    else if (arg === '--dir') out.dir = argv[++index]
    else if (arg === '--label') out.label = argv[++index]
    else if (arg === '--untrust') out.untrust = argv[++index]
    else if (arg === '-h' || arg === '--help') return undefined
    else if (out.fingerprint === undefined) out.fingerprint = arg
    else return undefined
  }
  return out
}

function main(argv) {
  const args = parseArgs(argv)
  if (args === undefined) {
    usage()
    return 2
  }
  const table = new PeerTable({ dir: args.dir })

  if (args.list) {
    const peers = table.list()
    if (peers.length === 0) {
      console.log('no peers seen yet')
      return 0
    }
    for (const peer of peers) {
      console.log(`${peer.fingerprint}  ${peer.status.padEnd(7)}  ${peer.label || '(no label)'}  first seen ${peer.firstSeen}  last seen ${peer.lastSeen}`)
    }
    return 0
  }

  if (args.untrust !== undefined) {
    const peer = table.byFingerprint(args.untrust)
    if (peer === undefined) {
      console.error(`no peer with fingerprint ${args.untrust}; run --list to see the table`)
      return 1
    }
    peer.status = 'pending'
    table.save()
    console.log(`${peer.fingerprint} is pending again: its messages will be dropped`)
    return 0
  }

  if (args.fingerprint === undefined) {
    usage()
    return 2
  }
  const peer = table.byFingerprint(args.fingerprint)
  if (peer === undefined) {
    console.error(`no peer with fingerprint ${args.fingerprint}: it appears in p2p_peers after it first connects`)
    return 1
  }
  peer.status = 'trusted'
  if (args.label) table.setLabel(peer.publicKey, args.label)
  table.save()
  console.log(`${peer.fingerprint} (${peer.label || 'no label'}) is trusted: its messages will be delivered`)
  return 0
}

process.exit(main(process.argv.slice(2)))
