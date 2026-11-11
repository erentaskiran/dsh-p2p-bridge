/**
 * Ed25519 identity for one DSH fleet.
 *
 * The keypair is the fleet's stable name on the network: `publicKey` is what a peer pins, and the
 * fingerprint below is the short form a human reads out loud. The file is created once and read on
 * every start; its permissions are enforced, not trusted, because a group-readable private key is a
 * silent hole rather than a visible bug.
 *
 * No module here reaches the network or the plugin context, so it is testable on its own.
 */
import {
  createHash,
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign as edSign,
  verify as edVerify,
} from 'node:crypto'
import { chmodSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

const DIR_MODE = 0o700
const FILE_MODE = 0o600

/** `~/.dsh/p2p` — or `$DSH_HOME/p2p` when the host sets one (the Bot host does). */
export function defaultDir() {
  const home = process.env.DSH_HOME || join(homedir(), '.dsh')
  return join(home, 'p2p')
}

function b64urlToBuf(text) {
  return Buffer.from(String(text), 'base64url')
}

function bufToB64url(buf) {
  return Buffer.from(buf).toString('base64url')
}

/** The short, human-checkable name of a public key: first 16 hex of sha256 over the raw 32 bytes. */
export function fingerprint(publicKeyB64) {
  const raw = Buffer.from(String(publicKeyB64), 'base64')
  return createHash('sha256').update(raw).digest('hex').slice(0, 16)
}

function privateKeyObject(seed, publicKey) {
  return createPrivateKey({
    key: { kty: 'OKP', crv: 'Ed25519', d: bufToB64url(seed), x: bufToB64url(publicKey) },
    format: 'jwk',
  })
}

function publicKeyObject(publicKey) {
  return createPublicKey({ key: { kty: 'OKP', crv: 'Ed25519', x: bufToB64url(publicKey) }, format: 'jwk' })
}

/** A fresh keypair, already in the stored shape. */
export function createIdentity(label = '') {
  const { publicKey, privateKey } = generateKeyPairSync('ed25519')
  const jwkPrivate = privateKey.export({ format: 'jwk' })
  const jwkPublic = publicKey.export({ format: 'jwk' })
  const publicRaw = b64urlToBuf(jwkPublic.x)
  return {
    version: 1,
    privateKey: b64urlToBuf(jwkPrivate.d).toString('base64'),
    publicKey: publicRaw.toString('base64'),
    createdAt: new Date().toISOString(),
    label: String(label).slice(0, 64),
  }
}

/** Signs arbitrary bytes with the stored private key. Returns base64. */
export function sign(identity, bytes) {
  const seed = Buffer.from(identity.privateKey, 'base64')
  const pub = Buffer.from(identity.publicKey, 'base64')
  return edSign(null, bytes, privateKeyObject(seed, pub)).toString('base64')
}

/** True when `signatureB64` is that identity's signature over `bytes`. A malformed key or sig is false, not a throw. */
export function verify(publicKeyB64, bytes, signatureB64) {
  try {
    const pub = Buffer.from(String(publicKeyB64), 'base64')
    if (pub.length !== 32) return false
    const sig = Buffer.from(String(signatureB64), 'base64')
    if (sig.length !== 64) return false
    return edVerify(null, bytes, publicKeyObject(pub), sig)
  } catch {
    return false
  }
}

function enforceMode(path) {
  try {
    const mode = statSync(path).mode & 0o777
    if (mode !== FILE_MODE) chmodSync(path, FILE_MODE)
  } catch {
    /* a file that vanished mid-check is handled by the caller's read */
  }
}

function writePrivate(path, data) {
  const tmp = `${path}.${process.pid}.tmp`
  writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`, { mode: FILE_MODE })
  chmodSync(tmp, FILE_MODE)
  renameSync(tmp, path)
}

/**
 * Loads the identity, creating it on first use.
 *
 * A file that cannot be parsed is not silently replaced: regenerating a key would change the fleet's
 * identity and break every peer that pinned it, so the caller gets an error it can show the user.
 */
export function loadIdentity({ dir = defaultDir(), label = '' } = {}) {
  mkdirSync(dir, { recursive: true, mode: DIR_MODE })
  try {
    chmodSync(dir, DIR_MODE)
  } catch {
    /* an existing directory owned by someone else is their business */
  }
  const path = join(dir, 'identity.json')
  let raw
  try {
    raw = JSON.parse(readFileSync(path, 'utf8'))
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw new Error(`cannot read identity ${path}: ${error.message}`)
    }
    raw = createIdentity(label)
    writePrivate(path, raw)
    return withFingerprint(raw, path)
  }
  if (raw?.version !== 1 || typeof raw.privateKey !== 'string' || typeof raw.publicKey !== 'string') {
    throw new Error(`identity ${path} is not a v1 identity (version ${raw?.version})`)
  }
  const seed = Buffer.from(raw.privateKey, 'base64')
  const pub = Buffer.from(raw.publicKey, 'base64')
  if (seed.length !== 32 || pub.length !== 32) {
    throw new Error(`identity ${path} has a malformed key (seed ${seed.length}B, public ${pub.length}B)`)
  }
  enforceMode(path)
  return withFingerprint(raw, path)
}

function withFingerprint(identity, path) {
  return { ...identity, fingerprint: fingerprint(identity.publicKey), path }
}
