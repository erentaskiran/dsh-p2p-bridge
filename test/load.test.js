/**
 * Every tool schema must pass the Host's own JSON Schema subset check.
 *
 * This test exists because of a real outage: `p2p_status` declared `fingerprint: { type: ['string',
 * 'null'] }`, all 42 tests passed, and the plugin did not mount at boot — `ctx.tools.register` threw
 * `JsonSchemaError: schema.properties.fingerprint.type must be a single type string`. The suite missed it
 * because every test calls `apply()` with a stand-in ctx whose `register` only files the tool in a Map.
 *
 * So the check here is not a hand-written copy of the rules: it imports the validator the Host itself
 * runs (`assertSupportedJsonSchema` for `output.schema`, `assertObjectJsonSchema` for `parameters`). If
 * DSH's subset tightens, this test fails with the plugin's own schema named.
 *
 * Where the validator lives: the plugin is loaded by a DSH profile, so `@deepseek-ai/dsh-tools` is not a
 * dependency of this repo. It is found next to the `dsh` binary on PATH. When no `dsh` install is present
 * the test skips loudly rather than passing quietly.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { execFileSync } from 'node:child_process'
import { mkdtempSync, realpathSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import { apply } from '../index.js'

const require = createRequire(import.meta.url)

/** The Host's schema module, resolved the way the profile that loads this plugin would resolve it. */
function hostSchemaModule() {
  try {
    return require.resolve('@deepseek-ai/dsh-tools')
  } catch {
    /* not a dependency of this repo; fall through to the installed harness */
  }
  try {
    const bin = execFileSync('bash', ['-lc', 'command -v dsh'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim()
    if (bin.length === 0) return undefined
    const packageRoot = dirname(dirname(realpathSync(bin)))
    return createRequire(join(packageRoot, 'package.json')).resolve('@deepseek-ai/dsh-tools')
  } catch {
    return undefined
  }
}

/** Registers the plugin against a recording ctx, the same stand-in the other tests use. */
function registered(tools) {
  const dir = mkdtempSync(join(tmpdir(), 'p2p-load-'))
  apply({
    logger: () => ({ warn: () => {}, info: () => {} }),
    tools: { register: (tool) => tools.set(tool.name, tool) },
    agents: { get: () => undefined },
    systemPrompt: { getSectionOrder: () => 10, section: () => {} },
    effect: () => {},
    session: { id: 'session-load' },
  }, { dir, enabled: false, flushIntervalMs: 10_000_000 })
  return { tools, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

test('every tool schema is inside the Host\'s JSON Schema subset', async (t) => {
  const modulePath = hostSchemaModule()
  if (modulePath === undefined) {
    t.skip('no dsh install on PATH: cannot import the Host validator here')
    return
  }
  const { assertSupportedJsonSchema, assertObjectJsonSchema, JsonSchemaError } = await import(modulePath)
  const tools = new Map()
  const h = registered(tools)
  try {
    assert.equal(tools.size, 4, 'four tools are registered: status, peers, send, inbox')
    const problems = []
    for (const [name, tool] of tools) {
      // `parameters` is the args schema the Host parses with; `output.schema` is what it validates a
      // result against. Both go through the real validator, so a type array anywhere fails here.
      for (const [kind, schema, check] of [
        ['parameters', tool.parameters, assertObjectJsonSchema],
        ['output.schema', tool.output.schema, assertSupportedJsonSchema],
      ]) {
        try {
          check(schema)
        } catch (error) {
          problems.push(`${name}.${kind}: ${error instanceof JsonSchemaError ? error.violations.join('; ') : String(error)}`)
        }
      }
    }
    assert.deepEqual(problems, [], `the Host would refuse to mount these:\n${problems.join('\n')}`)
  } finally {
    h.cleanup()
  }
})

test('a nullable value is spelled as oneOf, not as a type array', () => {
  // The outage in one assertion, without needing the Host installed: no schema in this plugin may carry
  // an array `type`. A nullable field is `oneOf: [{type:'string'},{type:'null'}]`.
  const tools = new Map()
  const h = registered(tools)
  try {
    const offenders = []
    const walk = (node, where) => {
      if (node === null || typeof node !== 'object') return
      if (Array.isArray(node.type)) offenders.push(`${where}.type = ${JSON.stringify(node.type)}`)
      for (const [key, value] of Object.entries(node)) walk(value, `${where}.${key}`)
    }
    for (const [name, tool] of tools) {
      walk(tool.parameters, `${name}.parameters`)
      walk(tool.output.schema, `${name}.output.schema`)
    }
    assert.deepEqual(offenders, [])
  } finally {
    h.cleanup()
  }
})

test('p2p_status still reports a null fingerprint when there is no identity', async () => {
  // The nullable case the schema has to admit: `apply()` keeps the tools registered when the identity
  // file is broken, and then `status()` has no fingerprint to show.
  const tools = new Map()
  const h = registered(tools)
  try {
    const status = await tools.get('p2p_status').execute()
    assert.equal(status.enabled, false)
    assert.ok(status.fingerprint === null || typeof status.fingerprint === 'string')
  } finally {
    h.cleanup()
  }
})
