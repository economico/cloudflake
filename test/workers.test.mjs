// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

import assert from 'node:assert/strict'
import { test } from 'vite-plus/test'
import { simulateWorkers } from 'cloudflake/miniflare'

// The KV marker and R2 content cross real Worker and service-binding boundaries.
const front = `
export default { async fetch(req, env) {
  await env.CACHE.put("latest", await req.text())
  return env.BACKEND.fetch("https://backend/commit", {
    method: "POST", body: await env.CACHE.get("latest")
  })
} }`
const backend = `
export default { async fetch(req, env) {
  const value = await req.text()
  await env.FILES.put(value, value)
  return new Response("ok")
} }`

test('real Workers, service binding, KV and R2 start fresh on replay', async () => {
  async function run() {
    const trace = []
    await simulateWorkers(
      { seed: 9, onStep: (e) => trace.push(e) },
      () => ({
        workers: [
          {
            name: 'front',
            modules: true,
            script: front,
            compatibilityDate: '2025-01-01',
            kvNamespaces: ['CACHE'],
            serviceBindings: { BACKEND: 'backend' },
          },
          {
            name: 'backend',
            modules: true,
            script: backend,
            compatibilityDate: '2025-01-01',
            r2Buckets: ['FILES'],
          },
        ],
      }),
      async (sim, runtime) => {
        const bucket = await runtime.getR2Bucket('FILES', 'backend')
        assert.equal((await bucket.list()).objects.length, 0)
        const value = String(sim.int(10000))
        sim.deliver(
          'upload',
          async () => {
            const r = await runtime.dispatchFetch('https://front/', { method: 'POST', body: value })
            assert.equal(await r.text(), 'ok')
          },
          { duplicate: 1, maxDelay: 10 },
        )
        sim.after(11, 'verify persisted content', async () => {
          assert.equal((await bucket.list()).objects.length, 1)
          assert.equal(await (await bucket.get(value)).text(), value)
        })
      },
    )
    return trace
  }
  assert.deepEqual(await run(), await run())
})

test('a real queue handler retries on the virtual calendar and commits to D1 once', async () => {
  const { workerQueue } = await import('cloudflake/miniflare')
  const times = []
  await simulateWorkers(
    { seed: 7, onStep: (e) => times.push(e.time) },
    () => ({
      name: 'consumer',
      modules: true,
      compatibilityDate: '2026-07-01',
      d1Databases: ['DB'],
      script: `
      export default { async queue(batch, env) {
        const [m] = batch.messages
        if (m.attempts === 1) { m.retry({delaySeconds: 60}); return }
        await env.DB.prepare("INSERT INTO inbox VALUES (?,?)").bind(m.id,m.body.value).run()
        m.ack()
      } }`,
    }),
    async (sim, runtime) => {
      const db = await runtime.getD1Database('DB')
      await db.exec('CREATE TABLE inbox (id TEXT PRIMARY KEY, value INTEGER)')
      const queue = await workerQueue(sim, runtime, 'consumer', 'effects')
      await queue.send({ value: 12 })
      sim.after(60001, 'verify', async () => {
        assert.equal(await db.prepare('SELECT value FROM inbox').first('value'), 12)
      })
    },
  )
  assert.deepEqual(times, [0, 0, 60000, 60001])
})

test('pauses live Worker requests at a binding and reproduces a lost update', async () => {
  const { client } = await import('cloudflake')
  const script = `
    export default { async fetch(req, env) {
      const count = Number(await env.KV.get("count"))
      await env.CHECKPOINT.fetch(req.url)
      await env.KV.put("count", String(count + 1))
      return new Response("ok")
    } }`
  async function reproduce() {
    const gates = new Map()
    return simulateWorkers(
      { seed: 18 },
      () => ({
        script,
        modules: true,
        compatibilityDate: '2026-07-01',
        kvNamespaces: ['KV'],
        serviceBindings: {
          CHECKPOINT: async (req) => {
            await gates.get(new URL(req.url).pathname)(1, 'after reading count')
            return new Response()
          },
        },
      }),
      async (sim, runtime) => {
        for (const path of ['/a', '/b'])
          client(sim, path, async (checkpoint) => {
            gates.set(path, checkpoint)
            const response = await runtime.dispatchFetch('https://test' + path)
            assert.equal(await response.text(), 'ok')
          })
        const kv = await runtime.getKVNamespace('KV')
        sim.after(2, 'both increments persisted', async () => {
          assert.equal(await kv.get('count'), '2')
        })
      },
    )
  }
  const failure = await reproduce().then(
    () => assert.fail('race undetected'),
    (e) => e,
  )
  assert.equal(failure.phase, 'both increments persisted')
  assert.equal(failure.cause.actual, '1')
  await assert.rejects(reproduce(), (e) => {
    assert.deepEqual(e.trace, failure.trace)
    assert.equal(e.cause.actual, '1')
    return true
  })
})
