import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'vite-plus/test'
import { createCheckpointHub } from 'cloudflake'
import {
  campaignWorkers,
  controlServiceBinding,
  createBindingProxies,
  createVirtualRuntimeModule,
  doNemesis,
  outboundFaultService,
  probeWorkers,
  simulateWorkers,
  snapshotPersistDir,
  workerQueue,
} from 'cloudflake/miniflare'

const RUNTIME_MODULE = createVirtualRuntimeModule()

test('workerd virtual runtime syncs Date.now, Math.random, crypto.randomUUID, assertions, and waitUntil drops', async () => {
  const workerScript = `
    import { wrapWorker, cloudflakeAssert } from './__cloudflake_runtime.mjs'
    export default wrapWorker({
      async fetch(req, env, ctx) {
        const cf = cloudflakeAssert(env)
        const now = Date.now()
        const rnd = Math.random()
        const uuid = crypto.randomUUID()
        await cf.always('virtual time matches expected', now >= 1000)
        await cf.reachable('worker fetch executed')
        ctx.waitUntil(async () => {
          await env.KV.put('bg', 'ran')
        })
        return Response.json({ now, rnd, uuid })
      }
    })
  `

  async function runOnce(waitUntilDrop) {
    let payload
    let bgValue
    const res = await simulateWorkers(
      { seed: 77, startTime: 1000 },
      (sim) => ({
        modules: [
          { type: 'ESModule', path: 'index.mjs', contents: workerScript },
          { type: 'ESModule', path: '__cloudflake_runtime.mjs', contents: RUNTIME_MODULE },
        ],
        compatibilityDate: '2026-07-01',
        kvNamespaces: ['KV'],
        serviceBindings: {
          __CLOUDFLAKE__: controlServiceBinding(sim, { waitUntilDrop }),
        },
      }),
      async (sim, runtime) => {
        sim.after(500, 'hit worker', async () => {
          const r = await runtime.dispatchFetch('https://app.test/')
          assert.equal(r.status, 200)
          payload = await r.json()
        })
        sim.onIdle('read kv', async () => {
          const kv = await runtime.getKVNamespace('KV')
          bgValue = await kv.get('bg')
        })
      },
    )
    return { payload, bgValue, res }
  }

  const first = await runOnce(1)
  const second = await runOnce(1)
  assert.equal(first.payload.now, 1500)
  assert.deepEqual(
    first.payload,
    second.payload,
    'Date.now, Math.random, and randomUUID replay identically',
  )
  assert.equal(first.bgValue, null, 'waitUntil was dropped when waitUntilDrop=1')
  assert.equal(first.res.faults.waituntil_drop, 1)
  assert.ok(first.res.assertions.some((a) => a.name === 'worker fetch executed' && a.hits === 1))
})

test('workerd Durable Object alarms run on virtual calendar with retries across eviction', async () => {
  const doScript = `
    import { DurableObject } from 'cloudflare:workers'
    import { wrapWorker, wrapDurableObject } from './__cloudflake_runtime.mjs'

    class ReminderBase extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env)
        this.sql = ctx.storage.sql
        this.sql.exec('CREATE TABLE IF NOT EXISTS events (k TEXT PRIMARY KEY, v TEXT)')
      }
      async fetch(req) {
        const url = new URL(req.url)
        if (url.pathname === '/schedule') {
          await this.ctx.storage.setAlarm(Date.now() + 60_000)
          return new Response('scheduled')
        }
        const rows = this.sql.exec('SELECT k, v FROM events ORDER BY k').toArray()
        return Response.json(rows)
      }
      async alarm(info) {
        if (info.retryCount === 0) throw new Error('transient alarm failure')
        this.sql.exec('INSERT OR REPLACE INTO events VALUES (?, ?)', 'fired', String(Date.now()) + ':' + info.retryCount)
      }
    }

    export const Reminder = wrapDurableObject(ReminderBase, 'Reminder')

    export default wrapWorker({
      async fetch(req, env) {
        const stub = env.REMINDERS.getByName('acct-1')
        return stub.fetch(req)
      }
    })
  `

  let runtimeRef
  const result = await simulateWorkers(
    { seed: 12, startTime: 1000 },
    (sim) => ({
      name: 'reminder-worker',
      modules: [
        { type: 'ESModule', path: 'index.mjs', contents: doScript },
        { type: 'ESModule', path: '__cloudflake_runtime.mjs', contents: RUNTIME_MODULE },
      ],
      compatibilityDate: '2026-07-01',
      unsafeInspectDurableObjects: true,
      durableObjects: { REMINDERS: { className: 'Reminder', useSQLite: true } },
      serviceBindings: {
        __CLOUDFLAKE__: controlServiceBinding(sim, {
          alarm: { maxRetries: 3, retryBaseDelayMs: 5000 },
          onAlarmFire: async (_objectKey, info) => {
            const res = await runtimeRef.dispatchFetch('https://app.test/', {
              headers: { 'x-cloudflake-alarm': JSON.stringify(info) },
            })
            if (!res.ok) throw new Error(await res.text())
          },
        }),
      },
    }),
    async (sim, runtime) => {
      runtimeRef = runtime
      sim.after(0, 'schedule 60s alarm', async () => {
        const r = await runtime.dispatchFetch('https://app.test/schedule')
        assert.equal(await r.text(), 'scheduled')
      })
      sim.after(30_000, 'evict DO before alarm fires', () =>
        runtime.unsafeEvictDurableObject('reminder-worker', 'Reminder', { name: 'acct-1' }),
      )
      sim.onIdle('alarm retried after eviction and persisted', async () => {
        const r = await runtime.dispatchFetch('https://app.test/state')
        const rows = await r.json()
        // Scheduled at 1000 + 60_000 = 61_000; attempt 0 fails -> retry 1 at +5000 = 66_000
        assert.deepEqual(rows, [{ k: 'fired', v: '66000:1' }])
      })
    },
  )

  assert.equal(result.time, 66_000)
  assert.equal(result.faults.alarm_retry, 1)
})

test('workerQueue drives multi-message batches with partial batch retry into D1', async () => {
  const consumer = `
    export default {
      async queue(batch, env) {
        for (const m of batch.messages) {
          if (m.body.flaky && m.attempts === 1) {
            m.retry({ delaySeconds: 30 })
            continue
          }
          await env.DB.prepare('INSERT OR IGNORE INTO inbox VALUES (?, ?)').bind(m.id, m.body.n).run()
          m.ack()
        }
      }
    }
  `

  await simulateWorkers(
    { seed: 5 },
    () => ({
      name: 'batch-consumer',
      modules: true,
      script: consumer,
      compatibilityDate: '2026-07-01',
      d1Databases: ['DB'],
    }),
    async (sim, runtime) => {
      const db = await runtime.getD1Database('DB')
      await db.exec('CREATE TABLE inbox (id TEXT PRIMARY KEY, n INTEGER)')
      const q = await workerQueue(sim, runtime, 'batch-consumer', 'jobs', {
        maxBatchSize: 3,
        maxBatchTimeoutMs: 5,
      })
      await q.sendBatch([
        { body: { n: 1, flaky: false } },
        { body: { n: 2, flaky: true } },
        { body: { n: 3, flaky: false } },
      ])
      sim.onIdle('all 3 rows committed', async () => {
        const rows = await db.prepare('SELECT n FROM inbox ORDER BY n').raw()
        assert.deepEqual(rows, [[1], [2], [3]])
      })
    },
  )
})

test('outboundFaultService + CheckpointHub interleaves same-URL requests and injects ambiguous writes', async () => {
  const script = `
    export default {
      async fetch(req, env) {
        const count = Number(await env.KV.get('count'))
        // Forward client correlation header to outbound fetch
        const headers = {}
        const cid = req.headers.get('x-cloudflake-client')
        if (cid) headers['x-cloudflake-client'] = cid
        const r = await fetch('https://upstream.test/charge', { method: 'POST', headers })
        if (!r.ok) return new Response('upstream error', { status: 502 })
        await env.KV.put('count', String(count + 1))
        return new Response('ok')
      }
    }
  `

  // 1. Two concurrent requests hitting the EXACT same URL lose an update across outbound fetch
  let finalCount = ''
  await simulateWorkers(
    { seed: 4 },
    (sim) => {
      const hub = createCheckpointHub(sim)
      sim._hub = hub
      return {
        script,
        modules: true,
        compatibilityDate: '2026-07-01',
        kvNamespaces: ['KV'],
        outboundService: outboundFaultService(sim, () => new Response('charged', { status: 200 }), {
          hub,
          checkpointDelayMs: 5,
        }),
      }
    },
    async (sim, runtime) => {
      const hub = sim._hub
      for (const name of ['req1', 'req2']) {
        hub.spawn(name, async ({ headers }) => {
          const r = await runtime.dispatchFetch('https://app.test/increment', { headers })
          assert.equal(await r.text(), 'ok')
        })
      }
      sim.onIdle('lost update observed', async () => {
        const kv = await runtime.getKVNamespace('KV')
        finalCount = await kv.get('count')
      })
    },
  )
  assert.equal(
    finalCount,
    '1',
    'two concurrent requests to identical URL interleaved at outbound fetch',
  )

  // 2. Ambiguous outbound write: upstream executed, wrapped Worker receives thrown network error
  let upstreamCharges = 0
  let workerStatus = 0
  await simulateWorkers(
    { seed: 9 },
    (sim) => ({
      modules: [
        {
          type: 'ESModule',
          path: 'index.mjs',
          contents: `
            import { wrapWorker } from './__cloudflake_runtime.mjs'
            export default wrapWorker({
              async fetch() {
                try {
                  await fetch('https://upstream.test/charge', { method: 'POST' })
                  return new Response('ok', { status: 200 })
                } catch {
                  return new Response('network fail', { status: 504 })
                }
              }
            })
          `,
        },
        { type: 'ESModule', path: '__cloudflake_runtime.mjs', contents: RUNTIME_MODULE },
      ],
      compatibilityDate: '2026-07-01',
      serviceBindings: { __CLOUDFLAKE__: controlServiceBinding(sim) },
      outboundService: outboundFaultService(
        sim,
        () => {
          upstreamCharges++
          return new Response('charged')
        },
        { ambiguousRate: 1 },
      ),
    }),
    async (sim, runtime) => {
      sim.after(0, 'charge', async () => {
        const r = await runtime.dispatchFetch('https://app.test/')
        workerStatus = r.status
      })
    },
  )
  assert.equal(upstreamCharges, 1)
  assert.equal(workerStatus, 504)
})

test('campaignWorkers runs parallel workerd seeds with doNemesis and property coverage', async () => {
  const script = `
    import { DurableObject } from 'cloudflare:workers'
    export class Counter extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env)
        this.sql = ctx.storage.sql
        this.sql.exec('CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)')
      }
      async fetch(req) {
        if (req.method === 'POST') {
          this.sql.exec('INSERT OR IGNORE INTO seen VALUES (?)', await req.text())
        }
        return Response.json(this.sql.exec('SELECT count(*) AS n FROM seen').one().n)
      }
    }
    export default {
      fetch(req, env) { return env.COUNTERS.getByName('shared').fetch(req) }
    }
  `

  const res = await campaignWorkers(
    { seed: 0, runs: 4, concurrency: 2 },
    () => ({
      name: 'counter',
      modules: true,
      script,
      compatibilityDate: '2026-07-01',
      unsafeInspectDurableObjects: true,
      durableObjects: { COUNTERS: { className: 'Counter', useSQLite: true } },
    }),
    async (sim, runtime) => {
      doNemesis(
        sim,
        runtime,
        [{ scriptName: 'counter', className: 'Counter', id: { name: 'shared' } }],
        {
          ticks: 2,
          windowMs: 10,
          probability: 0.75,
        },
      )
      for (const id of ['a', 'b']) {
        sim.deliver(
          'inc ' + id,
          async () => {
            const r = await runtime.dispatchFetch('https://app.test/', { method: 'POST', body: id })
            assert.equal(r.status, 200)
          },
          { duplicate: 1, maxDelay: 10 },
        )
      }
      sim.onIdle('exact count', async () => {
        const n = await (await runtime.dispatchFetch('https://app.test/')).json()
        sim.always('counter equals 2', n === 2, { n })
      })
    },
  )

  assert.equal(res.runs, 4)
  assert.ok(res.report.assertions.some((a) => a.name === 'do evicted' && a.passed >= 1))
})

test('snapshotPersistDir branches two independent workerd SQLite DO timelines from a midpoint', async () => {
  const script = `
    import { DurableObject } from 'cloudflare:workers'
    export class Store extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env)
        this.sql = ctx.storage.sql
        this.sql.exec('CREATE TABLE IF NOT EXISTS items (k TEXT PRIMARY KEY)')
      }
      async fetch(req) {
        if (req.method === 'POST') this.sql.exec('INSERT OR IGNORE INTO items VALUES (?)', await req.text())
        return Response.json(this.sql.exec('SELECT k FROM items ORDER BY k').toArray().map(r => r.k))
      }
    }
    export default { fetch(req, env) { return env.STORE.getByName('main').fetch(req) } }
  `

  const baseDir = await mkdtemp(join(tmpdir(), 'cloudflake-base-'))
  let snapshot
  let branchA
  let branchB
  try {
    await simulateWorkers(
      { seed: 1 },
      () => ({
        modules: true,
        script,
        compatibilityDate: '2026-07-01',
        durableObjectsPersist: baseDir,
        durableObjects: { STORE: { className: 'Store', useSQLite: true } },
      }),
      async (sim, runtime) => {
        sim.after(0, 'seed base item', async () => {
          await runtime.dispatchFetch('https://app.test/', { method: 'POST', body: 'base' })
        })
      },
    )

    snapshot = await snapshotPersistDir(baseDir)
    branchA = await snapshot.branch()
    branchB = await snapshot.branch()

    const runBranch = async (dir, item) => {
      let items
      await simulateWorkers(
        { seed: 2 },
        () => ({
          modules: true,
          script,
          compatibilityDate: '2026-07-01',
          durableObjectsPersist: dir,
          durableObjects: { STORE: { className: 'Store', useSQLite: true } },
        }),
        async (sim, runtime) => {
          sim.after(0, 'write branch item', async () => {
            await runtime.dispatchFetch('https://app.test/', { method: 'POST', body: item })
            items = await (await runtime.dispatchFetch('https://app.test/')).json()
          })
        },
      )
      return items
    }

    const [itemsA, itemsB] = await Promise.all([
      runBranch(branchA.dir, 'alpha'),
      runBranch(branchB.dir, 'beta'),
    ])
    assert.deepEqual(itemsA, ['alpha', 'base'])
    assert.deepEqual(itemsB, ['base', 'beta'])
  } finally {
    await branchA?.dispose()
    await branchB?.dispose()
    await snapshot?.dispose()
    await rm(baseDir, { recursive: true, force: true })
  }
})

test('createBindingProxies intercepts Worker-side env.QUEUE, env.KV, and env.DO with zero app code changes', async () => {
  const appScript = `
    import { DurableObject } from 'cloudflare:workers'
    export class Ledger extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env)
        this.sql = ctx.storage.sql
        this.sql.exec('CREATE TABLE IF NOT EXISTS entries (id TEXT PRIMARY KEY)')
      }
      async fetch(req) {
        if (req.method === 'POST') {
          this.sql.exec('INSERT OR IGNORE INTO entries VALUES (?)', await req.text())
          return new Response('committed')
        }
        return Response.json(this.sql.exec('SELECT count(*) AS n FROM entries').one().n)
      }
    }
    export default {
      async fetch(req, env) {
        const url = new URL(req.url)
        if (url.pathname === '/enqueue') {
          await env.JOBS.send({ id: 'job-1' }, { delaySeconds: 30 })
          return new Response('enqueued')
        }
        if (url.pathname === '/kv') {
          await env.CACHE.put('k', 'v2')
          return Response.json({ seen: await env.CACHE.get('k') })
        }
        if (url.pathname === '/do') {
          try {
            await env.LEDGER.getByName('acct').fetch('https://do/', { method: 'POST', body: 'tx-1' })
            return new Response('ok')
          } catch (err) {
            return new Response('reset:' + err.message, { status: 503 })
          }
        }
        return env.LEDGER.getByName('acct').fetch('https://do/')
      },
      async queue(batch, env) {
        for (const m of batch.messages) {
          await env.CACHE.put('processed:' + m.body.id, 'yes')
          m.ack()
        }
      }
    }
  `

  let proxiesRef
  let kvSeen
  let doResponseStatus
  let doFinalCount

  const res = await simulateWorkers(
    { seed: 7 },
    (sim) => {
      const proxies = createBindingProxies(sim, {
        queues: {
          JOBS: { workerName: 'app', queueName: 'jobs' },
        },
        kv: {
          CACHE: { propagationDelayMs: 60_000, staleRead: 1 },
        },
        durableObjects: {
          LEDGER: { scriptName: 'app', className: 'Ledger', rpcAmbiguous: 1 },
        },
      })
      proxiesRef = proxies
      return {
        workers: [
          {
            name: 'app',
            modules: true,
            script: appScript,
            compatibilityDate: '2026-07-01',
            unsafeInspectDurableObjects: true,
            durableObjects: { REAL_LEDGER: { className: 'Ledger', useSQLite: true } },
            serviceBindings: { ...proxies.serviceBindings },
          },
          proxies.proxyWorker,
        ],
      }
    },
    async (sim, runtime) => {
      await proxiesRef.attach(runtime)
      // Pre-populate KV with v1 and let it propagate before testing staleRead=1
      await proxiesRef.kv.CACHE.put('k', 'v1')

      sim.after(60_000, 'test kv stale read', async () => {
        const r = await runtime.dispatchFetch('https://app.test/kv')
        kvSeen = (await r.json()).seen
      })
      sim.after(60_001, 'test worker-side queue.send()', async () => {
        const r = await runtime.dispatchFetch('https://app.test/enqueue')
        assert.equal(await r.text(), 'enqueued')
      })
      sim.after(60_002, 'test ambiguous DO RPC', async () => {
        const r = await runtime.dispatchFetch('https://app.test/do')
        doResponseStatus = r.status
      })
      sim.after(95_000, 'heal and inspect DO', async () => {
        sim.heal()
        const r = await runtime.dispatchFetch('https://app.test/read')
        doFinalCount = await r.json()
      })
      sim.onIdle('verify queue processed on virtual time', () => {
        assert.equal(proxiesRef.kv.CACHE.inspect('processed:job-1'), 'yes')
      })
    },
  )

  assert.equal(
    kvSeen,
    'v1',
    'Worker-side env.CACHE.get() observed stale v1 during propagation window',
  )
  assert.equal(doResponseStatus, 503, 'Worker received reset error on ambiguous DO RPC')
  assert.equal(doFinalCount, 1, 'DO actually committed tx-1 before ambiguous reset!')
  assert.equal(res.faults.kv_stale_read, 1)
  assert.equal(res.faults.do_rpc_ambiguous, 1)
})

test('probeWorkers collects failures, minimizes faults, and emits structured agent guidance', async () => {
  const brokenScript = `
    export default {
      async fetch(req, env) {
        const n = Number(await env.KV.get('n') ?? '0')
        await env.KV.put('n', String(n + 1))
        return new Response('ok')
      }
    }
  `

  const report = await probeWorkers(
    {
      name: 'broken-counter-worker',
      seed: 0,
      runs: 2,
      concurrency: 2,
      allowInCI: true,
      throwOnFailure: false,
      printGuidance: 'never',
      replayCommand: 'CLOUDFLAKE_SEED={seed} node probe-worker.mjs',
    },
    () => ({
      modules: true,
      script: brokenScript,
      compatibilityDate: '2026-07-01',
      kvNamespaces: ['KV'],
    }),
    async (sim, runtime) => {
      for (let i = 0; i < 3; i++) {
        sim.deliver(
          `inc-${i}`,
          async () => {
            const r = await runtime.dispatchFetch('https://app.test/')
            assert.equal(r.status, 200)
          },
          { duplicate: 1 },
        )
      }
      sim.onIdle('exact count', async () => {
        const kv = await runtime.getKVNamespace('KV')
        assert.equal(await kv.get('n'), '3')
      })
    },
  )

  assert.equal(report.status, 'fail_safety')
  assert.equal(report.findings.length, 1)
  assert.equal(report.findings[0].minimalFaults.length, 1)
  assert.match(report.guidance, /CLOUDFLAKE_SEED=0 node probe-worker\.mjs/)
  assert.match(report.guidance, /Duplicate delivery triggered this failure/)
})

test('createBindingProxies forwards DO JS RPC methods through reset/ambiguous faults', async () => {
  const appScript = `
    import { DurableObject } from 'cloudflare:workers'
    export class Counter extends DurableObject {
      async increment(by) {
        const n = ((await this.ctx.storage.get('n')) ?? 0) + by
        await this.ctx.storage.put('n', n)
        return n
      }
    }
    export default {
      async fetch(req, env) {
        try {
          return Response.json(await env.COUNTER.getByName('a').increment(1))
        } catch (err) {
          return new Response(err.message, { status: 503 })
        }
      }
    }
  `
  let proxiesRef
  const seen = []
  const res = await simulateWorkers(
    { seed: 3 },
    (sim) => {
      proxiesRef = createBindingProxies(sim, {
        durableObjects: {
          COUNTER: {
            scriptName: 'app',
            className: 'Counter',
            rpcAmbiguous: 1,
            rpcMethods: ['increment'],
          },
        },
      })
      return {
        workers: [
          {
            name: 'app',
            modules: true,
            script: appScript,
            compatibilityDate: '2026-07-01',
            durableObjects: { REAL_COUNTER: 'Counter' },
            serviceBindings: { ...proxiesRef.serviceBindings },
          },
          proxiesRef.proxyWorker,
        ],
      }
    },
    async (sim, runtime) => {
      await proxiesRef.attach(runtime)
      sim.after(1, 'ambiguous rpc', async () => {
        const r = await runtime.dispatchFetch('https://app.test/')
        seen.push([r.status, await r.text()])
      })
      sim.after(2, 'healed rpc', async () => {
        sim.heal()
        const r = await runtime.dispatchFetch('https://app.test/')
        seen.push([r.status, await r.text()])
      })
    },
  )
  assert.equal(seen[0][0], 503)
  assert.match(seen[0][1], /connection lost after execution/)
  assert.deepEqual(seen[1], [200, '2'], 'the ambiguous call committed before the reset')
  assert.equal(res.faults.do_rpc_ambiguous, 1)
  assert.throws(
    () =>
      createBindingProxies(
        { rngStream: () => ({}) },
        {
          durableObjects: { X: { scriptName: 'a', className: 'B', rpcMethods: ['bad name'] } },
        },
      ),
    RangeError,
  )
})

test('a Worker that swallows a failed assertion still fails the run', async () => {
  const workerScript = `
    import { wrapWorker, cloudflakeAssert } from './__cloudflake_runtime.mjs'
    export default wrapWorker({
      async fetch(req, env) {
        try { await cloudflakeAssert(env).always('balance non-negative', false) } catch {}
        return new Response('ok')
      }
    })
  `
  await assert.rejects(
    simulateWorkers(
      { seed: 1 },
      (sim) => ({
        modules: [
          { type: 'ESModule', path: 'index.mjs', contents: workerScript },
          { type: 'ESModule', path: '__cloudflake_runtime.mjs', contents: RUNTIME_MODULE },
        ],
        compatibilityDate: '2026-07-01',
        serviceBindings: { __CLOUDFLAKE__: controlServiceBinding(sim) },
      }),
      async (sim, runtime) => {
        sim.after(1, 'hit worker', async () => {
          const r = await runtime.dispatchFetch('https://app.test/')
          assert.equal(await r.text(), 'ok')
        })
      },
    ),
    (e) =>
      /always\("balance non-negative"\) violated/.test(e.cause?.message) &&
      e.phase === 'always balance non-negative',
  )
})

test('campaignWorkers validates runs and durationMs', async () => {
  const noop = () => ({})
  await assert.rejects(campaignWorkers({ seed: 0, runs: Number('abc') }, noop, noop), RangeError)
  await assert.rejects(campaignWorkers({ seed: 0, runs: 0 }, noop, noop), RangeError)
  await assert.rejects(
    campaignWorkers({ seed: 0, durationMs: Number('abc') }, noop, noop),
    RangeError,
  )
})
