# Cloudflake patterns

Recipes for common Cloudflare failure modes. Each one is drawn from the library's
own tests and examples. Imports assume `node:test` and `node:assert/strict`.

## Duplicate delivery to a real Durable Object, with eviction

Each request id must count once, even when delivered twice and the object is
evicted mid-run.

```js
import { simulateWorkers } from 'cloudflake/miniflare'

await simulateWorkers({ seed }, () => ({
  name: 'counter', modules: true, script, compatibilityDate: '2026-07-01',
  unsafeInspectDurableObjects: true,
  durableObjects: { COUNTERS: { className: 'Counter', useSQLite: true } }
}), async (sim, runtime) => {
  const url = 'https://counter.test/'
  for (const id of ['a', 'b', 'c']) {
    sim.deliver('increment ' + id, async () => {
      const response = await runtime.dispatchFetch(url, { method: 'POST', body: id })
      assert.equal(response.status, 200)
      await response.json()
    }, { maxDelay: 5, duplicate: 1 })
  }
  sim.after(sim.int(6), 'evict counter', () =>
    runtime.unsafeEvictDurableObject('counter', 'Counter', { name: 'shared' }))
  sim.after(6, 'each increment counted once', async () => {
    assert.equal(await (await runtime.dispatchFetch(url)).json(), 3)
  })
})
```

The Worker deduplicates with `INSERT OR IGNORE` on the request id. Replace it
with a plain counter to watch the campaign fail.

## Lost update between two live requests

Plain events run to completion, so two `dispatchFetch` calls never interleave
inside the Worker. Expose a checkpoint through a service binding and call it at
the I/O boundary where the race lives.

```js
import { client } from 'cloudflake'

const script = `export default { async fetch(req, env) {
  const count = Number(await env.KV.get("count"))
  await env.CHECKPOINT.fetch(req.url)              // pause between read and write
  await env.KV.put("count", String(count + 1))
  return new Response("ok")
} }`

const gates = new Map()
await simulateWorkers({ seed }, () => ({
  script, modules: true, compatibilityDate: '2026-07-01', kvNamespaces: ['KV'],
  serviceBindings: { CHECKPOINT: async req => {
    await gates.get(new URL(req.url).pathname)(1, 'after reading count')
    return new Response()
  } }
}), async (sim, runtime) => {
  for (const path of ['/a', '/b']) client(sim, path, async checkpoint => {
    gates.set(path, checkpoint)
    assert.equal(await (await runtime.dispatchFetch('https://test' + path)).text(), 'ok')
  })
  const kv = await runtime.getKVNamespace('KV')
  sim.after(2, 'both increments persisted', async () => assert.equal(await kv.get('count'), '2'))
})
```

In production code, keep the checkpoint binding out of the deployed
configuration, or make it a no-op service there.

## A real queue consumer with retries and D1

```js
import { simulateWorkers, workerQueue } from 'cloudflake/miniflare'

await simulateWorkers({ seed }, () => ({
  name: 'consumer', modules: true, compatibilityDate: '2026-07-01',
  d1Databases: ['DB'], scriptPath: './build/consumer.mjs'
}), async (sim, runtime) => {
  const db = await runtime.getD1Database('DB')
  await db.exec('CREATE TABLE inbox (id TEXT PRIMARY KEY, value INTEGER)')
  const queue = await workerQueue(sim, runtime, 'consumer', 'effects', { duplicate: 0.5 })
  await queue.send({ value: 12 })
  sim.after(60_001, 'committed once', async () => {
    assert.equal(await db.prepare('SELECT count(*) AS n FROM inbox').first('n'), 1)
  })
})
```

`m.retry({ delaySeconds: 60 })` inside the handler moves the next attempt 60,000
virtual ms later. The test does not wait in wall-clock time.

## Model test of a Durable Object with an injected alarm

Write the object's logic against a small storage interface so the test can swap
in `simulatedAlarm` and an in-memory map. `ReminderLogic` stands for your own class.

```js
import { simulate, simulatedAlarm } from 'cloudflake'

await simulate({ seed }, async sim => {
  const storage = new Map()                         // durable state survives "restarts"
  let instance
  const alarm = simulatedAlarm(sim, 'reminder', () => instance.alarm())
  const boot = () => { instance = new ReminderLogic({ storage, alarm, now: () => sim.now }) }
  boot()
  await instance.schedule('invoice-17', 5_000)
  sim.after(sim.int(5_000), 'restart', boot)        // new instance, same storage and alarm slot
  sim.after(5_001, 'reminder sent once', () => assert.equal(storage.get('sent:invoice-17'), 1))
})
```

Resolve the *current* instance inside `fire`, as above, so an alarm after a
restart reaches the new object.

## Injecting time and ids

Code that calls `Date.now()` or `crypto.randomUUID()` directly escapes the seed.
Pass them in:

```js
const deps = {
  now: () => sim.now,
  id: () => 'id-' + sim.int(2 ** 32).toString(16)
}
```

For a Worker under `simulateWorkers`, expose the same values through a service
binding or bindings created in the deployment factory, which receives `sim`.

## Turning a failure into a regression test

```js
test('regression: duplicate after eviction (seed 7311)', async () => {
  await simulateWorkers({ seed: 7311 }, deployment, scenario)
})

test('campaign over 500 schedules', async () => {
  await campaign({ seed: 0, runs: 500 }, scenario)
})
```

Replay requires the same code, inputs, dependency versions and options
(`maxDelay`, `duplicate`, `maxRetries` and so on). Record them with the seed.

## Asserting that a bug is found

When the test itself is the deliverable, prove it detects the broken variant:

```js
const failure = await campaign({ seed: 0, runs: 64 }, brokenScenario)
  .then(() => assert.fail('race undetected'), e => e)
assert.equal(failure.phase, 'both increments committed')
await assert.rejects(simulate({ seed: failure.seed }, brokenScenario), e => {
  assert.deepEqual(e.trace, failure.trace)          // same schedule on replay
  return true
})
```

## Sizing a campaign

- Model tests: thousands of runs are cheap. The scheduler alone handles
  millions of events per second.
- `simulateWorkers`: each run starts workerd, so tens to low hundreds of runs
  per test is typical. Bundle once outside the loop.
- Raise `traceLimit` when the failing event is far from its cause, or use
  `onStep` on replay to see everything.
