# ![Cloudflake — deterministic simulation testing for Workers and Durable Objects](assets/banner.png)

Cloudflake explores failures in Cloudflare applications by varying request order, delivery, retries and eviction. Give it a scenario, a property that must hold, and a seed. A failure reports the seed, virtual time, failing operation, recent event history and original error.

The TypeScript scheduler runs in process with zero runtime dependencies. The optional Miniflare adapter runs your bundled Workers and their bindings locally in workerd. Use the same scheduling API for fast model tests and application tests.

We use Cloudflake at Economico to test our Workers and Durable Objects.

## Try it

Use Node.js 24 or newer. From this checkout run `npm ci && npm test`. To use the library in another project, run `npm pack` here and install the resulting tarball as a dev dependency there. Install a compatible Miniflare 4 alongside it for the Workers adapter.

The following scenario tests a consumer that could receive a payment more than once. It explores a thousand schedules, checking the balance after every event.

```js
import assert from 'node:assert/strict'
import { campaign, simulate, simulatedQueue } from 'cloudflake'

async function payments(sim) {
  const posted = new Set()
  let balance = 0
  const queue = simulatedQueue(sim, 'payments', ({ messages }) => {
    for (const { id, body } of messages) {
      if (posted.has(id)) continue
      posted.add(id)
      balance += body.amount
    }
  }, { duplicate: 0.5 })
  sim.invariant('one posting per payment', () => assert.equal(balance, 7 * posted.size))
  for (let i = 0; i < 8; i++) await queue.send({ amount: 7 })
  sim.after(1, 'all payments settled', () => assert.equal(balance, 56))
}

await campaign({ seed: 0, runs: 1000 }, payments)
// Replay one schedule with a full event trace.
await simulate({ seed: 42, onStep: console.log }, payments)
```

Remove the `posted.has(id)` guard to see a duplicate delivery violate the invariant. For replay, use the failure's seed with the same code, inputs, dependency versions and options.

## Property-based testing with Hegel

Run `npm run test:hegel` from this checkout. The first example generates payment amounts and a Cloudflake scheduling seed, redelivers every payment, and checks the settled sum. The second uses a scripted Workers AI binding in workerd and verifies the exact D1 rows after valid, malformed and failed responses. Model quality and live provider availability require separate evaluation.

Draw workload values directly with Hegel so it can shrink individual inputs as well as the seed. Each generated case and each shrinking attempt starts with fresh state. For a permanent regression, keep the minimized inputs and the Cloudflake seed together. The seed determines delivery order for those inputs. [Runnable examples](examples/hegel.test.mjs) · [Hegel TypeScript guide](https://hegel.dev/typescript)

## Run an application

`simulateWorkers(options, deployment, scenario)` takes a factory returning normal Miniflare options, including the `workers` array for multi-Worker deployments. Each invocation creates and disposes one runtime. Omit persistence directories and remote bindings for isolated local runs. Bundle once outside a campaign, then reuse the bundle across seeds.

```js
import assert from 'node:assert/strict'
import { simulateWorkers } from 'cloudflake/miniflare'

await simulateWorkers({ seed: 42 }, () => ({
  modules: true,
  scriptPath: './build/worker.mjs',
  compatibilityDate: '2026-07-01',
  durableObjects: { BOOK: { className: 'Book', useSQLite: true } },
  r2Buckets: ['FILES']
}), (sim, runtime) => {
  sim.deliver('upload invoice 17', async () => {
    const response = await runtime.dispatchFetch('https://app.test/invoices/17', {
      method: 'PUT', body: 'invoice data'
    })
    assert.ok(response.ok)
  }, { maxDelay: 50, duplicate: 1 })
  sim.after(51, 'one stored invoice', async () => {
    const bucket = await runtime.getR2Bucket('FILES')
    assert.equal((await bucket.list()).objects.length, 1)
  })
})
```

For a complete runnable example, run `node examples/counter.mjs` from this checkout. It redelivers increment requests to a SQLite-backed Durable Object, evicts it, and checks that each request counted once.

### Control points

| API | Behavior |
| --- | --- |
| `sim.after(ms, label, action)` | Schedules a turn in virtual milliseconds and returns a cancel function. Time advances directly to the next live event. Equal-time events have seeded priorities. |
| `sim.int(limit)` | Generates a uniform integer from zero through `limit - 1`. Workload generation, scheduling priorities and delivery faults have separate PRNG streams. |
| `sim.deliver(label, action, policy)` | Adds seeded latency, omission or one extra delivery. `maxDelay` is in milliseconds. `drop` and `duplicate` range from zero to one. Recreate consumable request bodies inside the callback. |
| `sim.invariant(name, check)` | Checks a property after setup and each completed event. Use a final scheduled assertion for eventual convergence. |
| `client(sim, name, async checkpoint => …)` | Runs a sequential client until completion or its next awaited `checkpoint(delayMs, label)`. Other clients can run while it is parked. A test service binding can await this same checkpoint inside a live Worker request. |
| `simulatedQueue(sim, name, consumer, options)` | Provides `send` and `sendBatch`, stable delivery IDs, payload copies, duplicates, retry delays and bounded retries. Each delivery is a one-message batch. Exhausted retries fail the run unless an `onExhausted` handler is supplied. |
| `workerQueue(sim, runtime, workerName, queueName, options)` | Calls the actual Worker's queue handler with virtual timestamps and honors its retry response. Use the returned producer to inject deliveries. A native producer binding uses Miniflare's own broker. |
| `simulatedAlarm(sim, objectName, fire)` | Provides one replaceable alarm slot to dependency-injected code. Setting an earlier time wakes on the next scheduler turn. Exceptions fail the scenario. |
| `maxSteps`, `traceLimit`, `onStep` | Bound event count and retained failure history, or stream every step. Defaults are 100,000 events and 32 retained steps. |

Normal actions run to completion. To explore an in-flight race, put a checkpoint at an I/O boundary and await it on one sequential chain per client. The Workers integration test demonstrates two live requests pausing after a KV read, losing an increment, and reproducing the failure. Native calls can use Miniflare's service bindings, namespaces, SQL, R2, KV, D1, queue handlers, cron entry points and object eviction. Configure service-specific behavior in the deployment factory.

### Replay boundary

Cloudflake controls its own time, random draws, scheduled events, explicit faults and checkpoints. Workerd owns native clocks, entropy, alarm delivery, background work and internal asynchronous scheduling. An application that depends on those values needs injected clocks, entropy or service doubles for exact replay. A seed by itself reproduces the Cloudflake schedule. Properties that use only controlled inputs can reproduce the entire failure. `maxSteps` detects runaway scheduled work. Use a test-runner timeout for a hung external call.

## Performance and design

Run `npm run build && npm run bench` for the scheduler benchmark. It warms up, then reports five trials of one million events each with 256 pending events and bounded tracing. On the development Linux arm64 machine with Node 26.12, the median was about 4.9 million events per second. This benchmark measures the scheduler in isolation.

The event calendar is a binary heap. The scheduler invokes synchronous operations directly, keeps a bounded failure trace, and runs alongside JavaScript scenario code. TypeScript avoids a native boundary per scheduling decision. Runtime startup, storage and RPC dominate the measured end-to-end workload. There is no Rust comparison benchmark. Hegel can generate scenario inputs or seeds independently. Cloudflake owns schedule selection and replay and requires no separate property-testing engine.

## License

Apache License 2.0.
