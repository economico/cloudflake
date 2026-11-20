---
name: cloudflake
description: Write deterministic simulation tests (DST) for Cloudflare Workers, Durable Objects, Queues, alarms, KV, R2 and D1 with the cloudflake library. Use when testing for duplicate delivery, retries, reordering, lost updates, races, eviction or idempotency in Cloudflare code; when a test should explore many seeded schedules and replay a failure exactly; when combining Hegel property-based testing with schedule exploration; or when the user mentions cloudflake, simulation testing, DST, seeds or replay for Workers.
---

# Cloudflake

Cloudflake is a seeded scheduler for testing Cloudflare applications. A scenario
registers events on a virtual calendar. Cloudflake orders them from a seed, injects
latency, drops and duplicates, checks invariants after every event, and reports a
failing seed that replays the same schedule. It has no runtime dependencies. The
optional `cloudflake/miniflare` adapter runs real bundled Workers in workerd.

Read [references/patterns.md](references/patterns.md) for complete recipes and
[references/further-reading.md](references/further-reading.md) for background on
DST and property-based testing.

## Install

```sh
npm install -D cloudflake            # Node.js 24 or newer
npm install -D miniflare@4           # only for cloudflake/miniflare
npm install -D @hegeldev/hegel       # only for property-based inputs
```

Tests run under any runner. The repository uses `node --test`.

## Workflow

1. **Name the property.** Write down what must stay true regardless of order,
   e.g. "each payment posts exactly once", "the counter equals the number of
   distinct requests", "no row exists for a failed model response". Split it into
   an _invariant_ (true after every event) and a _final check_ (true once the
   system settles).
2. **Find the nondeterminism.** List every place where order, timing or delivery
   count can vary: queue deliveries, retries, fetches between Workers, alarms,
   Durable Object eviction, concurrent requests that read then write. Each one
   becomes a scheduled event, a `sim.deliver`, or a `checkpoint`.
3. **Start with a model test.** Run the logic in-process with `simulate` and the
   simulated queue or alarm. It runs thousands of schedules per second. Move to
   `simulateWorkers` when the bug lives in real bindings or storage.
4. **Explore.** Run `campaign({ seed: 0, runs: 1000 }, scenario)`. It tries
   seeds `0..999` and stops on the first failure.
5. **Replay and diagnose.** Rerun the failing seed with
   `simulate({ seed, onStep: console.log }, scenario)` to print every event.
   The thrown `SimulationFailure` also carries `seed`, `steps`, `time` (virtual
   ms), `phase` (the event or invariant label that failed), `trace` (recent steps)
   and `cause` (the original error).
6. **Prove the test bites.** Before fixing anything, or after, remove the guard
   (dedupe check, `INSERT OR IGNORE`, transaction) and confirm the campaign
   fails. A DST that never fails proves nothing.
7. **Keep the regression.** Add a test that runs the failing seed explicitly,
   next to the broader campaign. With Hegel, keep the minimized inputs together
   with the seed.

When reporting results, state the seed range explored, the number of runs, and
for any failure the seed, phase, virtual time and cause.

## Core API (`cloudflake`)

```js
import {
  simulate,
  campaign,
  probe,
  exploreMultiverse,
  minimizeFaults,
  HistoryRecorder,
  checkLinearizability,
  formatCampaignReport,
  formatTrace,
  simulatedQueue,
  simulatedAlarm,
  simulatedKV,
  simulatedDurableObject,
  simulatedWaitUntil,
  simulatedFetch,
  client,
  createCheckpointHub,
  SimulationFailure,
  PropertyCoverageFailure,
  ProbeFailure,
  seedFromEnv,
} from 'cloudflake'
```

| Call                                                                             | Behavior                                                                                                                                                                                         |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `simulate(options, scenario)`                                                    | Runs one schedule. Resolves `{ seed, steps, time, assertions, faults, firedFaults }` or throws `SimulationFailure`.                                                                              |
| `campaign({ ...options, runs, durationMs, onFailure }, scenario)`                | Runs multiple seeds, aggregates Antithesis property coverage (`sometimes`/`reachable`/`always`), and buckets failures when `onFailure: 'collect'`.                                               |
| `probe({ name, durationMs, replayCommand }, scenario)`                           | **Local exploratory prober** (auto-skips in CI unless `CLOUDFLAKE_PROBE=1`). Runs longer exploration, buckets failures, runs `minimizeFaults`, and prints structured guidance for coding agents. |
| `exploreMultiverse(options)`                                                     | Antithesis-style coverage-guided snapshot & branching explorer (`sim.cover()`, assertions, faults, and optional V8 branch coverage).                                                             |
| `minimizeFaults(options, scenario)`                                              | Delta-debugs a failing seed to find the minimal subset of injected faults that still reproduces the failure.                                                                                     |
| `sim.after(ms, label, action)`                                                   | Schedules `action` at `now + ms` virtual ms. Returns a cancel function. Equal-time events are ordered by the seed.                                                                               |
| `sim.onIdle(label, action)`                                                      | Runs once the event calendar drains to quiescence. Prefer this over hardcoded settlement timestamps.                                                                                             |
| `sim.heal()` / `healAt`                                                          | Transitions from chaos to quiescence so retries drain before `onIdle` convergence checks.                                                                                                        |
| `sim.deliver(label, action, policy)`                                             | Schedules delivery with seeded latency (`maxDelay` / `delayDistribution`), zero-step `drop`, `1..maxDuplicates` copies, and `ambiguous` (gray) failures.                                         |
| `sim.invariant(name, check)`                                                     | Runs `check` after setup and after every event. Throw (e.g. `assert`) to fail.                                                                                                                   |
| `sim.always` / `alwaysOrUnreachable` / `sometimes` / `reachable` / `unreachable` | Antithesis-style safety and reachability assertions. `sometimes` and expected `reachable`/`always` fail the campaign with `PropertyCoverageFailure` if never satisfied.                          |
| `sim.rng` / `sim.rngStream(name)` / `sim.int(limit)`                             | 128-bit `xoshiro128++` PRNG with isolated named streams and combinators (`range`, `chance`, `pick`, `shuffle`, `weighted`, `bytes`, `uuid`, `delay`).                                            |
| `sim.profile`                                                                    | TigerBeetle VOPR-style swarm fault profile randomized per seed (constrain with `options.swarm`).                                                                                                 |
| `HistoryRecorder` / `checkLinearizability`                                       | Verifies concurrent operation histories (including `indeterminate` ambiguous outcomes) against a pure sequential reference model.                                                                |
| `sim.waitFor(registerWake, label)`                                               | Cooperatively parks an active `client()` on a lock/input-gate and detects deadlocks if the calendar drains while parked.                                                                         |
| `sim.cover(edge)`                                                                | Records a state/branch coverage edge for `exploreMultiverse`.                                                                                                                                    |

Cloudflare in-process simulators:

- `simulatedQueue(sim, name, consumer, { maxBatchSize, maxBatchTimeoutMs, maxDelay, maxRetries, retryDelaySeconds, duplicate, maxDuplicates, deadLetterQueue, onExhausted })`: multi-message batching, partial batch `ack`/`retry`, reordering, duplicates, and DLQ.
- `simulatedAlarm(sim, objectName, fire, { maxRetries, retryBaseDelayMs, duplicate, maxDelay })`: passes `{ retryCount, isRetry }`, retries with exponential backoff on throw, and supports at-least-once duplicates and jitter.
- `simulatedKV(sim, name, { propagationDelayMs, staleRead, errorRate })`: eventual consistency stale reads, tombstones, virtual-time TTLs, and strongly consistent `.inspect(key)`.
- `simulatedDurableObject(sim, name, factory, { alarm, rpcReset, rpcAmbiguous, evictBeforeCall, evictAfterCall, clockSkewMs })`: durable transactional storage vs ephemeral instance memory across `evict()` / `ctx.abort()`, cooperative `blockConcurrencyWhile` input gates, and ambiguous RPC resets.
- `simulatedWaitUntil(sim, { drop, maxDelay })` & `simulatedFetch(sim, handler, { errorRate, timeoutRate, ambiguousRate })`.
- `client(sim, name, task)` & `createCheckpointHub(sim)`: cooperative clients with request-header (`x-cloudflake-client`) checkpoint routing and deadlock detection.

## Workers adapter (`cloudflake/miniflare`)

```js
import {
  simulateWorkers,
  campaignWorkers,
  probeWorkers,
  workerQueue,
  createBindingProxies,
  controlServiceBinding,
  createVirtualRuntimeModule,
  outboundFaultService,
  doNemesis,
  snapshotPersistDir,
} from 'cloudflake/miniflare'
```

- `simulateWorkers` / `campaignWorkers`: run single or parallel (`concurrency`) `workerd` runs with aggregated Antithesis property reports.
- `probeWorkers`: local exploratory `workerd` prober (auto-skips in CI by default) with fault minimization and agent-ready failure guidance.
- `createBindingProxies(sim, { queues, kv, durableObjects })`: drop-in `WorkerEntrypoint`/`RpcTarget` proxies so real Workers calling `env.MY_QUEUE.send()`, `env.MY_KV.get/put()`, and `env.MY_DO.getByName().fetch()` (plus JS RPC methods listed in `durableObjects[binding].rpcMethods`) get virtual-time queues, KV stale reads, and DO RPC reset/ambiguous faults with zero app code changes.
- `controlServiceBinding(sim)` + `createVirtualRuntimeModule()`: syncs `Date.now()`, `new Date()` and `Date()`, `Math.random()`, `crypto.randomUUID()`, `ctx.waitUntil(() => task())` drops (factories only; a passed Promise is already running), real DO `ctx.storage` alarms, and `cloudflakeAssert(env)` inside `workerd` onto Cloudflake's virtual calendar.
- `outboundFaultService(sim, handler, { hub, errorRate, timeoutRate, ambiguousRate })`: drop-in Miniflare `outboundService` for outbound `fetch()` faults and `CheckpointHub` interleaving.
- `doNemesis(sim, runtime, targets, options)`: seeded `unsafeEvictDurableObject` chaos actor with `sometimes('do evicted')` coverage.
- `snapshotPersistDir(persistDir)`: clone SQLite/KV/R2/DO persistence directories for midpoint multiverse branching.

## Local exploratory probing for coding agents

Keep CI tests fast with small fixed seed counts. Put longer TigerBeetle/Antithesis-style probing in dedicated scripts using `probe()` or `probeWorkers()`:

- Automatically skips in CI (`CI=true`) unless `CLOUDFLAKE_PROBE=1` is set.
- Override locally with `CLOUDFLAKE_PROBE_MS=30000`, `CLOUDFLAKE_PROBE_RUNS=2000`, or replay one failing seed with `CLOUDFLAKE_SEED=<seed>`.
- On failure, Cloudflake collects all distinct failure buckets, delta-debugs the injected faults down to the minimal trigger, and prints a replay command, minimal faults, causal trace, and diagnostic hints directly for you to act on.

## Rules that keep replay exact

- **Fresh state per run.** Construct every map, counter, database and runtime
  inside the scenario. Module-level state leaks between seeds and breaks replay.
- **No ambient time or randomness in the code under test.** `Date.now()`,
  `new Date()`, `Math.random()`, `crypto.randomUUID()` and real `setTimeout` are
  outside Cloudflake's control in a model test. Inject a clock reading `sim.now`
  and an id/random source built on `sim.int`, or the seed will not reproduce the
  failure. Inside `workerd`, the virtual runtime module moves all but `setTimeout`
  onto the virtual calendar; test timeouts by advancing virtual time, never by
  waiting in real time.
- **Actions run to completion.** An event's async body is awaited before the next
  event starts. A read-then-write race only shows up if the read and the write
  are separate events (`sim.after` inside the action) or a `checkpoint` sits
  between them.
- **Recreate request bodies inside the callback.** `sim.deliver` may run the
  callback twice; a consumed `Request` or stream cannot be resent.
- **Schedule the final check after the last possible delivery.** If deliveries
  use `maxDelay: 50`, put the settled assertion at `sim.after(51, …)` or later.
  Account for queue retry delays (`retryDelaySeconds * 1000` per attempt).
- **Invariants must hold mid-flight.** Use `sim.invariant` only for properties
  true after every event; eventual properties go in a scheduled final check.
- **Workerd is not fully controlled.** Native clocks, entropy, alarm delivery,
  `waitUntil` background work and workerd's internal scheduling are outside the
  seed. A seed reproduces Cloudflake's schedule; it reproduces the whole failure
  only when the property depends on controlled inputs. Prefer injected clocks,
  service-binding doubles and explicit `sim.after` alarms.
- **Bound runaway work.** A timer that reschedules forever fails with
  `maxSteps exceeded`. A hung external call needs a test-runner timeout.

## Minimal model test

```js
import assert from 'node:assert/strict'
import { campaign, simulate, simulatedQueue } from 'cloudflake'

async function payments(sim) {
  const posted = new Set()
  let balance = 0
  const queue = simulatedQueue(
    sim,
    'payments',
    ({ messages }) => {
      for (const { id, body } of messages) {
        if (posted.has(id)) continue // remove to see the failure
        posted.add(id)
        balance += body.amount
      }
    },
    { duplicate: 0.5 },
  )
  sim.invariant('one posting per payment', () => assert.equal(balance, 7 * posted.size))
  for (let i = 0; i < 8; i++) await queue.send({ amount: 7 })
  sim.after(1, 'all payments settled', () => assert.equal(balance, 56))
}

await campaign({ seed: 0, runs: 1000 }, payments)
await simulate({ seed: 42, onStep: console.log }, payments) // replay one schedule
```

## With Hegel property-based testing

Let Hegel generate the workload _and_ the seed, so it can shrink both:

```js
import { testAsync } from '@hegeldev/hegel'
import * as g from '@hegeldev/hegel/generators'

test('redelivery preserves the sum', () =>
  testAsync(
    async (tc) => {
      const amounts = tc.draw(
        g.arrays(g.integers({ minValue: 1, maxValue: 1000 }), { minSize: 1, maxSize: 12 }),
      )
      const seed = tc.draw(g.integers({ minValue: 0, maxValue: 0xffffffff }))
      await simulate({ seed }, async (sim) => {
        /* fresh state, use amounts */
      })
    },
    { testCases: 100 },
  ))
```

Draw values with `tc.draw` rather than `sim.int` when you want Hegel to shrink
them. Every case and every shrink attempt must build fresh state. Use fewer
`testCases` with `simulateWorkers`, since each case starts workerd.

## Choosing the tool

| Situation                                                       | Use                                                                                                      |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Logic with queue/alarm/retry semantics, no real storage needed  | `simulate` + `simulatedQueue` / `simulatedAlarm`                                                         |
| Real D1/KV/R2/DO storage, service bindings, multi-Worker        | `simulateWorkers`                                                                                        |
| A real `queue()` handler and its ack/retry behavior             | `workerQueue`                                                                                            |
| Two requests interleaving between a read and a write            | `client` + `checkpoint`, exposed to the Worker through a service binding                                 |
| Inputs whose shape matters (amounts, lists, malformed payloads) | Hegel generators + a drawn seed                                                                          |
| Durable Object restarts                                         | `runtime.unsafeEvictDurableObject(worker, className, { name })` with `unsafeInspectDurableObjects: true` |

Runnable references live in the package: `examples/counter.mjs`,
`examples/hegel.test.mjs`, and in the repository `test/workers.test.mjs`.
