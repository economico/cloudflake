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
   an *invariant* (true after every event) and a *final check* (true once the
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
import { simulate, campaign, simulatedQueue, simulatedAlarm, client, SimulationFailure } from 'cloudflake'
```

| Call | Behavior |
| --- | --- |
| `simulate(options, scenario)` | Runs one schedule. Resolves `{ seed, steps, time }` or throws `SimulationFailure`. |
| `campaign({ ...options, runs }, scenario)` | Runs seeds `seed … seed + runs - 1`. Resolves `{ runs, steps }`; throws the first failure. |
| `sim.after(ms, label, action)` | Schedules `action` at `now + ms` virtual milliseconds. Returns a cancel function. Equal-time events are ordered by the seed. |
| `sim.deliver(label, action, { maxDelay, drop, duplicate })` | Schedules a delivery with seeded latency `0..maxDelay`, a drop probability, and a probability of one extra delivery. The callback runs again for the duplicate. |
| `sim.invariant(name, check)` | Runs `check` after setup and after every event. Throw (e.g. `assert`) to fail. |
| `sim.int(limit)` | Seeded integer in `[0, limit)`. Use for workload choices inside the scenario. |
| `sim.now`, `sim.steps`, `sim.seed` | Virtual time in ms, events run so far, current seed. |
| `sim.signal` | `AbortSignal` that aborts parked clients when the run fails. |

Options: `seed` (uint32, required), `startTime` (ms, default `0`), `maxSteps`
(default `100000`; exceeding it fails the run), `traceLimit` (steps kept on the
failure, default `32`), `onStep(step)` (called with `{ id, time, label }`).

Helpers:

- `simulatedQueue(sim, name, consumer, { maxRetries = 3, retryDelaySeconds = 0, duplicate = 0, onExhausted })`
  returns `{ send(body, { delaySeconds }), sendBatch(messages, options) }`. The
  consumer receives `{ queue, messages: [message], ackAll, retryAll }` with exactly
  one message per batch. A message has a stable `id`, `timestamp`, `attempts`,
  a `structuredClone`d `body`, `ack()` and `retry({ delaySeconds })`. Returning
  without a decision acks. Throwing retries. After `maxRetries` retries the run
  fails unless `onExhausted(message)` is supplied. Duplicated deliveries keep the
  same `id`, as Cloudflare's at-least-once delivery can.
- `simulatedAlarm(sim, objectName, fire)` returns `{ getAlarm, setAlarm, deleteAlarm }`
  with one replaceable slot. Times are absolute virtual ms, so pass
  `sim.now + delay`. Inject it in place of `ctx.storage` alarm methods.
- `client(sim, name, async checkpoint => { … })` runs a sequential client that
  yields at `await checkpoint(delayMs, label)`. Other events, including other
  clients, run while it is parked. Use this to interleave two requests *inside*
  their I/O, which plain events cannot do.

## Workers adapter (`cloudflake/miniflare`)

```js
import { simulateWorkers, workerQueue } from 'cloudflake/miniflare'

await simulateWorkers({ seed }, sim => miniflareOptions, async (sim, runtime) => { … })
```

- The second argument is a factory returning normal `MiniflareOptions`, including
  `workers: [...]` for multi-Worker deployments. It receives `sim`, so test
  doubles can read `sim.now` or `sim.int`.
- A fresh Miniflare is created and disposed for every run. Do not set
  persistence directories or remote bindings; state must start empty each seed.
- `runtime` is the Miniflare instance: `dispatchFetch`, `getKVNamespace`,
  `getR2Bucket`, `getD1Database`, `getWorker`, `unsafeEvictDurableObject`, etc.
  Pass the Worker name as the second argument in multi-Worker setups.
- `await workerQueue(sim, runtime, workerName, queueName, options)` returns a
  producer that drives the Worker's real `queue()` handler on the virtual
  calendar and honors its `ack`/`retry` response, including `delaySeconds`.
- Build or bundle the Worker once outside the campaign and reuse `scriptPath`.

## Rules that keep replay exact

- **Fresh state per run.** Construct every map, counter, database and runtime
  inside the scenario. Module-level state leaks between seeds and breaks replay.
- **No ambient time or randomness in the code under test.** `Date.now()`,
  `Math.random()`, `crypto.randomUUID()` and real `setTimeout` are outside
  Cloudflake's control. Inject a clock reading `sim.now` and an id/random source
  built on `sim.int`, or the seed will not reproduce the failure.
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
  const queue = simulatedQueue(sim, 'payments', ({ messages }) => {
    for (const { id, body } of messages) {
      if (posted.has(id)) continue          // remove to see the failure
      posted.add(id)
      balance += body.amount
    }
  }, { duplicate: 0.5 })
  sim.invariant('one posting per payment', () => assert.equal(balance, 7 * posted.size))
  for (let i = 0; i < 8; i++) await queue.send({ amount: 7 })
  sim.after(1, 'all payments settled', () => assert.equal(balance, 56))
}

await campaign({ seed: 0, runs: 1000 }, payments)
await simulate({ seed: 42, onStep: console.log }, payments)   // replay one schedule
```

## With Hegel property-based testing

Let Hegel generate the workload *and* the seed, so it can shrink both:

```js
import { testAsync } from '@hegeldev/hegel'
import * as g from '@hegeldev/hegel/generators'

test('redelivery preserves the sum', () => testAsync(async tc => {
  const amounts = tc.draw(g.arrays(g.integers({ minValue: 1, maxValue: 1000 }), { minSize: 1, maxSize: 12 }))
  const seed = tc.draw(g.integers({ minValue: 0, maxValue: 0xffffffff }))
  await simulate({ seed }, async sim => { /* fresh state, use amounts */ })
}, { testCases: 100 }))
```

Draw values with `tc.draw` rather than `sim.int` when you want Hegel to shrink
them. Every case and every shrink attempt must build fresh state. Use fewer
`testCases` with `simulateWorkers`, since each case starts workerd.

## Choosing the tool

| Situation | Use |
| --- | --- |
| Logic with queue/alarm/retry semantics, no real storage needed | `simulate` + `simulatedQueue` / `simulatedAlarm` |
| Real D1/KV/R2/DO storage, service bindings, multi-Worker | `simulateWorkers` |
| A real `queue()` handler and its ack/retry behavior | `workerQueue` |
| Two requests interleaving between a read and a write | `client` + `checkpoint`, exposed to the Worker through a service binding |
| Inputs whose shape matters (amounts, lists, malformed payloads) | Hegel generators + a drawn seed |
| Durable Object restarts | `runtime.unsafeEvictDurableObject(worker, className, { name })` with `unsafeInspectDurableObjects: true` |

Runnable references live in the package: `examples/counter.mjs`,
`examples/hegel.test.mjs`, and in the repository `test/workers.test.mjs`.
