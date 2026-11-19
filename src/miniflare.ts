import { cp, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  Miniflare,
  Response as MiniflareResponse,
  type MiniflareOptions,
  type Request as MiniflareRequest
} from 'miniflare'
import { simulatedAlarm, type AlarmInfo, type AlarmOptions } from './alarm.js'
import {
  CampaignCollector,
  PropertyCoverageFailure,
  type CampaignReport
} from './assertions.js'
import type { CheckpointHub } from './client.js'
import { simulatedKV, type SimulatedKVOptions } from './cloudflare.js'
import {
  Simulation,
  SimulationFailure,
  simulate,
  type CampaignOptions,
  type CampaignResult,
  type FaultEvent,
  type Options,
  type SimulationResult,
  type Step
} from './index.js'
import {
  ProbeFailure,
  formatAgentGuidance,
  inferFailureHints,
  shouldRunProbe,
  type ProbeFinding,
  type ProbeOptions,
  type ProbeReport
} from './probe.js'
import { simulatedQueue, type QueueOptions } from './queue.js'

/** One local deployment per simulation. The options factory can bind test doubles
 * to the seed's virtual clock and random source. */
export async function simulateWorkers(
  options: Options,
  deployment: (sim: Simulation) => MiniflareOptions,
  scenario: (sim: Simulation, runtime: Miniflare) => void | Promise<void>
): Promise<SimulationResult> {
  let runtime: Miniflare | undefined
  try {
    return await simulate(options, async sim => {
      runtime = new Miniflare({ cf: false, ...deployment(sim) })
      await runtime.ready
      await scenario(sim, runtime)
    })
  } finally {
    await runtime?.dispose()
  }
}

export type CampaignWorkersOptions = CampaignOptions & {
  /** Number of isolated Miniflare instances to run in parallel. Default 1. */
  concurrency?: number
}

/**
 * Multi-seed campaign runner for `simulateWorkers` with optional parallel `workerd`
 * execution, duration budgets, Antithesis property coverage aggregation, and failure bucketing.
 */
function integerOption(name: string, value: number, min: number) {
  if (!Number.isSafeInteger(value) || value < min) {
    throw new RangeError(`campaignWorkers ${name}: expected an integer >= ${min}, received ${value}`)
  }
  return value
}

export async function campaignWorkers(
  options: CampaignWorkersOptions,
  deployment: (sim: Simulation) => MiniflareOptions,
  scenario: (sim: Simulation, runtime: Miniflare) => void | Promise<void>
): Promise<CampaignResult> {
  if (options.runs === undefined && options.durationMs === undefined) {
    throw new RangeError('campaignWorkers requires either runs or durationMs')
  }
  const maxRuns = options.runs === undefined ? Number.MAX_SAFE_INTEGER : integerOption('runs', options.runs, 1)
  const durationMs = options.durationMs === undefined ? Infinity : integerOption('durationMs', options.durationMs, 1)
  integerOption('seed', options.seed, 0)
  const concurrency = Math.max(1, Math.floor(options.concurrency ?? 1))
  const onFailure = options.onFailure ?? 'stop'
  const requireProperties = options.requireProperties ?? true

  const collector = new CampaignCollector()
  for (const name of options.requireReachable ?? []) collector.expect(name, 'reachable')
  for (const name of options.requireSometimes ?? []) collector.expect(name, 'sometimes')

  const startedAt = performance.now()
  let nextIndex = 0
  let stopped = false
  let firstError: unknown

  const runOne = async (index: number) => {
    const seed = options.seed + index
    let runtime: Miniflare | undefined
    const sim = new Simulation({ ...options, seed })
    try {
      const result = await sim.run(async s => {
        runtime = new Miniflare({ cf: false, ...deployment(s) })
        await runtime.ready
        await scenario(s, runtime)
      })
      collector.merge(sim._tracker(), result.steps)
    } catch (err) {
      collector.merge(sim._tracker(), sim.steps)
      if (onFailure === 'stop') {
        if (!stopped) {
          stopped = true
          firstError = err
        }
      } else if (err instanceof SimulationFailure) {
        collector.recordFailure(err.seed, err.phase, err.cause)
      } else {
        collector.recordFailure(seed, 'unknown', err)
      }
    } finally {
      await runtime?.dispose()
    }
  }

  const workerLoop = async () => {
    while (!stopped) {
      const idx = nextIndex++
      if (idx >= maxRuns) break
      if (idx > 0 && performance.now() - startedAt >= durationMs) break
      await runOne(idx)
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, maxRuns) }, workerLoop))

  if (firstError !== undefined) throw firstError

  const report: CampaignReport = collector.finalize(performance.now() - startedAt)
  if (requireProperties && report.unmetProperties.length > 0 && report.failures.length === 0) {
    throw new PropertyCoverageFailure(report.unmetProperties, report)
  }
  return { runs: report.runs, steps: report.steps, report }
}

/**
 * Drive a real Worker's `queue()` handler from the virtual calendar and honor its
 * ack/retry response, including multi-message batches and partial batch retries.
 */
export async function workerQueue<T>(
  sim: Simulation,
  runtime: Miniflare,
  workerName: string,
  queueName: string,
  options?: QueueOptions<T>
) {
  const worker = await runtime.getWorker(workerName)
  return simulatedQueue<T>(
    sim,
    queueName,
    async ({ messages }) => {
      const payload = messages.map(({ id, timestamp, attempts, body }) => ({
        id,
        timestamp,
        attempts,
        body
      }))
      const response = await worker.queue(queueName, payload)
      for (const message of messages) {
        if (response.explicitAcks.includes(message.id)) {
          message.ack()
          continue
        }
        const retry = response.retryMessages.find(
          (entry: { msgId: string }) => entry.msgId === message.id
        )
        if (retry || response.retryBatch.retry || response.outcome !== 'ok') {
          message.retry({
            delaySeconds: retry?.delaySeconds ?? response.retryBatch.delaySeconds
          })
        }
      }
    },
    options
  )
}

export type OutboundFaultOptions = {
  /** Optional CheckpointHub to pause calling `client()`s across outbound fetch() calls. */
  hub?: CheckpointHub
  /** Checkpoint pause delay in virtual ms (or range `[min, max]`) when `hub` is provided. Default 1. */
  checkpointDelayMs?: number | readonly [number, number]
  /** Probability of returning a transient HTTP error (429/500/503) without invoking handler. Default 0. */
  errorRate?: number
  errorStatuses?: readonly number[]
  /** Probability of failing the fetch with a network/timeout error before invoking handler. Default 0. */
  timeoutRate?: number
  /**
   * Probability of an ambiguous failure: invokes `handler` (mutating external state),
   * then returns a 504/network failure to the calling Worker. Default 0.
   */
  ambiguousRate?: number
  /** HTTP status returned for injected timeouts/ambiguous failures (default 504). */
  faultStatus?: number
}

/**
 * Drop-in Miniflare `outboundService` (or `serviceBindings` handler) that intercepts
 * outbound `fetch()` calls from Workers/DOs and injects checkpoints, transient errors,
 * timeouts, and ambiguous writes.
 *
 * Returns `faultStatus` (504) with `x-cloudflake-fault: timeout | ambiguous` headers on
 * simulated network drops; Workers using `createVirtualRuntimeModule()` automatically
 * convert those headers into thrown `TypeError('Network connection lost')` errors.
 */
export function outboundFaultService(
  sim: Simulation,
  handler: (request: MiniflareRequest) => Response | MiniflareResponse | Promise<Response | MiniflareResponse>,
  options: OutboundFaultOptions = {}
) {
  const rng = sim.rngStream('miniflare-outbound')
  const errorRate = options.errorRate ?? 0
  const errorStatuses = options.errorStatuses ?? [429, 500, 503]
  const timeoutRate = options.timeoutRate ?? 0
  const ambiguousRate = options.ambiguousRate ?? 0
  const faultStatus = options.faultStatus ?? 504

  const sampleCheckpointDelay = () => {
    const d = options.checkpointDelayMs ?? 1
    return typeof d === 'number' ? d : rng.range(d[0], d[1])
  }

  return async (request: MiniflareRequest): Promise<MiniflareResponse> => {
    const label = `${request.method} ${request.url}`
    if (options.hub) {
      await options.hub.pause(request, sampleCheckpointDelay(), `outbound ${label}`)
    }

    if (timeoutRate > 0 && sim.decideFault('outbound_timeout', timeoutRate, label, rng)) {
      return new MiniflareResponse(`outbound fetch timeout: ${label}`, {
        status: faultStatus,
        headers: { 'x-cloudflake-fault': 'timeout' }
      })
    }
    if (errorRate > 0 && sim.decideFault('outbound_error', errorRate, label, rng)) {
      const status = rng.pick(errorStatuses)
      return new MiniflareResponse(`injected transient error ${status}`, { status })
    }

    const res = await handler(request)

    if (ambiguousRate > 0 && sim.decideFault('outbound_ambiguous', ambiguousRate, label, rng)) {
      await res.body?.cancel?.()
      return new MiniflareResponse(`outbound connection lost after remote execution: ${label}`, {
        status: faultStatus,
        headers: { 'x-cloudflake-fault': 'ambiguous' }
      })
    }

    if (res instanceof MiniflareResponse) return res
    const body = await res.arrayBuffer()
    return new MiniflareResponse(body, {
      status: res.status,
      statusText: res.statusText,
      headers: Object.fromEntries(res.headers.entries())
    })
  }
}

export type ControlBindingOptions = {
  /** Signed per-Worker clock skew in virtual ms. Default 0. */
  clockSkewMs?: number
  /**
   * Probability a ctx.waitUntil task is dropped when using the virtual runtime shim. Default 0.
   * Only factory tasks (`ctx.waitUntil(() => work())`) can be dropped; a Promise is already
   * running, so it is passed through and never draws a drop fault.
   */
  waitUntilDrop?: number
  /** Alarm retry/duplicate/jitter policy for virtualized DO alarms. */
  alarm?: AlarmOptions
  /** Callback invoked on the host when a virtual DO alarm fires on `sim`'s calendar. */
  onAlarmFire?: (objectKey: string, info: AlarmInfo) => void | Promise<void>
}

/**
 * Miniflare `serviceBindings` handler (`__CLOUDFLAKE__`) that bridges inside-`workerd`
 * Workers & Durable Objects to Cloudflake's virtual clock, seeded PRNG, virtual DO
 * alarms, `waitUntil` drop faults, and Antithesis assertions (`always`/`sometimes`/`reachable`).
 */
export function controlServiceBinding(sim: Simulation, options: ControlBindingOptions = {}) {
  const rng = sim.rngStream('miniflare-control')
  const clockSkewMs = options.clockSkewMs ?? 0
  const waitUntilDrop = options.waitUntilDrop ?? 0
  const alarmSlots = new Map<string, ReturnType<typeof simulatedAlarm>>()

  const getAlarmSlot = (objectKey: string) => {
    let slot = alarmSlots.get(objectKey)
    if (!slot) {
      slot = simulatedAlarm(
        sim,
        objectKey,
        async info => {
          if (options.onAlarmFire) await options.onAlarmFire(objectKey, info)
        },
        options.alarm
      )
      alarmSlots.set(objectKey, slot)
    }
    return slot
  }

  return async (request: MiniflareRequest): Promise<MiniflareResponse> => {
    const url = new URL(request.url)
    if (url.pathname === '/sync') {
      const payload = {
        now: Math.max(0, sim.now + clockSkewMs),
        seed: rng.nextUint32(),
        chaosEnabled: sim.chaosEnabled
      }
      return new MiniflareResponse(JSON.stringify(payload), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/assert') {
      const body = (await request.json()) as {
        kind: 'always' | 'alwaysOrUnreachable' | 'sometimes' | 'reachable' | 'unreachable'
        name: string
        condition?: boolean
        details?: Record<string, unknown>
      }
      try {
        if (body.kind === 'always') sim.always(body.name, Boolean(body.condition), body.details)
        else if (body.kind === 'alwaysOrUnreachable') sim.alwaysOrUnreachable(body.name, Boolean(body.condition), body.details)
        else if (body.kind === 'sometimes') sim.sometimes(body.name, Boolean(body.condition), body.details)
        else if (body.kind === 'reachable') sim.reachable(body.name, body.details)
        else if (body.kind === 'unreachable') sim.unreachable(body.name, body.details)
      } catch (err) {
        // The Worker may catch the thrown error; the violation must still fail the run.
        sim._failDeferred(err)
        return new MiniflareResponse(
          JSON.stringify({ ok: false, error: err instanceof Error ? err.message : String(err) }),
          { status: 400, headers: { 'content-type': 'application/json' } }
        )
      }
      return new MiniflareResponse(JSON.stringify({ ok: true }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/cover') {
      const body = (await request.json()) as { edge: string }
      sim.cover(body.edge)
      return new MiniflareResponse(JSON.stringify({ ok: true }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/waituntil') {
      const body = (await request.json()) as { label?: string }
      const drop =
        waitUntilDrop > 0 &&
        sim.decideFault('waituntil_drop', waitUntilDrop, body.label ?? 'waitUntil', rng)
      return new MiniflareResponse(JSON.stringify({ drop }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/alarm/get') {
      const key = url.searchParams.get('key') ?? 'default'
      const at = await getAlarmSlot(key).getAlarm()
      return new MiniflareResponse(JSON.stringify({ at }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/alarm/set') {
      const body = (await request.json()) as { key: string; when: number }
      await getAlarmSlot(body.key).setAlarm(body.when)
      return new MiniflareResponse(JSON.stringify({ ok: true }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    if (url.pathname === '/alarm/delete') {
      const body = (await request.json()) as { key: string }
      await getAlarmSlot(body.key).deleteAlarm()
      return new MiniflareResponse(JSON.stringify({ ok: true }), {
        headers: { 'content-type': 'application/json' }
      })
    }

    return new MiniflareResponse('not found', { status: 404 })
  }
}

/**
 * Returns an ES module source string (`__cloudflake_runtime.mjs`) that can be bundled or
 * attached as a Miniflare module to synchronize `Date.now`, `Math.random`, `crypto.randomUUID`,
 * `ctx.waitUntil`, DO `ctx.storage` alarms, and Antithesis assertions inside `workerd`
 * with `controlServiceBinding(sim)`.
 */
export function createVirtualRuntimeModule(bindingName = '__CLOUDFLAKE__'): string {
  return `
let virtualNow = 0;
let rngState = 1;
let installed = false;

function nextUint32() {
  rngState = (rngState + 0x6d2b79f5) >>> 0;
  let x = Math.imul(rngState ^ (rngState >>> 15), 1 | rngState);
  x ^= x + Math.imul(x ^ (x >>> 7), 61 | x);
  return (x ^ (x >>> 14)) >>> 0;
}

function nextFloat() {
  return nextUint32() / 0x100000000;
}

function nextUUID() {
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 4) {
    const w = nextUint32();
    bytes[i] = w & 0xff;
    bytes[i + 1] = (w >>> 8) & 0xff;
    bytes[i + 2] = (w >>> 16) & 0xff;
    bytes[i + 3] = (w >>> 24) & 0xff;
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
  return hex.slice(0, 8) + '-' + hex.slice(8, 12) + '-' + hex.slice(12, 16) + '-' + hex.slice(16, 20) + '-' + hex.slice(20);
}

function patchGlobals() {
  if (installed) return;
  installed = true;
  Date.now = () => virtualNow;
  Math.random = () => nextFloat();
  if (globalThis.crypto) {
    try {
      Object.defineProperty(globalThis.crypto, 'randomUUID', {
        configurable: true,
        value: () => nextUUID()
      });
    } catch {}
  }
  const origFetch = globalThis.fetch;
  if (typeof origFetch === 'function') {
    globalThis.fetch = async (...args) => {
      const res = await origFetch(...args);
      const fault = res.headers?.get?.('x-cloudflake-fault');
      if (fault === 'timeout' || fault === 'ambiguous') {
        throw new TypeError('Network connection lost (' + fault + ')');
      }
      return res;
    };
  }
}

export async function syncRuntime(env) {
  patchGlobals();
  const control = env?.${bindingName};
  if (!control) return;
  const res = await control.fetch('https://cloudflake.internal/sync');
  const data = await res.json();
  virtualNow = data.now;
  rngState = data.seed >>> 0;
}

export function cloudflakeAssert(env) {
  const control = env?.${bindingName};
  const send = async (kind, name, condition, details) => {
    if (!control) return;
    const res = await control.fetch('https://cloudflake.internal/assert', {
      method: 'POST',
      body: JSON.stringify({ kind, name, condition, details })
    });
    if (!res.ok) {
      const body = await res.json();
      throw new Error(body.error || 'assertion failed');
    }
  };
  return {
    always: (name, condition, details) => send('always', name, condition, details),
    alwaysOrUnreachable: (name, condition, details) => send('alwaysOrUnreachable', name, condition, details),
    sometimes: (name, condition, details) => send('sometimes', name, condition, details),
    reachable: (name, details) => send('reachable', name, true, details),
    unreachable: (name, details) => send('unreachable', name, false, details),
    cover: async (edge) => {
      if (!control) return;
      await control.fetch('https://cloudflake.internal/cover', {
        method: 'POST',
        body: JSON.stringify({ edge })
      });
    }
  };
}

export function wrapWorker(worker) {
  const wrapped = {};
  if (worker.fetch) {
    wrapped.fetch = async (request, env, ctx) => {
      await syncRuntime(env);
      const control = env?.${bindingName};
      const wrappedCtx = ctx && control ? {
        passThroughOnException: () => ctx.passThroughOnException(),
        waitUntil(promiseOrFactory) {
          // A Promise's work is already running and cannot be dropped; only a factory
          // (ctx.waitUntil(() => work())) can be suppressed, so only factories draw a drop fault.
          if (typeof promiseOrFactory !== 'function') {
            ctx.waitUntil(promiseOrFactory);
            return;
          }
          ctx.waitUntil((async () => {
            const res = await control.fetch('https://cloudflake.internal/waituntil', {
              method: 'POST',
              body: JSON.stringify({ label: request.url })
            });
            const { drop } = await res.json();
            if (drop) return;
            await promiseOrFactory();
          })());
        }
      } : ctx;
      return worker.fetch(request, env, wrappedCtx);
    };
  }
  if (worker.queue) {
    wrapped.queue = async (batch, env, ctx) => {
      await syncRuntime(env);
      return worker.queue(batch, env, ctx);
    };
  }
  if (worker.scheduled) {
    wrapped.scheduled = async (controller, env, ctx) => {
      await syncRuntime(env);
      return worker.scheduled(controller, env, ctx);
    };
  }
  return wrapped;
}

export function wrapDurableObject(BaseClass, keyPrefix = 'do') {
  return class CloudflakeWrappedDO extends BaseClass {
    constructor(ctx, env) {
      const objectKey = keyPrefix + ':' + ctx.id.toString();
      const control = env?.${bindingName};
      if (control && ctx.storage) {
        const setAlarm = async (when) => {
          await control.fetch('https://cloudflake.internal/alarm/set', {
            method: 'POST',
            body: JSON.stringify({ key: objectKey, when: Number(when) })
          });
        };
        const getAlarm = async () => {
          const res = await control.fetch('https://cloudflake.internal/alarm/get?key=' + encodeURIComponent(objectKey));
          const { at } = await res.json();
          return at;
        };
        const deleteAlarm = async () => {
          await control.fetch('https://cloudflake.internal/alarm/delete', {
            method: 'POST',
            body: JSON.stringify({ key: objectKey })
          });
        };
        try {
          Object.defineProperty(ctx.storage, 'setAlarm', { configurable: true, value: setAlarm });
          Object.defineProperty(ctx.storage, 'getAlarm', { configurable: true, value: getAlarm });
          Object.defineProperty(ctx.storage, 'deleteAlarm', { configurable: true, value: deleteAlarm });
        } catch {}
      }
      super(ctx, env);
      this.__cfEnv = env;
    }
    async fetch(request) {
      await syncRuntime(this.__cfEnv);
      const alarmHeader = request.headers.get('x-cloudflake-alarm');
      if (alarmHeader && typeof this.alarm === 'function') {
        const info = JSON.parse(alarmHeader);
        await this.alarm(info);
        return new Response('alarm-ok');
      }
      return super.fetch(request);
    }
  };
}
`
}

export type DurableObjectTarget = {
  scriptName: string
  className: string
  id: { name: string } | { id: string }
}

export type DONemesisOptions = {
  /** Number of candidate eviction ticks to schedule. Default 4. */
  ticks?: number
  /** Virtual time window (ms) over which ticks are spread. Default 100. */
  windowMs?: number
  /** Eviction probability per tick (defaults to `sim.profile.eviction`). */
  probability?: number
}

/**
 * Automated Durable Object eviction nemesis for `simulateWorkers`.
 * Schedules seeded `unsafeEvictDurableObject` calls across the chaos window and
 * records `sim.sometimes('do evicted', ...)` coverage when a live DO is evicted.
 */
export function doNemesis(
  sim: Simulation,
  runtime: Miniflare,
  targets: readonly DurableObjectTarget[],
  options: DONemesisOptions = {}
) {
  if (targets.length === 0) return
  const ticks = options.ticks ?? 4
  const windowMs = options.windowMs ?? 100
  const prob = options.probability ?? sim.profile.eviction
  const rng = sim.rngStream('do-nemesis')

  for (let i = 0; i < ticks; i++) {
    const at = windowMs > 0 ? rng.range(0, windowMs) : 0
    sim.after(
      at,
      `nemesis tick ${i}`,
      async () => {
        const target = rng.pick(targets)
        const targetLabel = 'name' in target.id ? target.id.name : target.id.id
        const shouldTry = sim.decideFault(
          'do_evict',
          prob,
          `${target.className}/${targetLabel}`,
          rng
        )
        if (!shouldTry) {
          sim.sometimes('do evicted', false, { className: target.className, target: targetLabel })
          return
        }
        try {
          await runtime.unsafeEvictDurableObject(target.scriptName, target.className, target.id)
          sim.sometimes('do evicted', true, { className: target.className, target: targetLabel })
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          if (msg.includes('not currently running')) {
            sim.sometimes('do evicted', false, { className: target.className, target: targetLabel, skipped: 'cold' })
            return
          }
          throw err
        }
      },
      { kind: 'nemesis', actor: 'do-nemesis' }
    )
  }
}

/**
 * Snapshot a Miniflare persistence directory to an isolated temporary folder so you can
 * branch multiple fault timelines from the exact same `workerd` storage state (SQLite DOs,
 * D1, KV, R2) — lightweight Antithesis-style multiverse branching.
 */
export async function snapshotPersistDir(persistDir: string): Promise<{
  readonly dir: string
  /** Create an independent writable branch copy from this snapshot. */
  branch(): Promise<{ readonly dir: string; dispose(): Promise<void> }>
  dispose(): Promise<void>
}> {
  const snapshotDir = await mkdtemp(join(tmpdir(), 'cloudflake-snap-'))
  await cp(persistDir, snapshotDir, { recursive: true })
  return {
    dir: snapshotDir,
    async branch() {
      const branchDir = await mkdtemp(join(tmpdir(), 'cloudflake-branch-'))
      await cp(snapshotDir, branchDir, { recursive: true })
      return {
        dir: branchDir,
        async dispose() {
          await rm(branchDir, { recursive: true, force: true })
        }
      }
    },
    async dispose() {
      await rm(snapshotDir, { recursive: true, force: true })
    }
  }
}

export type QueueProxyConfig = {
  /** Consumer worker name in Miniflare's `workers` array. */
  workerName: string
  /** Queue name passed to the consumer's `queue(batch, env)` handler. */
  queueName: string
  options?: QueueOptions<unknown>
}

export type DOProxyConfig = {
  /** Worker name defining the Durable Object class. */
  scriptName: string
  /** Durable Object class name. */
  className: string
  /** Probability a DO RPC fails cleanly before executing. Default 0. */
  rpcReset?: number
  /** Probability a DO RPC commits, evicts the DO, and throws a reset error to the caller. Default 0. */
  rpcAmbiguous?: number
  /** Probability the DO is evicted right after the RPC completes. Default 0. */
  evictAfterCall?: number
  /**
   * JS RPC method names to forward through the fault-injecting stub (e.g. `['increment']`
   * for `env.COUNTER.getByName('a').increment()`). workerd resolves RpcTarget methods from
   * the class prototype, so each method must be listed. `fetch` is always forwarded.
   */
  rpcMethods?: readonly string[]
}

export type BindingProxiesConfig = {
  /** Name of the generated proxy worker (default `'__cloudflake_proxy__'`). */
  proxyWorkerName?: string
  /** Compatibility date for the proxy worker (default `'2026-07-01'`). */
  compatibilityDate?: string
  /** Virtual-time queue bindings: Workers calling `env[binding].send()` route into `workerQueue`. */
  queues?: Record<string, QueueProxyConfig>
  /** Fault-injecting KV bindings: Workers calling `env[binding].get/put/delete/list` route into `simulatedKV`. */
  kv?: Record<string, SimulatedKVOptions>
  /** Fault-injecting DO bindings: Workers calling `env[binding].getByName/get` route through RPC fault injection. */
  durableObjects?: Record<string, DOProxyConfig>
}

/**
 * Create drop-in `WorkerEntrypoint` / `RpcTarget` binding proxies for `env.QUEUE`,
 * `env.KV`, and `env.DO` inside `workerd` so real Workers get virtual-time queues,
 * eventual-consistency KV stale reads, and DO RPC reset/ambiguous faults without
 * changing application code.
 */
export function createBindingProxies(sim: Simulation, config: BindingProxiesConfig) {
  const proxyWorkerName = config.proxyWorkerName ?? '__cloudflake_proxy__'
  const compatibilityDate = config.compatibilityDate ?? '2026-07-01'
  const rng = sim.rngStream('miniflare-proxies')

  const kvStores: Record<string, ReturnType<typeof simulatedKV>> = {}
  for (const [binding, opts] of Object.entries(config.kv ?? {})) {
    kvStores[binding] = simulatedKV(sim, binding, opts)
  }

  let runtimeRef: Miniflare | undefined
  const queueProducers = new Map<string, Awaited<ReturnType<typeof workerQueue>>>()

  const ensureQueue = async (binding: string) => {
    let prod = queueProducers.get(binding)
    if (!prod) {
      const cfg = config.queues?.[binding]
      if (!cfg) throw new Error(`unknown proxied queue binding "${binding}"`)
      if (!runtimeRef) throw new Error('call proxies.attach(runtime) before sending to proxied queues')
      prod = await workerQueue(sim, runtimeRef, cfg.workerName, cfg.queueName, cfg.options)
      queueProducers.set(binding, prod)
    }
    return prod
  }

  const tryEvict = async (cfg: DOProxyConfig, target: { name?: string; id?: string }) => {
    if (!runtimeRef) return
    const idOpt = target.name !== undefined ? { name: target.name } : { id: target.id! }
    try {
      await runtimeRef.unsafeEvictDurableObject(cfg.scriptName, cfg.className, idOpt)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      if (!msg.includes('not currently running')) throw err
    }
  }

  const hostHandler = async (request: MiniflareRequest): Promise<MiniflareResponse> => {
    const url = new URL(request.url)
    const parts = url.pathname.split('/').filter(Boolean)
    const kind = parts[0]
    const binding = parts[1]
    const op = parts[2]

    if (kind === 'queue' && binding) {
      const prod = await ensureQueue(binding)
      const body = (await request.json()) as Record<string, unknown>
      if (op === 'send') {
        await prod.send(body.body, body.options as { delaySeconds?: number } | undefined)
        return new MiniflareResponse(JSON.stringify({ ok: true }), {
          headers: { 'content-type': 'application/json' }
        })
      }
      if (op === 'sendBatch') {
        await prod.sendBatch(
          body.messages as { body: unknown; delaySeconds?: number }[],
          body.options as { delaySeconds?: number } | undefined
        )
        return new MiniflareResponse(JSON.stringify({ ok: true }), {
          headers: { 'content-type': 'application/json' }
        })
      }
    }

    if (kind === 'kv' && binding) {
      const store = kvStores[binding]
      if (!store) return new MiniflareResponse('unknown kv', { status: 404 })
      const body = (await request.json()) as Record<string, unknown>
      try {
        if (op === 'get') {
          const val = await store.get(String(body.key))
          return new MiniflareResponse(JSON.stringify({ value: val }), {
            headers: { 'content-type': 'application/json' }
          })
        }
        if (op === 'put') {
          await store.put(
            String(body.key),
            String(body.value),
            body.options as { expirationTtl?: number; expiration?: number } | undefined
          )
          return new MiniflareResponse(JSON.stringify({ ok: true }), {
            headers: { 'content-type': 'application/json' }
          })
        }
        if (op === 'delete') {
          await store.delete(String(body.key))
          return new MiniflareResponse(JSON.stringify({ ok: true }), {
            headers: { 'content-type': 'application/json' }
          })
        }
        if (op === 'list') {
          const res = await store.list(body.options as { prefix?: string } | undefined)
          return new MiniflareResponse(JSON.stringify(res), {
            headers: { 'content-type': 'application/json' }
          })
        }
      } catch (err) {
        return new MiniflareResponse(
          JSON.stringify({ error: err instanceof Error ? err.message : String(err) }),
          { status: 500, headers: { 'content-type': 'application/json' } }
        )
      }
    }

    if (kind === 'do' && binding) {
      const cfg = config.durableObjects?.[binding]
      if (!cfg) return new MiniflareResponse('unknown do', { status: 404 })
      const body = (await request.json()) as { target: { name?: string; id?: string } }
      const targetLabel = `${cfg.className}/${body.target.name ?? body.target.id ?? 'unknown'}`

      if (op === 'pre') {
        const reset =
          (cfg.rpcReset ?? 0) > 0 &&
          sim.decideFault('do_rpc_reset', cfg.rpcReset ?? 0, `${targetLabel} pre`, rng)
        return new MiniflareResponse(JSON.stringify({ reset }), {
          headers: { 'content-type': 'application/json' }
        })
      }
      if (op === 'post') {
        const ambiguous =
          (cfg.rpcAmbiguous ?? 0) > 0 &&
          sim.decideFault('do_rpc_ambiguous', cfg.rpcAmbiguous ?? 0, `${targetLabel} post`, rng)
        if (ambiguous) {
          sim.after(0, `evict ${targetLabel} (post-ambiguous)`, () => tryEvict(cfg, body.target), {
            kind: 'do-evict',
            actor: targetLabel
          })
          return new MiniflareResponse(JSON.stringify({ ambiguous: true }), {
            headers: { 'content-type': 'application/json' }
          })
        }
        if (
          (cfg.evictAfterCall ?? 0) > 0 &&
          sim.decideFault('do_evict', cfg.evictAfterCall ?? 0, `${targetLabel} post`, rng)
        ) {
          sim.after(0, `evict ${targetLabel} (post-call)`, () => tryEvict(cfg, body.target), {
            kind: 'do-evict',
            actor: targetLabel
          })
        }
        return new MiniflareResponse(JSON.stringify({ ambiguous: false }), {
          headers: { 'content-type': 'application/json' }
        })
      }
    }

    return new MiniflareResponse('not found', { status: 404 })
  }

  // Generate WorkerEntrypoint classes for each configured binding
  const classBlocks: string[] = [
    `import { WorkerEntrypoint, RpcTarget } from 'cloudflare:workers';`
  ]
  const serviceBindings: Record<string, { name: string; entrypoint: string }> = {}
  const proxyDOBindings: Record<string, { className: string; scriptName: string }> = {}

  for (const binding of Object.keys(config.queues ?? {})) {
    const cls = `QueueProxy_${binding}`
    serviceBindings[binding] = { name: proxyWorkerName, entrypoint: cls }
    classBlocks.push(`
export class ${cls} extends WorkerEntrypoint {
  async send(body, options) {
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/queue/${binding}/send', {
      method: 'POST', body: JSON.stringify({ body, options })
    });
    if (!res.ok) throw new Error(await res.text());
  }
  async sendBatch(messages, options) {
    const payload = Array.from(messages, m => ({ body: m.body, delaySeconds: m.delaySeconds }));
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/queue/${binding}/sendBatch', {
      method: 'POST', body: JSON.stringify({ messages: payload, options })
    });
    if (!res.ok) throw new Error(await res.text());
  }
}`)
  }

  for (const binding of Object.keys(config.kv ?? {})) {
    const cls = `KVProxy_${binding}`
    serviceBindings[binding] = { name: proxyWorkerName, entrypoint: cls }
    classBlocks.push(`
export class ${cls} extends WorkerEntrypoint {
  async get(key, options) {
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/kv/${binding}/get', {
      method: 'POST', body: JSON.stringify({ key })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'KV get failed');
    if (data.value === null) return null;
    const type = typeof options === 'string' ? options : options?.type;
    if (type === 'json') return JSON.parse(data.value);
    return data.value;
  }
  async getWithMetadata(key, options) {
    const value = await this.get(key, options);
    return { value, metadata: null, cacheStatus: null };
  }
  async put(key, value, options) {
    const str = typeof value === 'string' ? value : String(value);
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/kv/${binding}/put', {
      method: 'POST', body: JSON.stringify({ key, value: str, options })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error || 'KV put failed');
    }
  }
  async delete(key) {
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/kv/${binding}/delete', {
      method: 'POST', body: JSON.stringify({ key })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error || 'KV delete failed');
    }
  }
  async list(options) {
    const res = await this.env.__CF_PROXY__.fetch('https://proxy/kv/${binding}/list', {
      method: 'POST', body: JSON.stringify({ options })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'KV list failed');
    return { keys: data.keys, list_complete: true, cacheStatus: null };
  }
}`)
  }

  if (Object.keys(config.durableObjects ?? {}).length > 0) {
    classBlocks.push(`
async function withDOFaults(self, run, discard) {
  const pre = await (await self.proxy.fetch('https://proxy/do/' + self.binding + '/pre', {
    method: 'POST', body: JSON.stringify({ target: self.target })
  })).json();
  if (pre.reset) throw new Error('Durable Object reset: overloaded');
  const result = await run();
  const post = await (await self.proxy.fetch('https://proxy/do/' + self.binding + '/post', {
    method: 'POST', body: JSON.stringify({ target: self.target })
  })).json();
  if (post.ambiguous) {
    await discard?.(result);
    throw new Error('Durable Object reset: connection lost after execution');
  }
  return result;
}

class FaultyDOStub extends RpcTarget {
  constructor(stub, proxy, binding, target) {
    super();
    this.stub = stub;
    this.proxy = proxy;
    this.binding = binding;
    this.target = target;
  }
  fetch(input, init) {
    return withDOFaults(this, () => this.stub.fetch(input, init), res => res.body?.cancel());
  }
}`)
  }

  for (const [binding, cfg] of Object.entries(config.durableObjects ?? {})) {
    const cls = `DOProxy_${binding}`
    const stubCls = `FaultyDOStub_${binding}`
    const realKey = `__REAL_DO_${binding}__`
    serviceBindings[binding] = { name: proxyWorkerName, entrypoint: cls }
    proxyDOBindings[realKey] = { className: cfg.className, scriptName: cfg.scriptName }
    const rpcMethods = (cfg.rpcMethods ?? []).filter(m => m !== 'fetch')
    for (const method of rpcMethods) {
      if (!/^[A-Za-z_$][\w$]*$/.test(method) || method === 'constructor' || method.startsWith('__')) {
        throw new RangeError(`durableObjects.${binding}.rpcMethods: invalid method name "${method}"`)
      }
    }
    classBlocks.push(`
class ${stubCls} extends FaultyDOStub {
${rpcMethods
  .map(m => `  ${m}(...args) { return withDOFaults(this, () => this.stub.${m}(...args)); }`)
  .join('\n')}
}`)
    classBlocks.push(`
export class ${cls} extends WorkerEntrypoint {
  idFromName(name) { return { name: String(name) }; }
  idFromString(id) { return { id: String(id) }; }
  getByName(name) {
    const stub = this.env.${realKey}.getByName(name);
    return new ${stubCls}(stub, this.env.__CF_PROXY__, '${binding}', { name: String(name) });
  }
  get(handle) {
    const isNamed = handle && typeof handle === 'object' && handle.name !== undefined;
    const stub = isNamed
      ? this.env.${realKey}.getByName(handle.name)
      : this.env.${realKey}.get(this.env.${realKey}.idFromString(handle.id ?? String(handle)));
    const target = isNamed ? { name: handle.name } : { id: handle.id ?? String(handle) };
    return new ${stubCls}(stub, this.env.__CF_PROXY__, '${binding}', target);
  }
}`)
  }

  classBlocks.push(`export default { fetch() { return new Response('cloudflake-proxy'); } };`)

  const proxyWorker = {
    name: proxyWorkerName,
    modules: true as const,
    compatibilityDate,
    script: classBlocks.join('\n'),
    serviceBindings: { __CF_PROXY__: hostHandler },
    ...(Object.keys(proxyDOBindings).length > 0 ? { durableObjects: proxyDOBindings } : {})
  }

  return {
    /** Worker config to append to Miniflare's `workers` array. */
    proxyWorker,
    /** Drop-in `serviceBindings` to spread into your app Worker(s). */
    serviceBindings,
    /** Direct access to host `simulatedKV` instances (e.g. `.inspect(key)` for invariants). */
    kv: kvStores,
    /** Attach the live Miniflare runtime inside your scenario so queues and DO evictions route cleanly. */
    async attach(runtime: Miniflare) {
      runtimeRef = runtime
      for (const binding of Object.keys(config.queues ?? {})) {
        await ensureQueue(binding)
      }
    }
  }
}

export type ProbeWorkersOptions = ProbeOptions & {
  /** Number of isolated Miniflare instances to run in parallel. Default 1. */
  concurrency?: number
}

/**
 * Local exploratory `workerd` prober (auto-skipped in CI by default): runs parallel
 * `campaignWorkers` in collect mode, delta-debugs failing seeds with `disabledFaults`,
 * and emits structured, actionable agent guidance.
 */
export async function probeWorkers(
  options: ProbeWorkersOptions,
  deployment: (sim: Simulation) => MiniflareOptions,
  scenario: (sim: Simulation, runtime: Miniflare) => void | Promise<void>
): Promise<ProbeReport> {
  const gate = shouldRunProbe({ allowInCI: options.allowInCI, requireEnv: options.requireEnv })
  if (!gate.run) {
    const skipped: Omit<ProbeReport, 'guidance'> = {
      name: options.name,
      status: 'skipped',
      skippedReason: gate.reason,
      findings: [],
      unmetProperties: []
    }
    return { ...skipped, guidance: formatAgentGuidance(skipped) }
  }

  const envSeed = process.env.CLOUDFLAKE_SEED
  const envMs = process.env.CLOUDFLAKE_PROBE_MS
  const envRuns = process.env.CLOUDFLAKE_PROBE_RUNS

  const replayTemplate = options.replayCommand ?? `CLOUDFLAKE_SEED={seed} CLOUDFLAKE_PROBE=1 node <probe-script>`
  const seed = envSeed !== undefined && envSeed !== '' ? Number(envSeed) : (options.seed ?? 0)
  const runs =
    envSeed !== undefined && envSeed !== ''
      ? 1
      : envRuns !== undefined && envRuns !== ''
        ? Number(envRuns)
        : options.runs
  const durationMs =
    envSeed !== undefined && envSeed !== ''
      ? undefined
      : envMs !== undefined && envMs !== ''
        ? Number(envMs)
        : options.durationMs ?? (runs === undefined ? 10_000 : undefined)

  const res = await campaignWorkers(
    {
      ...options,
      seed,
      runs,
      durationMs,
      onFailure: 'collect',
      requireProperties: false
    },
    deployment,
    scenario
  )

  const maxMinimize = options.maxMinimize ?? 3
  const findings: ProbeFinding[] = []

  for (let i = 0; i < res.report.failures.length; i++) {
    const bucket = res.report.failures[i]!
    let minimalFaults: readonly FaultEvent[] = []
    let originalFaultCount = 0
    let trace: readonly Step[] = []

    if (i < maxMinimize) {
      let initialFailure: SimulationFailure | undefined
      try {
        await simulateWorkers(
          { ...options, seed: bucket.firstSeed, traceLimit: Math.max(options.traceLimit ?? 32, 64) },
          deployment,
          scenario
        )
      } catch (err) {
        if (err instanceof SimulationFailure) initialFailure = err
      }

      if (initialFailure) {
        const original = initialFailure.firedFaults
        originalFaultCount = original.length
        minimalFaults = original
        trace = initialFailure.trace

        // Delta-debug faults for workerd failures when fault count is tractable
        if (original.length >= 2 && original.length <= 24) {
          const allIndices = new Set(original.map(f => f.index))
          let active = original.map(f => f.index)
          let n = 2
          while (active.length >= 2) {
            const chunkSize = Math.ceil(active.length / n)
            let reduced = false
            for (let j = 0; j < active.length; j += chunkSize) {
              const candidate = active.slice(0, j).concat(active.slice(j + chunkSize))
              const keep = new Set(candidate)
              const disabled = new Set<number>()
              for (const idx of allIndices) if (!keep.has(idx)) disabled.add(idx)
              try {
                await simulateWorkers(
                  { ...options, seed: bucket.firstSeed, traceLimit: 64, disabledFaults: disabled },
                  deployment,
                  scenario
                )
              } catch (err) {
                if (err instanceof SimulationFailure && err.phase === initialFailure.phase) {
                  active = candidate
                  trace = err.trace
                  n = Math.max(2, n - 1)
                  reduced = true
                  break
                }
              }
            }
            if (!reduced) {
              if (n >= active.length) break
              n = Math.min(active.length, n * 2)
            }
          }
          const kept = new Set(active)
          minimalFaults = original.filter(f => kept.has(f.index))
        }
      }
    }

    findings.push({
      phase: bucket.phase,
      message: bucket.message,
      occurrences: bucket.count,
      firstSeed: bucket.firstSeed,
      sampleSeeds: bucket.seeds,
      replayCommand: replayTemplate.replace('{seed}', String(bucket.firstSeed)),
      originalFaultCount,
      minimalFaults,
      trace,
      hints: inferFailureHints(bucket.phase, bucket.message, minimalFaults)
    })
  }

  const requireProperties = options.requireProperties ?? true
  const unmetProperties = requireProperties ? res.report.unmetProperties : []
  const status: ProbeReport['status'] =
    findings.length > 0
      ? 'fail_safety'
      : unmetProperties.length > 0
        ? 'fail_coverage'
        : 'pass'

  const base: Omit<ProbeReport, 'guidance'> = {
    name: options.name,
    status,
    report: res.report,
    findings,
    unmetProperties
  }
  const full: ProbeReport = { ...base, guidance: formatAgentGuidance(base) }

  const printMode = options.printGuidance ?? 'onFailure'
  if (printMode === 'always' || (printMode === 'onFailure' && status !== 'pass')) {
    const stream = status === 'pass' ? console.log : console.error
    stream(full.guidance)
  }

  if ((options.throwOnFailure ?? true) && status !== 'pass') {
    throw new ProbeFailure(full)
  }
  return full
}

