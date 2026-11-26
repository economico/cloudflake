// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

// In-process Cloudflare primitive simulators that model real edge failure modes:
// - KV eventual consistency (stale reads, propagation lag, virtual-time TTLs)
// - Durable Objects (durable vs ephemeral state, eviction, abort/rollback,
//   blockConcurrencyWhile input gates, realistic alarms, ambiguous RPC resets)
// - ctx.waitUntil() background drops/delays
// - Outbound fetch() transient 429/5xx, timeouts, and ambiguous writes
import { simulatedAlarm, type AlarmInfo, type AlarmOptions } from './alarm.js'
import type { Simulation } from './index.js'
import type { DelayDistribution } from './prng.js'

const DELETED: unique symbol = Symbol('deleted')

export class DurableObjectResetError extends Error {
  readonly retryable: boolean
  readonly ambiguous: boolean
  constructor(
    object: string,
    message: string,
    options: { retryable?: boolean; ambiguous?: boolean } = {},
  ) {
    super(`Durable Object "${object}" reset: ${message}`)
    this.name = 'DurableObjectResetError'
    this.retryable = options.retryable ?? true
    this.ambiguous = options.ambiguous ?? false
  }
}

// --- Simulated KV (Eventual Consistency) ---

export type SimulatedKVOptions = {
  /** Virtual ms before a write is guaranteed visible across all PoPs. Default 60_000. */
  propagationDelayMs?: number
  /** Probability a read returns a stale version within the propagation window (or previous value). Default 0. */
  staleRead?: number
  /** Probability a KV call throws a transient 5xx error. Default 0. */
  errorRate?: number
}

type KVVersion = {
  value: string | null
  writtenAt: number
  visibleAt: number
  expiresAt: number | null
}

export function simulatedKV(sim: Simulation, name: string, options: SimulatedKVOptions = {}) {
  const propagationDelayMs = options.propagationDelayMs ?? 60_000
  const staleRead = options.staleRead ?? 0
  const errorRate = options.errorRate ?? 0
  const rng = sim.rngStream(`kv:${name}`)
  const history = new Map<string, KVVersion[]>()

  const maybeError = (op: string) => {
    if (errorRate > 0 && sim.decideFault('kv_error', errorRate, `${name}.${op}`, rng)) {
      throw new Error(`KV "${name}" transient error during ${op}`)
    }
  }

  const readVersion = (key: string, allowStale: boolean): string | null => {
    const versions = history.get(key)
    if (!versions || versions.length === 0) return null
    const latest = versions[versions.length - 1]!

    // If within propagation window and staleRead triggers, serve an older version (or null if key is new).
    if (
      allowStale &&
      staleRead > 0 &&
      sim.now < latest.visibleAt &&
      sim.decideFault('kv_stale_read', staleRead, `${name}.get(${key})`, rng)
    ) {
      // Find versions written before the latest propagation window settled
      const older = versions.slice(0, -1)
      if (older.length === 0) return null
      const picked = older[rng.int(older.length)]!
      if (picked.expiresAt !== null && sim.now >= picked.expiresAt) return null
      return picked.value
    }

    if (latest.expiresAt !== null && sim.now >= latest.expiresAt) return null
    return latest.value
  }

  return {
    async get(key: string): Promise<string | null> {
      maybeError('get')
      return readVersion(key, true)
    },
    async put(
      key: string,
      value: string,
      opts?: { expirationTtl?: number; expiration?: number },
    ): Promise<void> {
      maybeError('put')
      let expiresAt: number | null = null
      if (opts?.expirationTtl !== undefined) {
        expiresAt = sim.now + opts.expirationTtl * 1000
      } else if (opts?.expiration !== undefined) {
        expiresAt = opts.expiration * 1000
      }
      const versions = history.get(key) ?? []
      versions.push({
        value,
        writtenAt: sim.now,
        visibleAt: sim.now + propagationDelayMs,
        expiresAt,
      })
      // Keep bounded version history per key
      if (versions.length > 16) versions.shift()
      history.set(key, versions)
    },
    async delete(key: string): Promise<void> {
      maybeError('delete')
      const versions = history.get(key) ?? []
      versions.push({
        value: null,
        writtenAt: sim.now,
        visibleAt: sim.now + propagationDelayMs,
        expiresAt: null,
      })
      if (versions.length > 16) versions.shift()
      history.set(key, versions)
    },
    async list(opts?: { prefix?: string }): Promise<{ keys: { name: string }[] }> {
      maybeError('list')
      const prefix = opts?.prefix ?? ''
      const keys: { name: string }[] = []
      for (const key of history.keys()) {
        if (!key.startsWith(prefix)) continue
        const val = readVersion(key, true)
        if (val !== null) keys.push({ name: key })
      }
      keys.sort((a, b) => a.name.localeCompare(b.name))
      return { keys }
    },
    /** Read strongly consistent latest value (for invariants / final checks). */
    inspect(key: string): string | null {
      return readVersion(key, false)
    },
  }
}

// --- Simulated Durable Object ---

export type SimulatedDOStorage = {
  get<T = unknown>(key: string): Promise<T | undefined>
  put<T = unknown>(key: string, value: T): Promise<void>
  delete(key: string): Promise<boolean>
  list<T = unknown>(opts?: { prefix?: string }): Promise<Map<string, T>>
  transaction<R>(closure: (txn: SimulatedDOStorage) => Promise<R>): Promise<R>
  getAlarm(): Promise<number | null>
  setAlarm(when: number | Date): Promise<void>
  deleteAlarm(): Promise<void>
}

export type SimulatedDOContext = {
  readonly id: string
  readonly storage: SimulatedDOStorage
  /** Virtual clock for this DO (includes optional clock skew). */
  now(): number
  /** Block other concurrent invocations until `fn` completes (models DO input gate). */
  blockConcurrencyWhile<R>(fn: () => Promise<R>): Promise<R>
  /** Immediately abort the DO instance, rolling back any active transaction and resetting ephemeral memory. */
  abort(reason?: string): never
}

export type SimulatedDOOptions = {
  /** Alarm retry/duplicate/jitter behavior. */
  alarm?: AlarmOptions
  /** Probability an RPC fails with a clean DurableObjectResetError before executing. Default 0. */
  rpcReset?: number
  /**
   * Probability an RPC commits on the DO, evicts the instance, and then throws an
   * ambiguous DurableObjectResetError to the caller. Default 0.
   */
  rpcAmbiguous?: number
  /** Probability the DO is evicted right before an RPC call. Default 0. */
  evictBeforeCall?: number
  /** Probability the DO is evicted right after an RPC call. Default 0. */
  evictAfterCall?: number
  /** Signed per-DO clock skew in virtual ms. Default 0. */
  clockSkewMs?: number
}

export function simulatedDurableObject<T extends { alarm?(info: AlarmInfo): void | Promise<void> }>(
  sim: Simulation,
  name: string,
  factory: (ctx: SimulatedDOContext) => T,
  options: SimulatedDOOptions = {},
) {
  const rng = sim.rngStream(`do:${name}`)
  const rpcReset = options.rpcReset ?? 0
  const rpcAmbiguous = options.rpcAmbiguous ?? 0
  const evictBeforeCall = options.evictBeforeCall ?? 0
  const evictAfterCall = options.evictAfterCall ?? 0
  const clockSkewMs = options.clockSkewMs ?? 0

  let durable = new Map<string, unknown>()
  let instance: T | undefined
  let evictions = 0
  let boots = 0
  let gateLocked = false
  const gateWaiters: (() => void)[] = []

  const waitGate = () => sim.waitFor((wake) => gateWaiters.push(wake), `${name} input gate`)

  const releaseGate = () => {
    gateLocked = false
    const waiters = gateWaiters.splice(0, gateWaiters.length)
    for (const wake of waiters) wake()
  }

  const alarmSlot = simulatedAlarm(
    sim,
    name,
    async (info) => {
      const obj = await enterInstance()
      if (obj.alarm) await obj.alarm(info)
    },
    options.alarm,
  )

  const makeStorage = (backing: Map<string, unknown>): SimulatedDOStorage => {
    const api: SimulatedDOStorage = {
      async get<V = unknown>(key: string) {
        const val = backing.get(key)
        return val === undefined ? undefined : (structuredClone(val) as V)
      },
      async put<V = unknown>(key: string, value: V) {
        backing.set(key, structuredClone(value))
      },
      async delete(key: string) {
        return backing.delete(key)
      },
      async list<V = unknown>(opts?: { prefix?: string }) {
        const prefix = opts?.prefix ?? ''
        const out = new Map<string, V>()
        for (const [k, v] of backing) {
          if (k.startsWith(prefix)) out.set(k, structuredClone(v) as V)
        }
        return out
      },
      async transaction<R>(closure: (txn: SimulatedDOStorage) => Promise<R>): Promise<R> {
        // Stage only this transaction's own writes over the live backing map, so writes from
        // invocations interleaved while the closure is suspended survive the commit.
        const writes = new Map<string, { value: unknown } | typeof DELETED>()
        const read = (key: string) => {
          const w = writes.get(key)
          if (w === DELETED) return undefined
          return w ? w.value : backing.get(key)
        }
        const txn: SimulatedDOStorage = {
          async get<V = unknown>(key: string) {
            const val = read(key)
            return val === undefined ? undefined : (structuredClone(val) as V)
          },
          async put<V = unknown>(key: string, value: V) {
            writes.set(key, { value: structuredClone(value) })
          },
          async delete(key: string) {
            const existed = read(key) !== undefined
            writes.set(key, DELETED)
            return existed
          },
          async list<V = unknown>(opts?: { prefix?: string }) {
            const prefix = opts?.prefix ?? ''
            const keys = new Set<string>()
            for (const k of backing.keys()) if (k.startsWith(prefix)) keys.add(k)
            for (const k of writes.keys()) if (k.startsWith(prefix)) keys.add(k)
            const out = new Map<string, V>()
            for (const k of keys) {
              const val = read(k)
              if (val !== undefined) out.set(k, structuredClone(val) as V)
            }
            return out
          },
          // Nested transactions flatten into the enclosing one.
          transaction: (inner) => inner(txn),
          getAlarm: () => alarmSlot.getAlarm(),
          setAlarm: (when) => alarmSlot.setAlarm(when),
          deleteAlarm: () => alarmSlot.deleteAlarm(),
        }
        const result = await closure(txn)
        // Commit atomically on success; a throw discards the staged writes.
        for (const [k, w] of writes) {
          if (w === DELETED) backing.delete(k)
          else backing.set(k, w.value)
        }
        return result
      },
      getAlarm: () => alarmSlot.getAlarm(),
      setAlarm: (when) => alarmSlot.setAlarm(when),
      deleteAlarm: () => alarmSlot.deleteAlarm(),
    }
    return api
  }

  const dropInstance = () => {
    instance = undefined
    evictions++
  }

  /** Test-injected eviction. Returns false when minimization disabled this fault. */
  const evict = (reason = 'evicted') => {
    if (!instance) return false
    if (!sim.recordFault('do_evict', `${name} (${reason})`)) return false
    dropInstance()
    return true
  }

  const ctx: SimulatedDOContext = {
    id: name,
    storage: makeStorage(durable),
    now: () => Math.max(0, sim.now + clockSkewMs),
    async blockConcurrencyWhile<R>(fn: () => Promise<R>): Promise<R> {
      // Lock synchronously when the gate is open, so a constructor's blockConcurrencyWhile
      // holds back the call that created the instance.
      while (gateLocked) await waitGate()
      gateLocked = true
      try {
        return await fn()
      } finally {
        releaseGate()
      }
    },
    abort(reason = 'aborted'): never {
      // Application-initiated, so not a minimizable fault: always reset the instance.
      if (instance) dropInstance()
      throw new DurableObjectResetError(name, reason, { retryable: true, ambiguous: false })
    },
  }

  const ensureInstance = (): T => {
    if (!instance) {
      boots++
      instance = factory(ctx)
    }
    return instance
  }

  /** Boot the instance if needed and wait until no blockConcurrencyWhile (including the constructor's) holds the gate. */
  const enterInstance = async (): Promise<T> => {
    for (;;) {
      while (gateLocked) await waitGate()
      const obj = ensureInstance()
      if (!gateLocked && instance === obj) return obj
    }
  }

  return {
    /** Invoke a method on the DO with realistic eviction, input-gate, and reset faults. */
    async call<R>(fn: (obj: T) => R | Promise<R>): Promise<R> {
      if (
        evictBeforeCall > 0 &&
        sim.decideFault('do_evict', evictBeforeCall, `${name} pre-call`, rng)
      ) {
        dropInstance()
      }
      if (rpcReset > 0 && sim.decideFault('do_rpc_reset', rpcReset, `${name} reset`, rng)) {
        throw new DurableObjectResetError(name, 'overloaded / connection reset', {
          retryable: true,
          ambiguous: false,
        })
      }
      // Wait for any active blockConcurrencyWhile gate before entering
      const obj = await enterInstance()
      const result = await fn(obj)
      if (
        rpcAmbiguous > 0 &&
        sim.decideFault('do_rpc_ambiguous', rpcAmbiguous, `${name} ambiguous`, rng)
      ) {
        dropInstance()
        throw new DurableObjectResetError(name, 'connection lost after execution', {
          retryable: true,
          ambiguous: true,
        })
      }
      if (
        evictAfterCall > 0 &&
        sim.decideFault('do_evict', evictAfterCall, `${name} post-call`, rng)
      ) {
        dropInstance()
      }
      return result
    },
    /** Explicitly evict the DO instance (durable storage and pending alarm survive). */
    evict,
    /** Direct access to durable storage for invariants/assertions without waking the instance. */
    storage: ctx.storage,
    get evictions() {
      return evictions
    },
    get boots() {
      return boots
    },
    get active() {
      return instance !== undefined
    },
  }
}

// --- Simulated ctx.waitUntil ---

export type SimulatedWaitUntilOptions = {
  /** Probability a waitUntil task is dropped before completing (isolate killed). Default 0. */
  drop?: number
  /** Maximum virtual delay (ms) before the background task executes. Default 0. */
  maxDelay?: number
}

export function simulatedWaitUntil(sim: Simulation, options: SimulatedWaitUntilOptions = {}) {
  const drop = options.drop ?? 0
  const maxDelay = options.maxDelay ?? 0
  const rng = sim.rngStream('waitUntil')
  let dropped = 0
  let completed = 0

  return {
    waitUntil(label: string, task: () => void | Promise<void>) {
      if (drop > 0 && sim.decideFault('waituntil_drop', drop, `waitUntil:${label}`, rng)) {
        dropped++
        return
      }
      const delay = maxDelay > 0 ? rng.int(maxDelay + 1) : 0
      sim.after(
        delay,
        `waitUntil ${label}`,
        async () => {
          await task()
          completed++
        },
        { kind: 'waitUntil' },
      )
    },
    get dropped() {
      return dropped
    },
    get completed() {
      return completed
    },
  }
}

// --- Simulated Outbound fetch() ---

export type SimulatedFetchOptions = {
  maxDelay?: number
  delayDistribution?: DelayDistribution
  /** Probability of returning a transient HTTP error (429/500/503) without invoking handler. Default 0. */
  errorRate?: number
  /** HTTP status codes used when injecting transient errors. Default [429, 500, 503]. */
  errorStatuses?: readonly number[]
  /** Probability of throwing a network timeout without invoking handler. Default 0. */
  timeoutRate?: number
  /**
   * Probability of an ambiguous write: executes `handler` (mutating remote state),
   * then throws a network timeout/reset error to the caller. Default 0.
   */
  ambiguousRate?: number
}

export function simulatedFetch(
  sim: Simulation,
  handler: (request: Request) => Response | Promise<Response>,
  options: SimulatedFetchOptions = {},
) {
  const rng = sim.rngStream('outbound-fetch')
  const errorRate = options.errorRate ?? 0
  const errorStatuses = options.errorStatuses ?? [429, 500, 503]
  const timeoutRate = options.timeoutRate ?? 0
  const ambiguousRate = options.ambiguousRate ?? 0

  return async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const req = input instanceof Request ? input : new Request(input, init)
    const label = `${req.method} ${req.url}`

    if (timeoutRate > 0 && sim.decideFault('fetch_timeout', timeoutRate, label, rng)) {
      throw new Error(`outbound fetch timeout: ${label}`)
    }
    if (errorRate > 0 && sim.decideFault('fetch_error', errorRate, label, rng)) {
      const status = rng.pick(errorStatuses)
      return new Response(`injected transient error ${status}`, { status })
    }

    const response = await handler(req)

    if (ambiguousRate > 0 && sim.decideFault('fetch_ambiguous', ambiguousRate, label, rng)) {
      throw new Error(`outbound fetch connection lost after remote execution: ${label}`)
    }
    return response
  }
}
