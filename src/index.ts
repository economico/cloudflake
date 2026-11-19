import {
  AssertionTracker,
  CampaignCollector,
  PropertyCoverageFailure,
  type AssertionRecord,
  type CampaignReport,
} from './assertions.js'
import { Prng, type DelayDistribution } from './prng.js'
import { generateFaultProfile, type FaultProfile, type SwarmBounds } from './swarm.js'

export type Action = () => void | Promise<void>
export type Step = Readonly<{
  time: number
  label: string
  id: number
  causedBy?: number
  kind?: string
  actor?: string
}>

type Event = {
  id: number
  time: number
  label: string
  priority: number
  action: Action | undefined
  causedBy?: number
  kind?: string
  actor?: string
}

export type DeliverPolicy = {
  maxDelay?: number
  delayDistribution?: DelayDistribution
  drop?: number
  duplicate?: number
  maxDuplicates?: number
  /**
   * Probability of an ambiguous (gray) failure: the action executes on the callee,
   * and then `onAmbiguous` is called (or an AmbiguousFailureError is thrown if no
   * callback is provided) so callers must handle uncertain commit outcomes.
   */
  ambiguous?: number
  onAmbiguous?: (error: AmbiguousFailureError) => void | Promise<void>
  actor?: string
}

export class AmbiguousFailureError extends Error {
  constructor(readonly label: string) {
    super(`ambiguous failure after executing "${label}": outcome uncertain`)
    this.name = 'AmbiguousFailureError'
  }
}

export type FaultEvent = Readonly<{
  index: number
  time: number
  kind: string
  label: string
}>

export type Options = {
  seed: number
  startTime?: number
  maxSteps?: number
  traceLimit?: number
  onStep?: (step: Step) => void
  /** Constrain the per-seed swarm fault profile (`sim.profile`). */
  swarm?: SwarmBounds
  /** Automatically enter the heal/quiescence phase at this virtual time (ms). */
  healAt?: number
  /** @internal Fault indices to suppress during fault minimization. */
  disabledFaults?: ReadonlySet<number>
}

export type SimulationResult = Readonly<{
  seed: number
  steps: number
  time: number
  assertions: readonly Readonly<AssertionRecord>[]
  faults: Readonly<Record<string, number>>
  firedFaults: readonly FaultEvent[]
}>

function integer(value: number, min: number, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`expected an integer in [${min}, ${max}], received ${value}`)
  }
  return value
}

function before(a: Event, b: Event) {
  return (
    a.time < b.time ||
    (a.time === b.time && (a.priority < b.priority || (a.priority === b.priority && a.id < b.id)))
  )
}

class Calendar {
  private heap: Event[] = []
  get size() {
    return this.heap.length
  }
  push(event: Event) {
    const h = this.heap
    let i = h.length
    h.push(event)
    while (i > 0) {
      const parent = (i - 1) >>> 1
      if (!before(event, h[parent]!)) break
      h[i] = h[parent]!
      i = parent
    }
    h[i] = event
  }
  pop() {
    const h = this.heap
    const first = h[0]!
    const last = h.pop()!
    if (h.length === 0) return first
    let i = 0
    while (2 * i + 1 < h.length) {
      let child = 2 * i + 1
      if (child + 1 < h.length && before(h[child + 1]!, h[child]!)) child++
      if (!before(h[child]!, last)) break
      h[i] = h[child]!
      i = child
    }
    h[i] = last
    return first
  }
}

export class SimulationFailure extends Error {
  constructor(
    readonly seed: number,
    readonly steps: number,
    readonly time: number,
    readonly phase: string,
    readonly trace: readonly Step[],
    cause: unknown,
    readonly firedFaults: readonly FaultEvent[] = [],
  ) {
    super(
      `seed ${seed}, step ${steps}, t=${time}, ${phase}: ${cause instanceof Error ? cause.message : String(cause)}`,
      { cause },
    )
    this.name = 'SimulationFailure'
  }
}

export function formatTrace(trace: readonly Step[]): string {
  return trace
    .map((step) => {
      const kind = step.kind ? `[${step.kind}]`.padEnd(12) : ''
      const actor = step.actor ? `(${step.actor}) ` : ''
      const cause = step.causedBy !== undefined ? ` <- #${step.causedBy}` : ''
      return `#${String(step.id).padEnd(5)} t=${String(step.time).padEnd(8)} ${kind}${actor}${step.label}${cause}`
    })
    .join('\n')
}

export class Simulation {
  readonly seed: number
  readonly rng: Prng
  readonly profile: FaultProfile
  private readonly rootRng: Prng
  private readonly orderRng: Prng
  private readonly networkRng: Prng
  private readonly namedStreams = new Map<string, Prng>()
  private readonly assertions: AssertionTracker
  private readonly disabledFaults?: ReadonlySet<number>
  private readonly firedFaultEvents: FaultEvent[] = []
  private faultDecisionIndex = 0

  private time: number
  private controller?: AbortController
  private calendar = new Calendar()
  private checks: [string, Action][] = []
  private idleHooks: [string, Action][] = []
  private recent: Step[] = []
  /** Total steps written to the `recent` ring (events plus drop/heal/ambiguous trace entries). */
  private traced = 0
  private count = 0
  private finished = false
  private deferredFailure?: { error: unknown; phase: string }
  private id = 0
  private currentEventId?: number
  private capacity: number
  private budget: number
  private observer?: Options['onStep']
  private phase = 'setup'
  private chaos = true
  private healAt?: number
  private pendingClients = 0
  private activeClientPark?: (
    registerWake: (wake: () => void) => void,
    label: string,
  ) => Promise<void>

  constructor(options: Options) {
    this.seed = integer(options.seed, 0, Number.MAX_SAFE_INTEGER)
    this.time = integer(options.startTime ?? 0, 0)
    this.budget = integer(options.maxSteps ?? 100_000, 1)
    this.capacity = integer(options.traceLimit ?? 32, 0)
    this.rootRng = new Prng(this.seed)
    this.rng = this.rootRng.branch('workload')
    this.orderRng = this.rootRng.branch('order')
    this.networkRng = this.rootRng.branch('network')
    this.profile = generateFaultProfile(this.rootRng.branch('swarm'), options.swarm)
    this.assertions = new AssertionTracker(this.seed)
    this.observer = options.onStep
    this.healAt = options.healAt === undefined ? undefined : integer(options.healAt, 0)
    this.disabledFaults = options.disabledFaults
  }

  /** Aborts parked client checkpoints when a run fails. */
  get signal() {
    return (this.controller ??= new AbortController()).signal
  }
  get now() {
    return this.time
  }
  get steps() {
    return this.count
  }
  get chaosEnabled() {
    return this.chaos
  }
  get currentId() {
    return this.currentEventId
  }

  /** Derive or retrieve a named deterministic PRNG stream isolated from other streams. */
  rngStream(name: string): Prng {
    let stream = this.namedStreams.get(name)
    if (!stream) {
      stream = this.rootRng.branch(`stream:${name}`)
      this.namedStreams.set(name, stream)
    }
    return stream
  }

  /** Uniform integer in [0, limit). */
  int(limit: number) {
    return this.rng.int(limit)
  }

  /** Enter the heal/quiescence phase: chaos faults (drops, partitions, resets, ambiguous) pause so retries drain. */
  heal(label = 'heal: chaos disabled') {
    if (!this.chaos) return
    this.chaos = false
    this.recordTrace(label, 'heal')
  }

  /** Re-enable chaos faults after a healed period. */
  resumeChaos(label = 'chaos: resumed') {
    if (this.chaos) return
    this.chaos = true
    this.recordTrace(label, 'chaos')
  }

  /**
   * Make a seeded fault decision that respects chaos state, records fault telemetry,
   * and participates in automatic delta-debugging (`minimizeFaults`).
   */
  decideFault(
    kind: string,
    probability: number,
    label = kind,
    rng: Prng = this.networkRng,
    options: { respectHeal?: boolean } = {},
  ): boolean {
    if (!(probability >= 0 && probability <= 1)) {
      throw new RangeError('probability outside [0,1]')
    }
    const respectHeal = options.respectHeal ?? true
    if (respectHeal && !this.chaos) return false
    // Always consume from PRNG even if suppressed during minimization so downstream draws stay aligned.
    const roll = rng.float()
    if (probability === 0) return false
    const triggered = probability === 1 || roll < probability
    if (!triggered) return false
    const idx = this.faultDecisionIndex++
    if (this.disabledFaults?.has(idx)) return false
    this.assertions.recordFault(kind)
    this.firedFaultEvents.push(Object.freeze({ index: idx, time: this.time, kind, label }))
    return true
  }

  /** Record a fault injected directly by user code or a binding proxy. */
  recordFault(kind: string, label = kind) {
    const idx = this.faultDecisionIndex++
    if (this.disabledFaults?.has(idx)) return false
    this.assertions.recordFault(kind)
    this.firedFaultEvents.push(Object.freeze({ index: idx, time: this.time, kind, label }))
    return true
  }

  // --- Antithesis-style assertion taxonomy ---

  /** Safety property: must be true every time reached, and reached at least once in a campaign. */
  always(name: string, condition: boolean, details?: Record<string, unknown>) {
    if (!this.assertions.record('always', name, condition, details)) {
      this.phase = 'always ' + name
      throw new Error(`always("${name}") violated${details ? `: ${JSON.stringify(details)}` : ''}`)
    }
  }

  /** Safety property: must be true whenever reached; allowed to be unreachable on a given run. */
  alwaysOrUnreachable(name: string, condition: boolean, details?: Record<string, unknown>) {
    if (!this.assertions.record('alwaysOrUnreachable', name, condition, details)) {
      this.phase = 'alwaysOrUnreachable ' + name
      throw new Error(
        `alwaysOrUnreachable("${name}") violated${details ? `: ${JSON.stringify(details)}` : ''}`,
      )
    }
  }

  /**
   * Liveness/coverage property: must evaluate to `true` at least once across a campaign.
   * Fails the campaign if no seed ever satisfies it.
   */
  sometimes(name: string, condition: boolean, details?: Record<string, unknown>) {
    this.assertions.record('sometimes', name, condition, details)
  }

  /** Marks an important code path or recovery state as reached. */
  reachable(name: string, details?: Record<string, unknown>) {
    this.assertions.record('reachable', name, true, details)
  }

  /** Fails immediately if this code path is ever executed. */
  unreachable(name: string, details?: Record<string, unknown>): never {
    this.assertions.record('unreachable', name, false, details)
    this.phase = 'unreachable ' + name
    throw new Error(
      `unreachable("${name}") was reached${details ? `: ${JSON.stringify(details)}` : ''}`,
    )
  }

  /** Pre-register a `reachable` property so 0 hits across a campaign fails coverage. */
  expectReachable(name: string) {
    this.assertions.expect(name, 'reachable')
  }

  /** Pre-register a `sometimes` property so 0 true hits across a campaign fails coverage. */
  expectSometimes(name: string) {
    this.assertions.expect(name, 'sometimes')
  }

  /** Pre-register an `always` property so 0 hits across a campaign fails coverage. */
  expectAlways(name: string) {
    this.assertions.expect(name, 'always')
  }

  /** Record a branch/state coverage edge for coverage-guided multiverse exploration. */
  cover(edge: string) {
    this.assertions.recordCoverage(edge)
  }

  /** Schedule a turn in virtual milliseconds and return a cancel function. */
  after(delay: number, label: string, action: Action, meta?: { kind?: string; actor?: string }) {
    const time = integer(this.time + integer(delay, 0), this.time)
    const event: Event = {
      time,
      label,
      id: this.id++,
      priority: this.orderRng.nextUint32(),
      action,
      causedBy: this.currentEventId,
      kind: meta?.kind,
      actor: meta?.actor,
    }
    this.calendar.push(event)
    return () => {
      event.action = undefined
    }
  }

  /**
   * Schedule a delivery with seeded latency, drops, duplicates (1..N), and optional
   * ambiguous (gray) failures. Each delivery executes the callback afresh.
   */
  deliver(label: string, action: Action, policy: DeliverPolicy = {}) {
    const delay = integer(policy.maxDelay ?? 0, 0, 0xffffffff)
    const drop = policy.drop ?? 0
    const duplicate = policy.duplicate ?? 0
    const ambiguous = policy.ambiguous ?? 0
    const maxDuplicates = integer(policy.maxDuplicates ?? 1, 1, 16)
    for (const p of [drop, duplicate, ambiguous]) {
      if (!(p >= 0 && p <= 1)) throw new RangeError('probability outside [0,1]')
    }

    if (this.decideFault('drop', drop, label, this.networkRng)) {
      this.recordTrace(label + ' [dropped]', 'drop', policy.actor)
      return
    }

    const sampleLatency = () => {
      if (policy.delayDistribution) return this.networkRng.delay(policy.delayDistribution)
      if (delay === 0) return 0
      return Math.floor(this.networkRng.float() * (delay + 1))
    }

    const wrapAction =
      (deliveryLabel: string): Action =>
      async () => {
        const res = action()
        if (res) await res
        if (
          ambiguous > 0 &&
          this.decideFault('ambiguous', ambiguous, deliveryLabel, this.networkRng)
        ) {
          const err = new AmbiguousFailureError(deliveryLabel)
          this.recordTrace(deliveryLabel + ' [ambiguous]', 'ambiguous', policy.actor)
          if (policy.onAmbiguous) {
            await policy.onAmbiguous(err)
          } else {
            throw err
          }
        }
      }

    const meta = { kind: 'deliver', actor: policy.actor }
    this.after(sampleLatency(), label, wrapAction(label), meta)

    let dupCount = 0
    while (
      dupCount < maxDuplicates &&
      this.decideFault('duplicate', duplicate, label, this.networkRng)
    ) {
      dupCount++
      const dupLabel = dupCount === 1 ? label + ' [duplicate]' : `${label} [duplicate #${dupCount}]`
      this.after(sampleLatency(), dupLabel, wrapAction(dupLabel), {
        kind: 'duplicate',
        actor: policy.actor,
      })
    }
  }

  /** Check a property after setup and after every completed event. */
  invariant(name: string, check: Action) {
    this.checks.push([name, check])
  }

  /**
   * Register a check or cleanup action that runs once the event calendar drains to quiescence.
   * Eliminates hardcoded settlement timestamps like `sim.after(60001, ...)`.
   */
  onIdle(label: string, action: Action) {
    this.idleHooks.push([label, action])
  }

  /** @internal Track cooperative clients for deadlock detection. */
  _enterClient() {
    this.pendingClients++
  }
  /** @internal Track cooperative clients for deadlock detection. */
  _exitClient() {
    this.pendingClients--
  }
  /** @internal Register the active client's park callback for cooperative locks/gates. */
  _setActiveClientPark(
    park?: (registerWake: (wake: () => void) => void, label: string) => Promise<void>,
  ) {
    this.activeClientPark = park
  }
  /**
   * Cooperatively wait for an external wake signal (e.g. a DO input gate / mutex).
   * Inside a `client()`, this yields to the scheduler so other clients can advance
   * and release the gate — and detects true deadlocks if the calendar drains while parked.
   */
  waitFor(registerWake: (wake: () => void) => void, label = 'wait'): Promise<void> {
    if (this.activeClientPark) {
      return this.activeClientPark(registerWake, label)
    }
    // Outside a client the waiter cannot yield to the scheduler: the current calendar event
    // stays blocked on it. If no event runs and the wake has not fired after the pending
    // microtasks settle, nothing can release it, so fail with a deadlock instead of hanging.
    return new Promise<void>((resolve, reject) => {
      let woken = false
      registerWake(() => {
        woken = true
        resolve()
      })
      if (woken) return
      let lastSteps = this.count
      let idleTurns = 0
      const watch = () => {
        if (woken || this.finished) return
        if (this.count !== lastSteps) {
          lastSteps = this.count
          idleTurns = 0
        } else if (++idleTurns >= 3) {
          this.phase = 'deadlock detection'
          reject(
            new Error(
              `deadlock: "${label}" was never woken outside a client() and the calendar cannot advance`,
            ),
          )
          return
        }
        setImmediate(watch)
      }
      setImmediate(watch)
    })
  }
  /**
   * @internal Fail the run at the next scheduler checkpoint even if the code that observed
   * the failure swallows it (e.g. a Worker catching an assertion error from the control binding).
   */
  _failDeferred(error: unknown) {
    this.deferredFailure ??= { error, phase: this.phase }
  }

  private throwDeferred() {
    if (!this.deferredFailure) return
    this.phase = this.deferredFailure.phase
    throw this.deferredFailure.error
  }

  /** @internal Access tracker for campaign aggregation. */
  _tracker() {
    return this.assertions
  }

  private recordTrace(label: string, kind?: string, actor?: string) {
    if (!this.capacity && !this.observer) return
    const step: Step = Object.freeze(
      this.currentEventId === undefined && !kind && !actor
        ? { id: this.id++, time: this.time, label }
        : {
            id: this.id++,
            time: this.time,
            label,
            ...(this.currentEventId !== undefined ? { causedBy: this.currentEventId } : {}),
            ...(kind ? { kind } : {}),
            ...(actor ? { actor } : {}),
          },
    )
    if (this.capacity) this.recent[this.traced++ % this.capacity] = step
    this.observer?.(step)
  }

  /** @internal */
  async run(setup: (sim: Simulation) => void | Promise<void>): Promise<SimulationResult> {
    try {
      if (this.healAt !== undefined) {
        this.after(this.healAt, 'heal: scheduled quiescence', () => this.heal(), { kind: 'heal' })
      }
      await setup(this)
      this.throwDeferred()
      const checkInvariants = async () => {
        for (const [name, invariant] of this.checks) {
          this.phase = 'invariant ' + name
          const r = invariant()
          if (r) await r
        }
      }
      if (this.checks.length) await checkInvariants()

      const drainCalendar = async () => {
        while (this.calendar.size) {
          const event = this.calendar.pop()
          if (!event.action) continue
          this.phase = 'event budget'
          if (this.count >= this.budget) throw new Error('maxSteps exceeded')
          this.time = event.time
          this.phase = event.label
          this.currentEventId = event.id
          if (this.capacity || this.observer) {
            let step: Step
            if (
              event.causedBy === undefined &&
              event.kind === undefined &&
              event.actor === undefined
            ) {
              step = Object.freeze({ id: event.id, time: event.time, label: event.label })
            } else {
              const s: {
                id: number
                time: number
                label: string
                causedBy?: number
                kind?: string
                actor?: string
              } = {
                id: event.id,
                time: event.time,
                label: event.label,
              }
              if (event.causedBy !== undefined) s.causedBy = event.causedBy
              if (event.kind !== undefined) s.kind = event.kind
              if (event.actor !== undefined) s.actor = event.actor
              step = Object.freeze(s)
            }
            if (this.capacity) this.recent[this.traced++ % this.capacity] = step
            this.observer?.(step)
          }
          this.count++
          const result = event.action()
          if (result) await result
          this.currentEventId = undefined
          this.throwDeferred()
          if (this.checks.length) await checkInvariants()
        }
      }

      await drainCalendar()

      // Run quiescence (onIdle) hooks in registration order; if any hook schedules new work, drain again.
      for (let i = 0; i < this.idleHooks.length; i++) {
        const [label, hook] = this.idleHooks[i]!
        this.phase = label
        const r = hook()
        if (r) await r
        await drainCalendar()
      }

      if (this.pendingClients > 0) {
        this.phase = 'deadlock detection'
        throw new Error(
          `${this.pendingClients} client(s) remained unfinished after calendar drained`,
        )
      }

      this.throwDeferred()
      this.finished = true
      const snap = this.assertions.snapshot()
      return {
        seed: this.seed,
        steps: this.count,
        time: this.now,
        assertions: snap.assertions,
        faults: snap.faults,
        firedFaults: this.firedFaultEvents,
      }
    } catch (cause) {
      this.finished = true
      this.controller?.abort(cause)
      const pivot = this.traced > this.capacity && this.capacity ? this.traced % this.capacity : 0
      const trace = this.recent.slice(pivot).concat(this.recent.slice(0, pivot))
      throw new SimulationFailure(
        this.seed,
        this.count,
        this.time,
        this.phase,
        trace,
        cause,
        this.firedFaultEvents,
      )
    }
  }
}

export type Scenario = (sim: Simulation) => void | Promise<void>

export function simulate(options: Options, setup: Scenario) {
  return new Simulation(options).run(setup)
}

export type CampaignOptions = Options & {
  /** Number of seeds to execute (optional if `durationMs` is provided). */
  runs?: number
  /** Run seeds until this wall-clock duration (ms) elapses. */
  durationMs?: number
  /** `'stop'` throws on the first failure (default). `'collect'` buckets failures across the whole campaign. */
  onFailure?: 'stop' | 'collect'
  /**
   * Verify Antithesis-style property coverage (`sometimes`, expected `reachable`/`always`)
   * across the campaign. Defaults to `true`.
   */
  requireProperties?: boolean
  /** Names of `reachable` properties that must be hit at least once across the campaign. */
  requireReachable?: readonly string[]
  /** Names of `sometimes` properties that must be true at least once across the campaign. */
  requireSometimes?: readonly string[]
}

export type CampaignResult = {
  readonly runs: number
  readonly steps: number
  readonly report: CampaignReport
}

/**
 * Explore multiple seeded schedules, aggregating Antithesis-style property coverage and
 * fault telemetry across the entire campaign.
 */
export async function campaign(options: CampaignOptions, setup: Scenario): Promise<CampaignResult> {
  if (options.runs === undefined && options.durationMs === undefined) {
    throw new RangeError('campaign requires either runs or durationMs')
  }
  const maxRuns =
    options.runs === undefined ? Number.MAX_SAFE_INTEGER : integer(options.runs, 1, 0x100000000)
  const durationMs = options.durationMs === undefined ? Infinity : integer(options.durationMs, 1)
  integer(options.seed, 0, Number.MAX_SAFE_INTEGER)

  const onFailure = options.onFailure ?? 'stop'
  const requireProperties = options.requireProperties ?? true
  const collector = new CampaignCollector()
  for (const name of options.requireReachable ?? []) collector.expect(name, 'reachable')
  for (const name of options.requireSometimes ?? []) collector.expect(name, 'sometimes')

  const startedAt = performance.now()
  for (let i = 0; i < maxRuns; i++) {
    if (i > 0 && performance.now() - startedAt >= durationMs) break
    const seed = options.seed + i
    const sim = new Simulation({ ...options, seed })
    try {
      const result = await sim.run(setup)
      collector.merge(sim._tracker(), result.steps)
    } catch (err) {
      collector.merge(sim._tracker(), sim.steps)
      if (onFailure === 'stop') throw err
      if (err instanceof SimulationFailure) {
        collector.recordFailure(err.seed, err.phase, err.cause)
      } else {
        collector.recordFailure(seed, 'unknown', err)
      }
    }
  }

  const report = collector.finalize(performance.now() - startedAt)
  if (requireProperties && report.unmetProperties.length > 0 && report.failures.length === 0) {
    throw new PropertyCoverageFailure(report.unmetProperties, report)
  }
  return { runs: report.runs, steps: report.steps, report }
}

/**
 * Delta-debug the injected faults for a failing seed to find the minimal subset of
 * faults that still reproduces the failure.
 */
export async function minimizeFaults(
  options: Options,
  setup: Scenario,
): Promise<{
  seed: number
  originalFaults: readonly FaultEvent[]
  minimalFaults: readonly FaultEvent[]
  failure: SimulationFailure
}> {
  let initialFailure: SimulationFailure | undefined
  try {
    await simulate(options, setup)
  } catch (err) {
    if (err instanceof SimulationFailure) initialFailure = err
    else throw err
  }
  if (!initialFailure) {
    throw new Error(`seed ${options.seed} did not fail; nothing to minimize`)
  }

  const original = initialFailure.firedFaults
  if (original.length <= 1) {
    return {
      seed: options.seed,
      originalFaults: original,
      minimalFaults: original,
      failure: initialFailure,
    }
  }

  let active = original.map((f) => f.index)
  let latestFailure = initialFailure
  const allIndices = new Set(active)

  const stillFails = async (keep: number[]): Promise<SimulationFailure | undefined> => {
    const keepSet = new Set(keep)
    const disabled = new Set<number>()
    for (const idx of allIndices) {
      if (!keepSet.has(idx)) disabled.add(idx)
    }
    try {
      await simulate({ ...options, disabledFaults: disabled }, setup)
      return undefined
    } catch (err) {
      if (err instanceof SimulationFailure && err.phase === initialFailure!.phase) {
        return err
      }
      return undefined
    }
  }

  // Standard ddmin delta-debugging over fault indices
  let n = 2
  while (active.length >= 2) {
    const chunkSize = Math.ceil(active.length / n)
    let reduced = false
    for (let i = 0; i < active.length; i += chunkSize) {
      const candidate = active.slice(0, i).concat(active.slice(i + chunkSize))
      const f = await stillFails(candidate)
      if (f) {
        active = candidate
        latestFailure = f
        n = Math.max(2, n - 1)
        reduced = true
        break
      }
    }
    if (!reduced) {
      if (n >= active.length) break
      n = Math.min(active.length, n * 2)
    }
  }

  const keptSet = new Set(active)
  return {
    seed: options.seed,
    originalFaults: original,
    minimalFaults: original.filter((f) => keptSet.has(f.index)),
    failure: latestFailure,
  }
}

/** Read a replay seed from `CLOUDFLAKE_SEED` when set, falling back to `fallback`. */
export function seedFromEnv(fallback = 0, envVar = 'CLOUDFLAKE_SEED'): number {
  const raw = process.env[envVar]
  if (raw === undefined || raw === '') return fallback
  const parsed = Number(raw)
  return integer(parsed, 0, Number.MAX_SAFE_INTEGER)
}

export { Prng, type DelayDistribution } from './prng.js'
export {
  PropertyCoverageFailure,
  formatCampaignReport,
  type AssertionKind,
  type AssertionRecord,
  type CampaignReport,
  type FailureBucket,
} from './assertions.js'
export { generateFaultProfile, type FaultProfile, type SwarmBounds } from './swarm.js'
export {
  HistoryRecorder,
  checkLinearizability,
  type HistoryEntry,
  type LinearModel,
  type LinearizabilityResult,
  type OperationStatus,
} from './model.js'
export { simulatedQueue, type Delivery, type QueueOptions } from './queue.js'
export { simulatedAlarm, type AlarmInfo, type AlarmOptions } from './alarm.js'
export { client, createCheckpointHub, type Checkpoint, type CheckpointHub } from './client.js'
export {
  simulatedKV,
  simulatedDurableObject,
  simulatedWaitUntil,
  simulatedFetch,
  DurableObjectResetError,
  type SimulatedKVOptions,
  type SimulatedDOOptions,
  type SimulatedDOStorage,
  type SimulatedDOContext,
  type SimulatedWaitUntilOptions,
  type SimulatedFetchOptions,
} from './cloudflare.js'
export {
  exploreMultiverse,
  withV8Coverage,
  type MultiverseOptions,
  type MultiverseResult,
  type V8CoverageOptions,
} from './coverage.js'
export {
  probe,
  isCI,
  shouldRunProbe,
  formatAgentGuidance,
  inferFailureHints,
  ProbeFailure,
  type ProbeOptions,
  type ProbeFinding,
  type ProbeReport,
} from './probe.js'
