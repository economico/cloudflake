export type Action = () => void | Promise<void>
export type Step = Readonly<{ time: number; label: string; id: number }>
type Event = Step & { priority: number; action: Action | undefined }
export type Options = {
  seed: number
  startTime?: number
  maxSteps?: number
  traceLimit?: number
  onStep?: (step: Step) => void
}

function integer(value: number, min: number, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new RangeError(`expected an integer in [${min}, ${max}], received ${value}`)
  return value
}

// Mulberry32. Keep the state in 32 bits even on long runs.
function stream(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) >>> 0
    let x = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    x ^= x + Math.imul(x ^ (x >>> 7), 61 | x)
    return (x ^ (x >>> 14)) >>> 0
  }
}

function before(a: Event, b: Event) {
  return a.time < b.time || (a.time === b.time &&
    (a.priority < b.priority || (a.priority === b.priority && a.id < b.id)))
}

class Calendar {
  private heap: Event[] = []
  get size() { return this.heap.length }
  push(event: Event) {
    const h = this.heap
    let i = h.length
    h.push(event)
    while (i > 0) {
      const parent = Math.floor((i - 1) / 2)
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
  constructor(readonly seed: number, readonly steps: number, readonly time: number,
    readonly phase: string, readonly trace: readonly Step[], cause: unknown) {
    super(`seed ${seed}, step ${steps}, t=${time}, ${phase}: ${cause instanceof Error ? cause.message : String(cause)}`, { cause })
    this.name = 'SimulationFailure'
  }
}

export class Simulation {
  readonly seed: number
  private time: number
  private controller?: AbortController
  private workload: () => number
  private order: () => number
  private network: () => number
  private calendar = new Calendar()
  private checks: [string, Action][] = []
  private recent: Step[] = []
  private count = 0
  private id = 0
  private capacity: number
  private budget: number
  private observer?: Options['onStep']
  private phase = 'setup'

  constructor(options: Options) {
    this.seed = integer(options.seed, 0, 0xffffffff)
    this.time = integer(options.startTime ?? 0, 0)
    this.budget = integer(options.maxSteps ?? 100_000, 1)
    this.capacity = integer(options.traceLimit ?? 32, 0)
    this.workload = stream(this.seed)
    this.order = stream(this.seed ^ 0x879ad3b1)
    this.network = stream(this.seed ^ 0x34df921b)
    this.observer = options.onStep
  }
  /** Aborts parked client checkpoints when a run fails. */
  get signal() { return (this.controller ??= new AbortController()).signal }
  get now() { return this.time }
  get steps() { return this.count }
  /** Uniform integer in [0, limit). */
  int(limit: number) {
    integer(limit, 1, 0x100000000)
    const cutoff = 0x100000000 - (0x100000000 % limit)
    let sample: number
    do { sample = this.workload() } while (sample >= cutoff)
    return sample % limit
  }
  after(delay: number, label: string, action: Action) {
    const time = integer(this.time + integer(delay, 0), this.time)
    const event: Event = { time, label, id: this.id++, priority: this.order(), action }
    this.calendar.push(event)
    return () => { event.action = undefined }
  }
  /** Each delivery executes the callback afresh, including retries of the same input. */
  deliver(label: string, action: Action, policy: { maxDelay?: number; drop?: number; duplicate?: number } = {}) {
    const delay = integer(policy.maxDelay ?? 0, 0, 0xffffffff)
    const drop = policy.drop ?? 0
    const duplicate = policy.duplicate ?? 0
    for (const p of [drop, duplicate]) {
      if (!(p >= 0 && p <= 1)) throw new RangeError('probability outside [0,1]')
    }
    if (this.network() / 0x100000000 < drop) {
      this.after(0, label + ' [dropped]', () => {})
      return
    }
    const latency = () => Math.floor(this.network() / 0x100000000 * (delay + 1))
    this.after(latency(), label, action)
    if (this.network() / 0x100000000 < duplicate)
      this.after(latency(), label + ' [duplicate]', action)
  }
  invariant(name: string, check: Action) { this.checks.push([name, check]) }

  /** @internal */
  async run(setup: (sim: Simulation) => void | Promise<void>) {
    try {
      await setup(this)
      const check = async () => {
        for (const [name, invariant] of this.checks) {
          this.phase = 'invariant ' + name
          await invariant()
        }
      }
      if (this.checks.length) await check()
      while (this.calendar.size) {
        const event = this.calendar.pop()
        if (!event.action) continue
        this.phase = 'event budget'
        if (this.count >= this.budget) throw new Error('maxSteps exceeded')
        this.time = event.time
        this.phase = event.label
        if (this.capacity || this.observer) {
          const step = Object.freeze({ id: event.id, time: event.time, label: event.label })
          if (this.capacity) this.recent[this.count % this.capacity] = step
          this.observer?.(step)
        }
        this.count++
        const result = event.action()
        if (result) await result
        if (this.checks.length) await check()
      }
      return { seed: this.seed, steps: this.count, time: this.now }
    } catch (cause) {
      this.controller?.abort(cause)
      const pivot = this.count > this.capacity && this.capacity ? this.count % this.capacity : 0
      const trace = this.recent.slice(pivot).concat(this.recent.slice(0, pivot))
      throw new SimulationFailure(this.seed, this.count, this.time, this.phase, trace, cause)
    }
  }
}

export type Scenario = (sim: Simulation) => void | Promise<void>
export function simulate(options: Options, setup: Scenario) {
  return new Simulation(options).run(setup)
}

/** Stop on the first failing seed. Setup must construct fresh scenario state each time. */
export async function campaign(options: Options & { runs: number }, setup: Scenario) {
  integer(options.runs, 1, 0x100000000)
  integer(options.seed, 0, 0xffffffff)
  let steps = 0
  for (let i = 0; i < options.runs; i++) {
    const result = await simulate({ ...options, seed: (options.seed + i) >>> 0 }, setup)
    steps += result.steps
  }
  return { runs: options.runs, steps }
}

export { simulatedQueue, type Delivery } from './queue.js'
export { simulatedAlarm } from './alarm.js'
export { client, type Checkpoint } from './client.js'
