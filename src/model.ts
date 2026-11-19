// Reference-model & linearizability checker (TigerBeetle StateChecker / Porcupine style).
// Handles definite ok/fail calls AND indeterminate (ambiguous) outcomes where a
// timed-out or reset request may or may not have linearized on the server.

export type OperationStatus = 'pending' | 'ok' | 'fail' | 'indeterminate'

export type HistoryEntry<Op, Res> = {
  readonly id: number
  readonly actor: string
  readonly op: Op
  readonly invokeTime: number
  completeTime?: number
  status: OperationStatus
  result?: Res
  error?: unknown
}

export type LinearModel<State, Op, Res> = {
  /** Initial sequential state. */
  init: () => State
  /**
   * Apply `op` to `state` and return the next state + expected return value.
   * Must be pure (do not mutate `state` in place).
   */
  step: (state: State, op: Op) => readonly [nextState: State, expected: Res]
  /** Deep equality for comparing actual vs expected results (defaults to JSON/Object.is). */
  equal?: (actual: Res, expected: Res) => boolean
  /** Deterministic state key for memoization (defaults to JSON.stringify). */
  stateKey?: (state: State) => string
}

export type LinearizabilityResult<Op, Res> =
  | { readonly ok: true; readonly linearization: readonly HistoryEntry<Op, Res>[] }
  | {
      readonly ok: false
      readonly reason: string
      readonly history: readonly HistoryEntry<Op, Res>[]
    }

function defaultEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false
  return JSON.stringify(a) === JSON.stringify(b)
}

function defaultStateKey(state: unknown): string {
  return JSON.stringify(state)
}

export class HistoryRecorder<Op, Res> {
  private readonly entries: HistoryEntry<Op, Res>[] = []
  private nextId = 0

  constructor(private readonly clock: () => number) {}

  invoke(actor: string, op: Op) {
    const entry: HistoryEntry<Op, Res> = {
      id: this.nextId++,
      actor,
      op,
      invokeTime: this.clock(),
      status: 'pending',
    }
    this.entries.push(entry)
    return {
      entry,
      ok: (result: Res) => {
        entry.status = 'ok'
        entry.result = result
        entry.completeTime = this.clock()
      },
      fail: (error?: unknown) => {
        entry.status = 'fail'
        entry.error = error
        entry.completeTime = this.clock()
      },
      indeterminate: (error?: unknown) => {
        entry.status = 'indeterminate'
        entry.error = error
        entry.completeTime = this.clock()
      },
    }
  }

  get history(): readonly HistoryEntry<Op, Res>[] {
    return this.entries
  }

  check<State>(
    model: LinearModel<State, Op, Res>,
    options?: { maxVisits?: number },
  ): LinearizabilityResult<Op, Res> {
    return checkLinearizability(this.entries, model, options)
  }
}

/**
 * Exact Wing-Gong linearizability search with BigInt bitmask + state memoization.
 * - `'ok'` entries must linearize in their `[invokeTime, completeTime]` interval and match `result`.
 * - `'fail'` entries are excluded (caller knows the op did not take effect).
 * - `'indeterminate'` and `'pending'` entries may linearize at any time >= `invokeTime`, or not at all.
 */
export function checkLinearizability<State, Op, Res>(
  history: readonly HistoryEntry<Op, Res>[],
  model: LinearModel<State, Op, Res>,
  options: { maxVisits?: number } = {},
): LinearizabilityResult<Op, Res> {
  const equal = model.equal ?? defaultEqual
  const stateKey = model.stateKey ?? defaultStateKey
  const maxVisits = options.maxVisits ?? 200_000

  const relevant = history.filter((e) => e.status !== 'fail')
  const n = relevant.length
  if (n === 0) return { ok: true, linearization: [] }

  // Precompute indices of definite 'ok' entries vs optional ('indeterminate'/'pending')
  const isRequired: boolean[] = relevant.map((e) => e.status === 'ok')
  const requiredMask = relevant.reduce(
    (mask, e, idx) => (e.status === 'ok' ? mask | (1n << BigInt(idx)) : mask),
    0n,
  )

  const visited = new Set<string>()
  const path: HistoryEntry<Op, Res>[] = []
  let visits = 0

  function dfs(usedMask: bigint, state: State): boolean {
    if ((usedMask & requiredMask) === requiredMask) return true
    if (++visits > maxVisits) return false

    const memoKey = `${usedMask.toString(36)}|${stateKey(state)}`
    if (visited.has(memoKey)) return false
    visited.add(memoKey)

    // Wing-Gong pruning: next linearized op must have started on or before
    // the earliest completion time among all remaining required ('ok') ops.
    let minEnd = Infinity
    for (let i = 0; i < n; i++) {
      if (!isRequired[i]) continue
      if ((usedMask & (1n << BigInt(i))) !== 0n) continue
      const ct = relevant[i]!.completeTime ?? Infinity
      if (ct < minEnd) minEnd = ct
    }

    // Try definite 'ok' ops first (prunes faster), then indeterminate/pending ops.
    for (let pass = 0; pass < 2; pass++) {
      const wantRequired = pass === 0
      for (let i = 0; i < n; i++) {
        if (isRequired[i] !== wantRequired) continue
        const bit = 1n << BigInt(i)
        if ((usedMask & bit) !== 0n) continue
        const entry = relevant[i]!
        if (entry.invokeTime > minEnd) continue

        const [nextState, expected] = model.step(state, entry.op)
        if (wantRequired && !equal(entry.result as Res, expected)) continue

        path.push(entry)
        if (dfs(usedMask | bit, nextState)) return true
        path.pop()
      }
    }
    return false
  }

  if (dfs(0n, model.init())) {
    return { ok: true, linearization: path }
  }
  const reason =
    visits > maxVisits
      ? `linearizability search budget exceeded (${maxVisits} states visited)`
      : `history of ${relevant.length} operations is not linearizable`
  return { ok: false, reason, history }
}
