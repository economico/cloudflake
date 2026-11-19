// TigerBeetle VOPR-style Swarm Testing: randomize fault regimes per seed so
// campaigns explore extreme corners of the state space instead of flat averages.
import type { DelayDistribution, Prng } from './prng.js'

export type FaultProfile = Readonly<{
  /** Probability a delivery is dropped (when chaos is active). */
  drop: number
  /** Probability a delivery is duplicated. */
  duplicate: number
  /** Maximum extra copies per duplicated delivery (1..3). */
  maxDuplicates: number
  /** Probability of an ambiguous failure (callee commits, caller sees an error). */
  ambiguous: number
  /** Maximum delivery latency in virtual ms. */
  maxDelay: number
  /** Latency distribution drawn for this seed. */
  delayDistribution: DelayDistribution
  /** Probability of a DO eviction at nemesis ticks/checkpoints. */
  eviction: number
  /** Probability a DO RPC experiences a reset/overloaded fault. */
  doReset: number
  /** Probability an alarm fires more than once (at-least-once). */
  alarmDuplicate: number
  /** Maximum extra delay (ms) before a DO alarm fires. */
  alarmMaxDelay: number
  /** Probability a KV read returns a stale cached value. */
  kvStaleRead: number
  /** Virtual ms for KV writes to propagate across PoPs. */
  kvPropagationDelayMs: number
  /** Probability a ctx.waitUntil() background task is dropped before completion. */
  waitUntilDrop: number
  /** Maximum per-actor clock skew in virtual ms (+/-). */
  clockSkewMs: number
  /** Probability of a temporary partition ("clogged" link) window. */
  partitionChance: number
  /** Duration of a partition window in virtual ms. */
  partitionDurationMs: number
}>

export type SwarmBounds = Partial<{
  maxDrop: number
  maxDuplicate: number
  maxAmbiguous: number
  maxDelay: number
  maxEviction: number
  maxDoReset: number
  maxAlarmDuplicate: number
  maxAlarmDelay: number
  maxKvStaleRead: number
  maxKvPropagationDelayMs: number
  maxWaitUntilDrop: number
  maxClockSkewMs: number
  maxPartitionChance: number
  maxPartitionDurationMs: number
}>

function drawRate(rng: Prng, max: number, activeChance = 0.7): number {
  if (max <= 0 || !rng.chance(activeChance)) return 0
  // Bias toward both low and high extremes (beta-like via squared/linear mix)
  const u = rng.chance(0.5) ? rng.float() : rng.float() * rng.float()
  return Math.min(max, u * max)
}

/** Draw from [min, max]; a bound below the floor (e.g. 0 to disable a fault) caps the value instead of throwing. */
function boundedRange(rng: Prng, min: number, max: number): number {
  return max < min ? Math.max(0, max) : rng.range(min, max)
}

export function generateFaultProfile(rng: Prng, bounds: SwarmBounds = {}): FaultProfile {
  const maxDelayCap = bounds.maxDelay ?? 200
  const maxDelay = maxDelayCap > 0 && rng.chance(0.85) ? rng.range(1, maxDelayCap) : 0

  const delayDistribution: DelayDistribution = maxDelay === 0
    ? { kind: 'uniform', max: 0 }
    : rng.weighted<DelayDistribution>([
        [3, { kind: 'uniform', max: maxDelay }],
        [3, { kind: 'pareto', min: 1, alpha: 1.4, max: maxDelay }],
        [2, {
          kind: 'bimodal',
          fastMax: Math.max(1, Math.floor(maxDelay * 0.1)),
          slowMin: Math.max(1, Math.floor(maxDelay * 0.5)),
          slowMax: maxDelay,
          slowChance: 0.1 + rng.float() * 0.25
        }],
        [2, { kind: 'exponential', mean: Math.max(1, Math.floor(maxDelay * 0.25)), max: maxDelay }]
      ])

  const duplicate = drawRate(rng, bounds.maxDuplicate ?? 0.45, 0.75)

  return Object.freeze({
    drop: drawRate(rng, bounds.maxDrop ?? 0.3, 0.65),
    duplicate,
    maxDuplicates: duplicate > 0 ? rng.range(1, 3) : 1,
    ambiguous: drawRate(rng, bounds.maxAmbiguous ?? 0.25, 0.65),
    maxDelay,
    delayDistribution,
    eviction: drawRate(rng, bounds.maxEviction ?? 0.4, 0.75),
    doReset: drawRate(rng, bounds.maxDoReset ?? 0.25, 0.6),
    alarmDuplicate: drawRate(rng, bounds.maxAlarmDuplicate ?? 0.3, 0.6),
    alarmMaxDelay: rng.chance(0.6) ? boundedRange(rng, 0, bounds.maxAlarmDelay ?? 500) : 0,
    kvStaleRead: drawRate(rng, bounds.maxKvStaleRead ?? 0.5, 0.7),
    kvPropagationDelayMs: rng.chance(0.7) ? boundedRange(rng, 10, bounds.maxKvPropagationDelayMs ?? 60_000) : 0,
    waitUntilDrop: drawRate(rng, bounds.maxWaitUntilDrop ?? 0.5, 0.65),
    clockSkewMs: rng.chance(0.6) ? boundedRange(rng, 0, bounds.maxClockSkewMs ?? 500) : 0,
    partitionChance: drawRate(rng, bounds.maxPartitionChance ?? 0.2, 0.5),
    partitionDurationMs: rng.chance(0.5) ? boundedRange(rng, 50, bounds.maxPartitionDurationMs ?? 5_000) : 0
  })
}
