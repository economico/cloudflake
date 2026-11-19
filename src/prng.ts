// 128-bit xoshiro128++ PRNG seeded via SplitMix64, with hierarchical named
// stream branching and TigerBeetle-style workload/fault combinators.

const MASK64 = 0xffffffffffffffffn
const MASK32 = 0xffffffffn

function splitMix64(state: bigint): [bigint, bigint] {
  state = (state + 0x9e3779b97f4a7c15n) & MASK64
  let z = state
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK64
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK64
  return [state, (z ^ (z >> 31n)) & MASK64]
}

function hashLabel(seed: bigint, label: string): bigint {
  // FNV-1a 64-bit mixed with seed
  let h = (0xcbf29ce484222325n ^ (seed & MASK64)) & MASK64
  for (let i = 0; i < label.length; i++) {
    h ^= BigInt(label.charCodeAt(i))
    h = (h * 0x100000001b3n) & MASK64
  }
  const [, mixed] = splitMix64(h)
  return mixed
}

function rotl(x: number, k: number): number {
  return ((x << k) | (x >>> (32 - k))) >>> 0
}

export type DelayDistribution =
  | { kind: 'uniform'; min?: number; max: number }
  | { kind: 'exponential'; mean: number; max?: number }
  | { kind: 'pareto'; min?: number; alpha?: number; max: number }
  | { kind: 'bimodal'; fastMax: number; slowMin: number; slowMax: number; slowChance: number }

export class Prng {
  private s0: number
  private s1: number
  private s2: number
  private s3: number
  private readonly rootSeed: bigint

  constructor(seed: number | bigint) {
    const base = typeof seed === 'bigint' ? (seed & MASK64) : BigInt(Number.isSafeInteger(seed) && seed >= 0 ? seed : seed >>> 0)
    this.rootSeed = base
    let st = base
    let out: bigint
    ;[st, out] = splitMix64(st)
    this.s0 = Number(out & MASK32)
    this.s1 = Number((out >> 32n) & MASK32)
    ;[st, out] = splitMix64(st)
    this.s2 = Number(out & MASK32)
    this.s3 = Number((out >> 32n) & MASK32)
    // Ensure non-zero state (vanishingly unlikely, but defensive)
    if ((this.s0 | this.s1 | this.s2 | this.s3) === 0) this.s0 = 0x9e3779b9
  }

  /** Derive an independent deterministic sub-stream from this PRNG's seed and a label. */
  branch(label: string): Prng {
    return new Prng(hashLabel(this.rootSeed, label))
  }

  /** Uniform 32-bit unsigned integer in [0, 2^32). */
  nextUint32(): number {
    const result = (rotl((this.s0 + this.s3) >>> 0, 7) + this.s0) >>> 0
    const t = (this.s1 << 9) >>> 0
    this.s2 = (this.s2 ^ this.s0) >>> 0
    this.s3 = (this.s3 ^ this.s1) >>> 0
    this.s1 = (this.s1 ^ this.s2) >>> 0
    this.s0 = (this.s0 ^ this.s3) >>> 0
    this.s2 = (this.s2 ^ t) >>> 0
    this.s3 = rotl(this.s3, 11)
    return result
  }

  /** Uniform float in [0, 1). */
  float(): number {
    return this.nextUint32() / 0x100000000
  }

  /** Unbiased uniform integer in [0, limit). */
  int(limit: number): number {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 0x100000000) {
      throw new RangeError(`expected an integer in [1, ${0x100000000}], received ${limit}`)
    }
    const cutoff = 0x100000000 - (0x100000000 % limit)
    let sample: number
    do {
      sample = this.nextUint32()
    } while (sample >= cutoff)
    return sample % limit
  }

  /** Unbiased uniform integer in [min, max] (inclusive). */
  range(min: number, max: number): number {
    if (!Number.isSafeInteger(min) || !Number.isSafeInteger(max) || max < min) {
      throw new RangeError(`invalid range [${min}, ${max}]`)
    }
    const span = max - min + 1
    if (span <= 0x100000000) return min + this.int(span)
    // Fallback for very wide safe-integer ranges using 53-bit precision
    const hi = this.nextUint32() & 0x1fffff
    const lo = this.nextUint32()
    const u53 = (hi * 0x100000000 + lo) / 0x20000000000000
    return min + Math.floor(u53 * span)
  }

  /** Returns true with probability p in [0, 1]. */
  chance(probability: number): boolean {
    if (!(probability >= 0 && probability <= 1)) {
      throw new RangeError(`probability outside [0, 1]: ${probability}`)
    }
    if (probability === 0) return false
    if (probability === 1) return true
    return this.float() < probability
  }

  /** Picks a uniform element from a non-empty array. */
  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new RangeError('cannot pick from empty array')
    return items[this.int(items.length)]!
  }

  /** Shuffles an array in place (Fisher-Yates) and returns it. */
  shuffle<T>(items: T[]): T[] {
    for (let i = items.length - 1; i > 0; i--) {
      const j = this.int(i + 1)
      const tmp = items[i]!
      items[i] = items[j]!
      items[j] = tmp
    }
    return items
  }

  /** Picks a value according to positive numeric weights. */
  weighted<T>(entries: readonly (readonly [weight: number, value: T])[]): T {
    if (entries.length === 0) throw new RangeError('weighted table must not be empty')
    let total = 0
    for (const [w] of entries) {
      if (!(w >= 0) || !Number.isFinite(w)) throw new RangeError(`invalid weight ${w}`)
      total += w
    }
    if (total <= 0) throw new RangeError('weighted table total must be > 0')
    let target = this.float() * total
    for (let i = 0; i < entries.length - 1; i++) {
      const [w, value] = entries[i]!
      if (target < w) return value
      target -= w
    }
    return entries[entries.length - 1]![1]
  }

  /** Deterministic random bytes. */
  bytes(length: number): Uint8Array {
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new RangeError(`invalid byte length ${length}`)
    }
    const out = new Uint8Array(length)
    let i = 0
    while (i < length) {
      const word = this.nextUint32()
      for (let b = 0; b < 4 && i < length; b++) {
        out[i++] = (word >>> (b * 8)) & 0xff
      }
    }
    return out
  }

  /** Deterministic UUID v4 string. */
  uuid(): string {
    const b = this.bytes(16)
    b[6] = (b[6]! & 0x0f) | 0x40
    b[8] = (b[8]! & 0x3f) | 0x80
    const hex = Array.from(b, byte => byte.toString(16).padStart(2, '0')).join('')
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
  }

  /** Sample a non-negative integer delay (ms) from a latency distribution. */
  delay(dist: DelayDistribution): number {
    switch (dist.kind) {
      case 'uniform': {
        const min = dist.min ?? 0
        return this.range(min, dist.max)
      }
      case 'exponential': {
        const u = Math.max(1e-12, 1 - this.float())
        const raw = Math.floor(-Math.log(u) * dist.mean)
        return dist.max === undefined ? raw : Math.min(dist.max, raw)
      }
      case 'pareto': {
        const min = Math.max(1, dist.min ?? 1)
        const alpha = dist.alpha ?? 1.5
        const u = Math.max(1e-12, 1 - this.float())
        const raw = Math.floor(min / Math.pow(u, 1 / alpha)) - (dist.min === 0 ? 1 : 0)
        return Math.max(0, Math.min(dist.max, raw))
      }
      case 'bimodal': {
        if (this.chance(dist.slowChance)) {
          return this.range(dist.slowMin, dist.slowMax)
        }
        return this.range(0, dist.fastMax)
      }
    }
  }
}
