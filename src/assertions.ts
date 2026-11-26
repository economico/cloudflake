// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

// Antithesis-style assertion taxonomy and campaign-level property/fault telemetry.

export type AssertionKind =
  | 'always'
  | 'alwaysOrUnreachable'
  | 'sometimes'
  | 'reachable'
  | 'unreachable'

export type AssertionRecord = {
  readonly name: string
  readonly kind: AssertionKind
  hits: number
  passed: number
  failed: number
  firstPassSeed?: number
  firstFailSeed?: number
  lastDetails?: Record<string, unknown>
}

export type FailureBucket = {
  readonly phase: string
  readonly message: string
  readonly count: number
  readonly firstSeed: number
  readonly seeds: number[]
}

export type CampaignReport = {
  readonly runs: number
  readonly steps: number
  readonly durationMs: number
  readonly assertions: readonly Readonly<AssertionRecord>[]
  readonly faults: Readonly<Record<string, number>>
  readonly coverageEdges: readonly string[]
  readonly unmetProperties: readonly Readonly<AssertionRecord>[]
  readonly failures: readonly FailureBucket[]
}

export class PropertyCoverageFailure extends Error {
  constructor(
    readonly unmetProperties: readonly Readonly<AssertionRecord>[],
    readonly report: CampaignReport,
  ) {
    const names = unmetProperties.map((p) => `${p.kind}("${p.name}")`).join(', ')
    super(`campaign property coverage failed: unmet ${names}`)
    this.name = 'PropertyCoverageFailure'
  }
}

export class AssertionTracker {
  private readonly records = new Map<string, AssertionRecord>()
  private readonly faults = new Map<string, number>()
  private readonly coverage = new Set<string>()

  constructor(private readonly seed: number) {}

  private ensure(name: string, kind: AssertionKind): AssertionRecord {
    let rec = this.records.get(name)
    if (!rec) {
      rec = { name, kind, hits: 0, passed: 0, failed: 0 }
      this.records.set(name, rec)
    } else if (rec.kind !== kind) {
      throw new Error(`assertion "${name}" registered as both ${rec.kind} and ${kind}`)
    }
    return rec
  }

  /** Declare an expectation upfront so even 0 hits across a campaign will fail coverage. */
  expect(name: string, kind: 'always' | 'sometimes' | 'reachable'): void {
    this.ensure(name, kind)
  }

  /** Returns false if the assertion is a safety violation that must immediately fail the run. */
  record(
    kind: AssertionKind,
    name: string,
    condition: boolean,
    details?: Record<string, unknown>,
  ): boolean {
    const rec = this.ensure(name, kind)
    rec.hits++
    if (details !== undefined) rec.lastDetails = details

    if (kind === 'unreachable') {
      rec.failed++
      rec.firstFailSeed ??= this.seed
      return false
    }
    if (kind === 'reachable') {
      rec.passed++
      rec.firstPassSeed ??= this.seed
      return true
    }
    if (condition) {
      rec.passed++
      rec.firstPassSeed ??= this.seed
      return true
    }
    rec.failed++
    rec.firstFailSeed ??= this.seed
    // Safety assertions fail immediately; `sometimes` is aggregated across the campaign.
    return kind === 'sometimes'
  }

  recordFault(kind: string, count = 1): void {
    this.faults.set(kind, (this.faults.get(kind) ?? 0) + count)
    this.coverage.add(`fault:${kind}`)
  }

  recordCoverage(edge: string): void {
    this.coverage.add(edge)
  }

  snapshot(): {
    assertions: readonly AssertionRecord[]
    faults: Record<string, number>
    coverageEdges: readonly string[]
  } {
    const faultsObj: Record<string, number> = {}
    for (const [k, v] of this.faults) faultsObj[k] = v
    // Include satisfied assertions as coverage signals for multiverse guidance
    const edges = new Set(this.coverage)
    for (const rec of this.records.values()) {
      if (rec.passed > 0) edges.add(`${rec.kind}:${rec.name}:true`)
      if (rec.failed > 0) edges.add(`${rec.kind}:${rec.name}:false`)
    }
    return {
      assertions: Array.from(this.records.values(), (r) => ({ ...r })),
      faults: faultsObj,
      coverageEdges: Array.from(edges).sort(),
    }
  }
}

export class CampaignCollector {
  private readonly records = new Map<string, AssertionRecord>()
  private readonly faults = new Map<string, number>()
  private readonly coverage = new Set<string>()
  private readonly buckets = new Map<string, FailureBucket>()
  runs = 0
  steps = 0

  expect(name: string, kind: 'always' | 'sometimes' | 'reachable'): void {
    if (!this.records.has(name)) {
      this.records.set(name, { name, kind, hits: 0, passed: 0, failed: 0 })
    }
  }

  merge(tracker: AssertionTracker, steps: number): void {
    this.runs++
    this.steps += steps
    const snap = tracker.snapshot()
    for (const rec of snap.assertions) {
      const existing = this.records.get(rec.name)
      if (!existing) {
        this.records.set(rec.name, { ...rec })
      } else {
        existing.hits += rec.hits
        existing.passed += rec.passed
        existing.failed += rec.failed
        existing.firstPassSeed ??= rec.firstPassSeed
        existing.firstFailSeed ??= rec.firstFailSeed
        if (rec.lastDetails !== undefined) existing.lastDetails = rec.lastDetails
      }
    }
    for (const [k, v] of Object.entries(snap.faults)) {
      this.faults.set(k, (this.faults.get(k) ?? 0) + v)
    }
    for (const edge of snap.coverageEdges) {
      this.coverage.add(edge)
    }
  }

  recordFailure(seed: number, phase: string, cause: unknown): void {
    const message = cause instanceof Error ? cause.message : String(cause)
    const key = `${phase}\0${message}`
    const existing = this.buckets.get(key)
    if (!existing) {
      this.buckets.set(key, { phase, message, count: 1, firstSeed: seed, seeds: [seed] })
    } else {
      const updated: FailureBucket = {
        ...existing,
        count: existing.count + 1,
        seeds: existing.seeds.length < 16 ? [...existing.seeds, seed] : existing.seeds,
      }
      this.buckets.set(key, updated)
    }
  }

  finalize(durationMs: number): CampaignReport {
    const assertions = Array.from(this.records.values())
    const unmetProperties = assertions.filter((rec) => {
      if (rec.kind === 'sometimes') return rec.passed === 0
      if (rec.kind === 'reachable') return rec.hits === 0
      if (rec.kind === 'always') return rec.hits === 0 || rec.failed > 0
      if (rec.kind === 'alwaysOrUnreachable' || rec.kind === 'unreachable') return rec.failed > 0
      return false
    })
    const faults: Record<string, number> = {}
    for (const [k, v] of this.faults) faults[k] = v
    const failures = Array.from(this.buckets.values(), (b) => {
      const sorted = [...b.seeds].sort((x, y) => x - y)
      return {
        ...b,
        firstSeed: sorted[0] ?? b.firstSeed,
        seeds: sorted,
      }
    })
    return {
      runs: this.runs,
      steps: this.steps,
      durationMs,
      assertions,
      faults,
      coverageEdges: Array.from(this.coverage).sort(),
      unmetProperties,
      failures,
    }
  }
}

export function formatCampaignReport(report: CampaignReport): string {
  const lines: string[] = []
  const rate =
    report.durationMs > 0 ? Math.round((report.runs * 1000) / report.durationMs) : report.runs
  lines.push(
    `Campaign: ${report.runs} runs, ${report.steps} steps in ${report.durationMs.toFixed(1)}ms (${rate} runs/s)`,
  )
  if (report.assertions.length > 0) {
    lines.push('Assertions:')
    for (const a of report.assertions) {
      const unmet = report.unmetProperties.some((u) => u.name === a.name)
      const status = unmet ? 'FAIL' : 'PASS'
      const extra =
        a.kind === 'sometimes'
          ? `true=${a.passed}/${a.hits}${a.firstPassSeed !== undefined ? ` (first seed ${a.firstPassSeed})` : ''}`
          : a.kind === 'reachable'
            ? `hits=${a.hits}${a.firstPassSeed !== undefined ? ` (first seed ${a.firstPassSeed})` : ''}`
            : `passed=${a.passed}, failed=${a.failed}${a.firstFailSeed !== undefined ? ` (first fail seed ${a.firstFailSeed})` : ''}`
      lines.push(`  [${status}] ${a.kind} "${a.name}" — ${extra}`)
    }
  }
  const faultEntries = Object.entries(report.faults)
  if (faultEntries.length > 0) {
    lines.push('Injected faults:')
    for (const [kind, count] of faultEntries.sort(([a], [b]) => a.localeCompare(b))) {
      lines.push(`  ${kind}: ${count}`)
    }
  }
  if (report.failures.length > 0) {
    lines.push('Failure buckets:')
    for (const b of report.failures) {
      lines.push(`  (${b.count}x, first seed ${b.firstSeed}) ${b.phase}: ${b.message}`)
    }
  }
  return lines.join('\n')
}
