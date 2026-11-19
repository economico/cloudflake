// Local exploratory probing runner (TigerBeetle VOPR / Antithesis style) that
// stays out of CI by default, explores deeply with swarm profiles, minimizes
// failures, and produces structured, actionable guidance for coding agents.
import type { AssertionRecord, CampaignReport } from './assertions.js'
import {
  SimulationFailure,
  campaign,
  formatTrace,
  minimizeFaults,
  simulate,
  type CampaignOptions,
  type FaultEvent,
  type Scenario,
  type Step,
} from './index.js'

export type ProbeFinding = {
  readonly phase: string
  readonly message: string
  readonly occurrences: number
  readonly firstSeed: number
  readonly sampleSeeds: readonly number[]
  readonly replayCommand: string
  readonly originalFaultCount: number
  readonly minimalFaults: readonly FaultEvent[]
  readonly trace: readonly Step[]
  readonly hints: readonly string[]
}

export type ProbeReport = {
  readonly name: string
  readonly status: 'pass' | 'fail_safety' | 'fail_coverage' | 'skipped'
  readonly skippedReason?: string
  readonly report?: CampaignReport
  readonly findings: readonly ProbeFinding[]
  readonly unmetProperties: readonly Readonly<AssertionRecord>[]
  readonly guidance: string
}

export class ProbeFailure extends Error {
  constructor(readonly probeReport: ProbeReport) {
    super(probeReport.guidance)
    this.name = 'ProbeFailure'
  }
}

export function isCI(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(
    (env.CI && env.CI !== '0' && env.CI !== 'false') ||
    env.GITHUB_ACTIONS ||
    env.BUILDKITE ||
    env.GITLAB_CI ||
    env.CIRCLECI ||
    env.JENKINS_URL,
  )
}

export function shouldRunProbe(
  options: { allowInCI?: boolean; requireEnv?: boolean; env?: NodeJS.ProcessEnv } = {},
): { run: boolean; reason?: string } {
  const env = options.env ?? process.env
  if (env.CLOUDFLAKE_PROBE === '0' || env.CLOUDFLAKE_PROBE === 'false') {
    return { run: false, reason: 'disabled via CLOUDFLAKE_PROBE=0' }
  }
  if (env.CLOUDFLAKE_PROBE === '1' || env.CLOUDFLAKE_PROBE === 'true') {
    return { run: true }
  }
  if (options.requireEnv) {
    return { run: false, reason: 'set CLOUDFLAKE_PROBE=1 to run local exploratory probe' }
  }
  if (!options.allowInCI && isCI(env)) {
    return { run: false, reason: 'skipped automatically in CI (set CLOUDFLAKE_PROBE=1 to force)' }
  }
  return { run: true }
}

export function inferFailureHints(
  phase: string,
  message: string,
  faults: readonly FaultEvent[],
): string[] {
  const hints: string[] = []
  const kinds = new Set(faults.map((f) => f.kind))
  const text = `${phase} ${message}`.toLowerCase()

  if (kinds.has('duplicate')) {
    hints.push(
      'Duplicate delivery triggered this failure. Ensure writes/effects are deduplicated by a stable idempotency key (e.g. `INSERT OR IGNORE`, conditional update, or durable seen-set).',
    )
  }
  if (kinds.has('do_evict')) {
    hints.push(
      'Durable Object eviction triggered this failure. Check for state held only in JS instance fields/caches instead of SQLite/durable storage, or missing cold-boot hydration in the constructor.',
    )
  }
  if (kinds.has('ambiguous') || kinds.has('do_rpc_ambiguous') || kinds.has('outbound_ambiguous')) {
    hints.push(
      'An ambiguous (gray) failure occurred: the callee executed/committed, then the response failed. Retrying without idempotency keys or assuming a failed RPC did not commit will corrupt state.',
    )
  }
  if (kinds.has('kv_stale_read')) {
    hints.push(
      'A stale KV read occurred within the propagation window. Cloudflare KV is eventually consistent; move strongly-consistent coordination or read-after-write state to a Durable Object or D1.',
    )
  }
  if (kinds.has('alarm_duplicate') || kinds.has('alarm_retry')) {
    hints.push(
      'A Durable Object alarm retried or fired more than once. Alarms are at-least-once; make `alarm()` idempotent and record completion durably before/alongside side effects.',
    )
  }
  if (kinds.has('waituntil_drop')) {
    hints.push(
      'A `ctx.waitUntil()` task was dropped before completion. Do not rely on `waitUntil` for durable state transitions; persist first or enqueue to a durable Queue/DO.',
    )
  }
  if (kinds.has('drop') || kinds.has('do_rpc_reset') || kinds.has('outbound_timeout')) {
    hints.push(
      'A dropped message or transient reset triggered this failure. Verify retry loops, backoff, and that partial progress is recoverable on retry.',
    )
  }
  if (text.includes('deadlock')) {
    hints.push(
      'A cooperative client remained parked after the calendar drained. Check for cyclic `blockConcurrencyWhile` / lock waits or a `waitFor()` whose wake signal is never triggered.',
    )
  }
  if (text.includes('linearizable')) {
    hints.push(
      'The observed operation history is not linearizable. Check for lost updates across async I/O boundaries (missing `blockConcurrencyWhile` or transaction) or stale reads.',
    )
  }
  if (text.includes('maxsteps')) {
    hints.push(
      'Event budget (`maxSteps`) exceeded. Check for unbounded retry loops, self-rescheduling alarms without termination, or missing backoff/heal termination.',
    )
  }
  if (hints.length === 0) {
    hints.push(
      'No injected faults were required to trigger this failure — inspect the causal trace for ordering/re-entrancy races or logic bugs in the handler.',
    )
  }
  return hints
}

export function formatAgentGuidance(probeReport: Omit<ProbeReport, 'guidance'>): string {
  const lines: string[] = []
  lines.push(`# Cloudflake Exploratory Probe: ${probeReport.name}`)
  lines.push(`Status: **${probeReport.status.toUpperCase()}**`)

  if (probeReport.status === 'skipped') {
    lines.push(`Reason: ${probeReport.skippedReason ?? 'skipped'}`)
    return lines.join('\n')
  }

  const r = probeReport.report
  if (r) {
    const rate = r.durationMs > 0 ? Math.round((r.runs * 1000) / r.durationMs) : r.runs
    lines.push(
      `Explored: ${r.runs} seeds, ${r.steps} steps, ${r.coverageEdges.length} unique coverage edges in ${(r.durationMs / 1000).toFixed(2)}s (${rate} runs/s)`,
    )
  }

  if (probeReport.findings.length > 0) {
    lines.push('')
    lines.push(
      `## Safety Failures (${probeReport.findings.length} distinct bucket${probeReport.findings.length === 1 ? '' : 's'})`,
    )
    probeReport.findings.forEach((f, idx) => {
      lines.push('')
      lines.push(`### Finding ${idx + 1}: \`${f.phase}\``)
      lines.push(`- **Error:** ${f.message}`)
      lines.push(
        `- **Frequency:** ${f.occurrences} failing seed(s) (first seed: \`${f.firstSeed}\`, sample: ${f.sampleSeeds.join(', ')})`,
      )
      lines.push(`- **Replay command:** \`${f.replayCommand}\``)
      if (f.minimalFaults.length > 0) {
        lines.push(
          `- **Minimal fault trigger** (minimized from ${f.originalFaultCount} injected faults to ${f.minimalFaults.length}):`,
        )
        for (const fault of f.minimalFaults) {
          lines.push(`  - \`t=${fault.time}\` \`[${fault.kind}]\` ${fault.label}`)
        }
      } else {
        lines.push(
          `- **Minimal fault trigger:** 0 faults required (pure schedule/ordering failure)`,
        )
      }
      lines.push(`- **Diagnostic guidance:**`)
      for (const hint of f.hints) lines.push(`  - ${hint}`)
      if (f.trace.length > 0) {
        const tail = f.trace.slice(-20)
        lines.push(`- **Causal trace (last ${tail.length} events):**`)
        lines.push('```text')
        lines.push(formatTrace(tail))
        lines.push('```')
      }
    })
  }

  if (probeReport.unmetProperties.length > 0) {
    lines.push('')
    lines.push(`## Unmet Property Coverage (${probeReport.unmetProperties.length})`)
    for (const prop of probeReport.unmetProperties) {
      lines.push(
        `- **\`${prop.kind}("${prop.name}")\`** — hits=${prop.hits}, passed=${prop.passed}, failed=${prop.failed}`,
      )
      lines.push(
        `  - Guidance: The probe never satisfied this state across ${r?.runs ?? 0} seeds. Increase probe duration, widen \`swarm\` fault bounds, or add workload actions that drive the system into this regime.`,
      )
    }
  }

  if (r && Object.keys(r.faults).length > 0) {
    lines.push('')
    lines.push('## Fault Injection Summary')
    for (const [kind, count] of Object.entries(r.faults).sort(([a], [b]) => a.localeCompare(b))) {
      lines.push(`- \`${kind}\`: ${count}`)
    }
  }

  if (probeReport.status === 'pass') {
    lines.push('')
    lines.push(
      'All safety invariants, linearizability checks, and Antithesis reachability/sometimes properties passed.',
    )
  }

  return lines.join('\n')
}

export type ProbeOptions = Omit<CampaignOptions, 'seed' | 'onFailure'> & {
  /** Human-readable name for the probe report. */
  name: string
  /** Base seed (overridden by `CLOUDFLAKE_SEED` if set for single-seed replay). Default 0. */
  seed?: number
  /** Replay command template printed in agent guidance. `{seed}` is replaced with the failing seed. */
  replayCommand?: string
  /** Maximum distinct failure buckets to run `minimizeFaults` on. Default 5. */
  maxMinimize?: number
  /** Allow running in CI without `CLOUDFLAKE_PROBE=1`. Default false. */
  allowInCI?: boolean
  /** Require `CLOUDFLAKE_PROBE=1` even locally (useful inside normal `npm test` suites). Default false. */
  requireEnv?: boolean
  /** Throw `ProbeFailure` when safety or property coverage fails. Default true. */
  throwOnFailure?: boolean
  /** Print formatted guidance to stderr on failure (or stdout if `'always'`). Default `'onFailure'`. */
  printGuidance?: 'never' | 'onFailure' | 'always'
}

/**
 * Run a longer exploratory DST probe locally (auto-skipped in CI by default), collecting
 * distinct failure buckets, minimizing injected faults for each failure, and generating
 * structured guidance for coding agents.
 */
export async function probe(options: ProbeOptions, scenario: Scenario): Promise<ProbeReport> {
  const gate = shouldRunProbe({ allowInCI: options.allowInCI, requireEnv: options.requireEnv })
  if (!gate.run) {
    const skipped: Omit<ProbeReport, 'guidance'> = {
      name: options.name,
      status: 'skipped',
      skippedReason: gate.reason,
      findings: [],
      unmetProperties: [],
    }
    return { ...skipped, guidance: formatAgentGuidance(skipped) }
  }

  const envSeed = process.env.CLOUDFLAKE_SEED
  const envMs = process.env.CLOUDFLAKE_PROBE_MS
  const envRuns = process.env.CLOUDFLAKE_PROBE_RUNS

  const replayTemplate =
    options.replayCommand ?? `CLOUDFLAKE_SEED={seed} CLOUDFLAKE_PROBE=1 node <probe-script>`
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
        : (options.durationMs ?? (runs === undefined ? 5_000 : undefined))

  const res = await campaign(
    {
      ...options,
      seed,
      runs,
      durationMs,
      onFailure: 'collect',
      requireProperties: false,
    },
    scenario,
  )

  const maxMinimize = options.maxMinimize ?? 5
  const findings: ProbeFinding[] = []

  for (let i = 0; i < res.report.failures.length; i++) {
    const bucket = res.report.failures[i]!
    let minimalFaults: readonly FaultEvent[] = []
    let originalFaultCount = 0
    let trace: readonly Step[] = []

    if (i < maxMinimize) {
      try {
        const min = await minimizeFaults(
          {
            ...options,
            seed: bucket.firstSeed,
            traceLimit: Math.max(options.traceLimit ?? 32, 64),
          },
          scenario,
        )
        minimalFaults = min.minimalFaults
        originalFaultCount = min.originalFaults.length
        trace = min.failure.trace
      } catch {
        // Fallback: replay once without minimization to grab trace
        try {
          await simulate({ ...options, seed: bucket.firstSeed, traceLimit: 64 }, scenario)
        } catch (err) {
          if (err instanceof SimulationFailure) {
            minimalFaults = err.firedFaults
            originalFaultCount = err.firedFaults.length
            trace = err.trace
          }
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
      hints: inferFailureHints(bucket.phase, bucket.message, minimalFaults),
    })
  }

  const requireProperties = options.requireProperties ?? true
  const unmetProperties = requireProperties ? res.report.unmetProperties : []
  const status: ProbeReport['status'] =
    findings.length > 0 ? 'fail_safety' : unmetProperties.length > 0 ? 'fail_coverage' : 'pass'

  const base: Omit<ProbeReport, 'guidance'> = {
    name: options.name,
    status,
    report: res.report,
    findings,
    unmetProperties,
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
