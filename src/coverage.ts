// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

// Coverage-guided multiverse exploration (Antithesis-style snapshot + branching)
// plus optional V8 precise branch coverage via Node's built-in inspector.
import { Session } from 'node:inspector/promises'
import { CampaignCollector, PropertyCoverageFailure, type CampaignReport } from './assertions.js'
import { Simulation, SimulationFailure, type Options } from './index.js'
import { Prng } from './prng.js'

export type V8CoverageOptions = {
  /** Only include scripts whose URL matches this filter (defaults to excluding node: & node_modules). */
  urlFilter?: (url: string) => boolean
}

const defaultUrlFilter = (url: string) =>
  url.length > 0 && !url.startsWith('node:') && !url.includes('/node_modules/')

/**
 * Capture V8 precise branch/function coverage edges executed during `fn()` using
 * Node's built-in inspector. Works cleanly for in-process model simulations.
 */
export async function withV8Coverage<T>(
  fn: () => Promise<T>,
  options: V8CoverageOptions = {},
): Promise<{ result: T; edges: string[] }> {
  const filter = options.urlFilter ?? defaultUrlFilter
  const session = new Session()
  session.connect()
  try {
    await session.post('Profiler.enable')
    await session.post('Profiler.startPreciseCoverage', { callCount: true, detailed: true })
    const result = await fn()
    const { result: scripts } = await session.post('Profiler.takePreciseCoverage')
    await session.post('Profiler.stopPreciseCoverage')
    await session.post('Profiler.disable')

    const edges: string[] = []
    for (const script of scripts) {
      if (!filter(script.url)) continue
      for (const func of script.functions) {
        for (const range of func.ranges) {
          if (range.count > 0) {
            edges.push(`v8:${script.url}:${range.startOffset}-${range.endOffset}`)
          }
        }
      }
    }
    return { result, edges }
  } finally {
    session.disconnect()
  }
}

export type MultiverseOptions<Snapshot> = Omit<Options, 'seed'> & {
  /** Base seed for deterministic exploration. */
  seed: number
  /** Total exploration episodes (branches) to run. */
  runs?: number
  /** Wall-clock exploration budget in ms. */
  durationMs?: number
  /** Maximum number of interesting snapshots retained in the corpus. Default 16. */
  maxSnapshots?: number
  /** Maximum branching depth from root. Default 6. */
  maxDepth?: number
  /** Collect V8 precise coverage in-process for each episode. Default false. */
  v8Coverage?: boolean
  v8UrlFilter?: (url: string) => boolean
  /** `'stop'` throws on first safety failure; `'collect'` gathers all distinct failures. Default `'stop'`. */
  onFailure?: 'stop' | 'collect'
  /** Enforce Antithesis property coverage (`sometimes`, `reachable`). Default true. */
  requireProperties?: boolean
  requireReachable?: readonly string[]
  requireSometimes?: readonly string[]
  /** Create the initial root snapshot (depth 0). */
  createRoot: () => Snapshot | Promise<Snapshot>
  /** Clone a snapshot into an isolated writable branch for one episode. */
  cloneSnapshot: (snapshot: Snapshot) => Snapshot | Promise<Snapshot>
  /** Clean up a snapshot when evicted or after exploration finishes. */
  disposeSnapshot?: (snapshot: Snapshot) => void | Promise<void>
  /**
   * Run one episode starting from `state` (already cloned for this episode).
   * Return `{ state }` (or mutate `state` in place) so if this episode discovers new
   * coverage edges, `state` can be promoted into the snapshot corpus!
   */
  step: (sim: Simulation, state: Snapshot, depth: number) => void | Promise<void>
}

export type MultiverseResult = {
  readonly runs: number
  readonly steps: number
  readonly uniqueEdges: number
  readonly snapshotsCreated: number
  readonly maxDepthReached: number
  readonly report: CampaignReport
}

type CorpusEntry<Snapshot> = {
  snapshot: Snapshot
  depth: number
  discoveredBySeed: number
  novelty: number
  uses: number
}

/**
 * Antithesis-style coverage-guided multiverse exploration.
 * Runs seeded episodes from a corpus of state snapshots; whenever an episode discovers
 * new coverage edges (via `sim.cover()`, `sim.sometimes()`, `sim.reachable()`, injected
 * faults, or optional V8 branch coverage), its post-episode state is promoted into the
 * corpus so future seeds branch directly from that newly unlocked state.
 */
export async function exploreMultiverse<Snapshot>(
  options: MultiverseOptions<Snapshot>,
): Promise<MultiverseResult> {
  if (options.runs === undefined && options.durationMs === undefined) {
    throw new RangeError('exploreMultiverse requires either runs or durationMs')
  }
  const maxRuns = options.runs ?? Number.MAX_SAFE_INTEGER
  const durationMs = options.durationMs ?? Infinity
  const maxSnapshots = Math.max(1, options.maxSnapshots ?? 16)
  const maxDepth = Math.max(1, options.maxDepth ?? 6)
  const onFailure = options.onFailure ?? 'stop'
  const requireProperties = options.requireProperties ?? true

  const selector = new Prng(options.seed).branch('multiverse-selector')
  const collector = new CampaignCollector()
  for (const name of options.requireReachable ?? []) collector.expect(name, 'reachable')
  for (const name of options.requireSometimes ?? []) collector.expect(name, 'sometimes')

  const globalEdges = new Set<string>()
  const corpus: CorpusEntry<Snapshot>[] = []
  const startedAt = performance.now()
  let snapshotsCreated = 0
  let maxDepthReached = 0

  const root = await options.createRoot()
  corpus.push({ snapshot: root, depth: 0, discoveredBySeed: options.seed, novelty: 1, uses: 0 })

  try {
    for (let i = 0; i < maxRuns; i++) {
      if (i > 0 && performance.now() - startedAt >= durationMs) break

      // Pick a parent snapshot: bias toward less-used, higher-novelty states (and occasionally root)
      const eligible = corpus.filter((c) => c.depth < maxDepth)
      const parent =
        eligible.length === 0 || (i > 0 && selector.chance(0.2))
          ? corpus[0]!
          : selector.weighted(
              eligible.map((c) => [Math.max(1, c.novelty) / (1 + c.uses * 0.5), c] as const),
            )
      parent.uses++

      const branchSeed = options.seed + i
      const branchState = await options.cloneSnapshot(parent.snapshot)
      const sim = new Simulation({ ...options, seed: branchSeed })
      let promoted = false

      try {
        const execEpisode = () => sim.run((s) => options.step(s, branchState, parent.depth + 1))
        let result
        let extraEdges: string[] = []
        if (options.v8Coverage) {
          const v8 = await withV8Coverage(execEpisode, { urlFilter: options.v8UrlFilter })
          result = v8.result
          extraEdges = v8.edges
        } else {
          result = await execEpisode()
        }

        for (const e of extraEdges) sim.cover(e)
        collector.merge(sim._tracker(), result.steps)

        const episodeEdges = sim._tracker().snapshot().coverageEdges
        let newEdges = 0
        for (const edge of episodeEdges) {
          if (!globalEdges.has(edge)) {
            globalEdges.add(edge)
            newEdges++
          }
        }

        const nextDepth = parent.depth + 1
        if (nextDepth > maxDepthReached) maxDepthReached = nextDepth

        // Promote this branch state to the snapshot corpus if it unlocked new coverage!
        if (newEdges > 0 && nextDepth < maxDepth) {
          promoted = true
          snapshotsCreated++
          corpus.push({
            snapshot: branchState,
            depth: nextDepth,
            discoveredBySeed: branchSeed,
            novelty: newEdges,
            uses: 0,
          })
          // Evict lowest-novelty non-root entry if corpus exceeds maxSnapshots
          if (corpus.length > maxSnapshots) {
            let worstIdx = 1
            for (let j = 2; j < corpus.length; j++) {
              const cur = corpus[j]!
              const worst = corpus[worstIdx]!
              if (cur.novelty / (1 + cur.uses) < worst.novelty / (1 + worst.uses)) {
                worstIdx = j
              }
            }
            const [evicted] = corpus.splice(worstIdx, 1)
            if (evicted) await options.disposeSnapshot?.(evicted.snapshot)
          }
        }
      } catch (err) {
        collector.merge(sim._tracker(), sim.steps)
        if (onFailure === 'stop') throw err
        if (err instanceof SimulationFailure) {
          collector.recordFailure(err.seed, err.phase, err.cause)
        } else {
          collector.recordFailure(branchSeed, 'unknown', err)
        }
      } finally {
        if (!promoted) {
          await options.disposeSnapshot?.(branchState)
        }
      }
    }
  } finally {
    for (const entry of corpus) {
      await options.disposeSnapshot?.(entry.snapshot)
    }
  }

  const report = collector.finalize(performance.now() - startedAt)
  if (requireProperties && report.unmetProperties.length > 0 && report.failures.length === 0) {
    throw new PropertyCoverageFailure(report.unmetProperties, report)
  }

  return {
    runs: report.runs,
    steps: report.steps,
    uniqueEdges: globalEdges.size,
    snapshotsCreated,
    maxDepthReached,
    report,
  }
}
