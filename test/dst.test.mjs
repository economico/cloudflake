// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

import assert from 'node:assert/strict'
import { test } from 'vite-plus/test'
import {
  AmbiguousFailureError,
  HistoryRecorder,
  Prng,
  ProbeFailure,
  PropertyCoverageFailure,
  SimulationFailure,
  campaign,
  client,
  exploreMultiverse,
  formatCampaignReport,
  formatTrace,
  minimizeFaults,
  probe,
  seedFromEnv,
  shouldRunProbe,
  simulate,
} from 'cloudflake'

test('Prng produces deterministic streams, isolated branches, and valid combinators', () => {
  const a = new Prng(12345)
  const b = new Prng(12345)
  assert.equal(a.uuid(), b.uuid())
  assert.deepEqual(Array.from(a.bytes(16)), Array.from(b.bytes(16)))
  assert.equal(a.branch('net').int(1_000_000), b.branch('net').int(1_000_000))
  assert.notEqual(a.branch('net').int(1_000_000), a.branch('workload').int(1_000_000))

  const arr = [1, 2, 3, 4, 5, 6]
  a.shuffle(arr)
  assert.deepEqual(
    [...arr].sort((x, y) => x - y),
    [1, 2, 3, 4, 5, 6],
  )

  const picked = a.weighted([
    [0, 'never'],
    [10, 'always'],
  ])
  assert.equal(picked, 'always')

  for (const dist of [
    { kind: 'uniform', min: 5, max: 10 },
    { kind: 'exponential', mean: 20, max: 100 },
    { kind: 'pareto', min: 1, alpha: 1.5, max: 500 },
    { kind: 'bimodal', fastMax: 5, slowMin: 50, slowMax: 100, slowChance: 0.3 },
  ]) {
    const d = a.delay(dist)
    assert.ok(Number.isSafeInteger(d) && d >= 0)
  }
})

test('Antithesis assertions: sometimes passes when hit on at least one seed and fails when never hit', async () => {
  const res = await campaign({ seed: 0, runs: 50 }, (sim) => {
    const hit = sim.int(10) === 7
    sim.sometimes('lucky seven', hit, { seed: sim.seed })
    sim.always('seed non-negative', sim.seed >= 0)
    sim.reachable('setup reached')
  })
  assert.equal(res.runs, 50)
  const summary = formatCampaignReport(res.report)
  assert.match(summary, /\[PASS\] sometimes "lucky seven"/)
  assert.match(summary, /\[PASS\] always "seed non-negative"/)

  await assert.rejects(
    campaign({ seed: 0, runs: 20 }, (sim) => {
      sim.sometimes('impossible state', false)
    }),
    (err) => {
      assert.ok(err instanceof PropertyCoverageFailure)
      assert.equal(err.unmetProperties.length, 1)
      assert.equal(err.unmetProperties[0].name, 'impossible state')
      return true
    },
  )
})

test('Antithesis assertions: always, unreachable, and requireReachable enforce safety and liveness', async () => {
  await assert.rejects(
    simulate({ seed: 1 }, (sim) => {
      sim.always('positive balance', false, { balance: -5 })
    }),
    (err) => err instanceof SimulationFailure && err.phase === 'always positive balance',
  )

  await assert.rejects(
    simulate({ seed: 1 }, (sim) => {
      sim.unreachable('corrupt state')
    }),
    (err) => err instanceof SimulationFailure && err.phase === 'unreachable corrupt state',
  )

  await assert.rejects(
    campaign({ seed: 0, runs: 10, requireReachable: ['recovery path'] }, (sim) => {
      sim.alwaysOrUnreachable('unused branch', true)
    }),
    (err) =>
      err instanceof PropertyCoverageFailure && err.unmetProperties[0].name === 'recovery path',
  )
})

test('Swarm testing varies fault profiles per seed and chaos->heal->onIdle drains cleanly', async () => {
  const profiles = []
  const res = await campaign({ seed: 100, runs: 25, healAt: 100 }, (sim) => {
    profiles.push(sim.profile)
    let delivered = 0
    const sendWithRetry = (attempt = 1) => {
      let succeeded = false
      sim.deliver(
        `msg-${attempt}`,
        () => {
          succeeded = true
          delivered++
        },
        {
          drop: sim.profile.drop,
          maxDelay: 10,
        },
      )
      sim.after(15, `check-${attempt}`, () => {
        if (!succeeded) sendWithRetry(attempt + 1)
      })
    }
    sendWithRetry()
    sim.onIdle('settled after heal', () => {
      sim.always('eventually delivered once healed', delivered >= 1)
      sim.sometimes('saw a drop during chaos', (sim._tracker().snapshot().faults.drop ?? 0) > 0)
    })
  })
  assert.equal(res.runs, 25)
  const distinctDropRates = new Set(profiles.map((p) => p.drop))
  assert.ok(distinctDropRates.size > 5, 'swarm testing should explore varied drop regimes')
})

test('deliver supports multi-duplicates, ambiguous failures, and zero-step drops', async () => {
  let runs = 0
  let ambiguousHits = 0
  const result = await simulate({ seed: 42 }, (sim) => {
    sim.deliver(
      'dropped',
      () => {
        assert.fail('must not run')
      },
      { drop: 1 },
    )
    sim.deliver(
      'multi',
      () => {
        runs++
      },
      {
        duplicate: 1,
        maxDuplicates: 2,
        ambiguous: 1,
        onAmbiguous: (err) => {
          assert.ok(err instanceof AmbiguousFailureError)
          ambiguousHits++
        },
      },
    )
  })
  assert.equal(runs, 3) // 1 original + 2 duplicates
  assert.equal(ambiguousHits, 3)
  assert.equal(result.steps, 3) // drop did not burn a step!
  assert.equal(result.faults.drop, 1)
  assert.equal(result.faults.duplicate, 2)
  assert.equal(result.faults.ambiguous, 3)
})

test('campaign supports onFailure collect mode and failure bucketing', async () => {
  const res = await campaign(
    { seed: 0, runs: 10, onFailure: 'collect', requireProperties: false },
    (sim) => {
      if (sim.seed % 2 === 0)
        sim.after(1, 'even crash', () => {
          throw new Error('boom even')
        })
    },
  )
  assert.equal(res.runs, 10)
  assert.equal(res.report.failures.length, 1)
  assert.equal(res.report.failures[0].count, 5)
  assert.equal(res.report.failures[0].firstSeed, 0)
})

test('minimizeFaults delta-debugs a noisy failure down to the minimal fault set', async () => {
  const scenario = (sim) => {
    let balance = 0
    for (let i = 0; i < 12; i++) {
      // Only item #7 lacks deduplication; all other items deduplicate properly
      const seen = new Set()
      sim.deliver(
        `pay-${i}`,
        () => {
          if (i !== 7 && seen.has(i)) return
          seen.add(i)
          balance += 10
        },
        { duplicate: 1, maxDelay: 5 },
      )
    }
    sim.onIdle('exact sum', () => assert.equal(balance, 120))
  }

  const min = await minimizeFaults({ seed: 3 }, scenario)
  assert.equal(min.originalFaults.length, 12)
  assert.equal(min.minimalFaults.length, 1)
  assert.equal(min.minimalFaults[0].label, 'pay-7')
})

test('HistoryRecorder & checkLinearizability validate concurrent + indeterminate histories and catch violations', () => {
  const counterModel = {
    init: () => 0,
    step: (state, op) => {
      if (op.type === 'inc') return [state + op.delta, state + op.delta]
      return [state, state]
    },
  }

  // Case 1: Indeterminate write DID commit on server; subsequent read sees it -> linearizable!
  let now = 0
  const rec1 = new HistoryRecorder(() => now)
  const w1 = rec1.invoke('c1', { type: 'inc', delta: 5 })
  now = 10
  w1.indeterminate('network reset after commit')
  now = 12
  const r1 = rec1.invoke('c2', { type: 'read' })
  now = 15
  r1.ok(5)
  const res1 = rec1.check(counterModel)
  assert.equal(res1.ok, true)
  assert.equal(res1.linearization.length, 2)

  // Case 2: Indeterminate write did NOT commit; subsequent read sees 0 -> also linearizable!
  now = 0
  const rec2 = new HistoryRecorder(() => now)
  const w2 = rec2.invoke('c1', { type: 'inc', delta: 5 })
  now = 10
  w2.indeterminate('dropped before commit')
  now = 12
  const r2 = rec2.invoke('c2', { type: 'read' })
  now = 15
  r2.ok(0)
  assert.equal(rec2.check(counterModel).ok, true)

  // Case 3: Stale read after committed write -> NOT linearizable!
  now = 0
  const rec3 = new HistoryRecorder(() => now)
  const w3 = rec3.invoke('c1', { type: 'inc', delta: 5 })
  now = 5
  w3.ok(5)
  now = 10
  const r3 = rec3.invoke('c2', { type: 'read' })
  now = 12
  r3.ok(0) // impossible after w3 completed at t=5!
  const res3 = rec3.check(counterModel)
  assert.equal(res3.ok, false)
})

test('formatTrace renders causal event chains and seedFromEnv parses env override', async () => {
  const steps = []
  await simulate({ seed: 7, onStep: (s) => steps.push(s) }, (sim) => {
    sim.after(
      0,
      'root',
      () => {
        sim.after(5, 'child', () => {}, { kind: 'deliver', actor: 'worker-a' })
      },
      { kind: 'client', actor: 'c1' },
    )
  })
  assert.equal(steps[1].causedBy, steps[0].id)
  const rendered = formatTrace(steps)
  assert.match(rendered, /\[client\]/)
  assert.match(rendered, /<- #0/)

  process.env.TEST_CLOUDFLAKE_SEED = '999'
  assert.equal(seedFromEnv(0, 'TEST_CLOUDFLAKE_SEED'), 999)
  delete process.env.TEST_CLOUDFLAKE_SEED
  assert.equal(seedFromEnv(42, 'TEST_CLOUDFLAKE_SEED'), 42)
})

test('waitFor detects deadlocks when parked clients never wake', async () => {
  await assert.rejects(
    simulate({ seed: 1 }, (sim) => {
      client(sim, 'stuck-client', async () => {
        await sim.waitFor(() => {
          // Never invokes wake() -> deadlock
        }, 'unreachable lock')
      })
    }),
    (err) => err instanceof SimulationFailure && err.phase === 'deadlock detection',
  )
})

test('exploreMultiverse discovers deep multi-stage states via coverage-guided snapshotting + V8 coverage', async () => {
  // Reaching stage 3 from stage 0 in one shot is rare, but snapshotting on each newly
  // unlocked stage lets multiverse exploration reach stage 3 reliably!
  const res = await exploreMultiverse({
    seed: 1,
    runs: 40,
    maxDepth: 5,
    maxSnapshots: 8,
    v8Coverage: true,
    createRoot: () => ({ stage: 0 }),
    cloneSnapshot: (s) => ({ stage: s.stage }),
    step: (sim, state) => {
      sim.cover(`enter-stage-${state.stage}`)
      if (state.stage === 0 && sim.rng.chance(0.4)) state.stage = 1
      else if (state.stage === 1 && sim.rng.chance(0.4)) state.stage = 2
      else if (state.stage === 2 && sim.rng.chance(0.4)) state.stage = 3
      sim.cover(`exit-stage-${state.stage}`)
      sim.sometimes('reached deep stage 3', state.stage === 3)
    },
  })

  assert.ok(res.snapshotsCreated >= 2, 'should promote novel states into snapshot corpus')
  assert.ok(res.maxDepthReached >= 3, 'should branch deeper than depth 1')
  assert.ok(res.uniqueEdges >= 6, 'should collect both explicit and V8 branch edges')
})

test('probe auto-skips in CI by default and emits minimized agent guidance on failure', async () => {
  assert.equal(shouldRunProbe({ env: { CI: 'true' } }).run, false)
  assert.equal(shouldRunProbe({ env: { CI: 'true', CLOUDFLAKE_PROBE: '1' } }).run, true)
  assert.equal(shouldRunProbe({ env: {} }).run, true)

  const report = await probe(
    {
      name: 'duplicate-payment-probe',
      seed: 0,
      runs: 15,
      allowInCI: true,
      throwOnFailure: false,
      printGuidance: 'never',
      replayCommand: 'CLOUDFLAKE_SEED={seed} node probe.mjs',
    },
    (sim) => {
      let balance = 0
      for (let i = 0; i < 6; i++) {
        const seen = new Set()
        sim.deliver(
          `tx-${i}`,
          () => {
            if (i !== 3 && seen.has(i)) return
            seen.add(i)
            balance += 10
          },
          { duplicate: 1 },
        )
      }
      sim.onIdle('exact balance', () => assert.equal(balance, 60))
    },
  )

  assert.equal(report.status, 'fail_safety')
  assert.equal(report.findings.length, 1)
  assert.equal(report.findings[0].minimalFaults.length, 1)
  assert.equal(report.findings[0].minimalFaults[0].label, 'tx-3')
  assert.match(report.guidance, /CLOUDFLAKE_SEED=0 node probe\.mjs/)
  assert.match(report.guidance, /idempotency key/i)
  assert.match(report.guidance, /minimized from 6 injected faults to 1/)

  await assert.rejects(
    probe(
      {
        name: 'throws-by-default',
        seed: 0,
        runs: 2,
        allowInCI: true,
        printGuidance: 'never',
      },
      (sim) => {
        sim.always('broken', false)
      },
    ),
    ProbeFailure,
  )
})
