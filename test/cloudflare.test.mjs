import assert from 'node:assert/strict'
import { test } from 'vite-plus/test'
import {
  DurableObjectResetError,
  HistoryRecorder,
  campaign,
  client,
  simulate,
  simulatedAlarm,
  simulatedDurableObject,
  simulatedFetch,
  simulatedKV,
  simulatedQueue,
  simulatedWaitUntil,
} from 'cloudflake'

test('simulatedQueue supports multi-message batches, partial batch retries, and DLQ', async () => {
  const processed = []
  const dlqItems = []

  await simulate({ seed: 11 }, async (sim) => {
    const dlq = simulatedQueue(sim, 'dlq', ({ messages }) => {
      for (const m of messages) dlqItems.push(m.body)
    })

    const q = simulatedQueue(
      sim,
      'orders',
      ({ messages }) => {
        // Explicit ack on first message, then throw on poison message:
        // msg 0 stays acked; remaining messages in batch retry!
        for (const m of messages) {
          if (m.body.kind === 'poison') {
            throw new Error('poison payload')
          }
          if (m.body.kind === 'flaky' && m.attempts === 1) {
            m.retry({ delaySeconds: 10 })
            continue
          }
          processed.push([m.id, m.attempts, m.body.kind])
          m.ack()
        }
      },
      {
        maxBatchSize: 3,
        maxBatchTimeoutMs: 5,
        maxRetries: 1,
        retryDelaySeconds: 5,
        deadLetterQueue: dlq,
      },
    )

    await q.sendBatch([
      { body: { kind: 'ok-1' } },
      { body: { kind: 'flaky' } },
      { body: { kind: 'poison' } },
    ])

    sim.onIdle('settled', () => {
      assert.deepEqual(processed, [
        ['orders-0', 1, 'ok-1'],
        ['orders-1', 2, 'flaky'],
      ])
      assert.deepEqual(dlqItems, [{ kind: 'poison' }])
    })
  })
})

test('simulatedAlarm models Cloudflare DO retries with exponential backoff, alarmInfo, and duplicates', async () => {
  const log = []
  await simulate({ seed: 3 }, async (sim) => {
    const slot = simulatedAlarm(
      sim,
      'billing',
      (info) => {
        log.push({ time: sim.now, retryCount: info.retryCount, isRetry: info.isRetry })
        if (info.retryCount < 2) throw new Error('transient downstream outage')
      },
      { maxRetries: 6, retryBaseDelayMs: 1000 },
    )
    await slot.setAlarm(500)
    sim.onIdle('alarm retried and succeeded', () => {
      // Attempt 0 at t=500 -> retry 1 at +1000 (t=1500) -> retry 2 at +2000 (t=3500)
      assert.deepEqual(log, [
        { time: 500, retryCount: 0, isRetry: false },
        { time: 1500, retryCount: 1, isRetry: true },
        { time: 3500, retryCount: 2, isRetry: true },
      ])
    })
  })

  // At-least-once duplicate alarm firing
  let fires = 0
  await simulate({ seed: 1 }, async (sim) => {
    const slot = simulatedAlarm(
      sim,
      'dup',
      () => {
        fires++
      },
      { duplicate: 1 },
    )
    await slot.setAlarm(10)
    sim.onIdle('fired twice', () => assert.equal(fires, 2))
  })
})

test('simulatedKV models eventual consistency stale reads and virtual-time TTL expiration', async () => {
  await campaign({ seed: 0, runs: 30 }, async (sim) => {
    const kv = simulatedKV(sim, 'FLAGS', { propagationDelayMs: 60_000, staleRead: 0.5 })
    await kv.put('mode', 'v1')
    sim.after(60_000, 'propagate v1', async () => {
      await kv.put('mode', 'v2', { expirationTtl: 120 })
      const observed = await kv.get('mode')
      sim.always('observed valid version', observed === 'v1' || observed === 'v2')
      sim.sometimes('observed stale KV read', observed === 'v1')
      assert.equal(kv.inspect('mode'), 'v2')
    })
    // After TTL (120s) expires at t = 60_000 + 120_000 = 180_000
    sim.after(180_001, 'expired', async () => {
      assert.equal(await kv.get('mode'), null)
    })
  })
})

test('simulatedDurableObject catches broken ephemeral caches, rolls back transactions on abort, and verifies linearizability under ambiguous RPC resets', async () => {
  // 1. Transaction rollback on ctx.abort()
  await simulate({ seed: 1 }, async (sim) => {
    const obj = simulatedDurableObject(sim, 'ledger', (ctx) => ({
      async transfer(shouldAbort) {
        await ctx.storage.transaction(async (txn) => {
          const bal = (await txn.get('bal')) ?? 100
          await txn.put('bal', bal - 40)
          if (shouldAbort) ctx.abort('simulated crash mid-transaction')
        })
      },
    }))

    await assert.rejects(
      obj.call((o) => o.transfer(true)),
      DurableObjectResetError,
    )
    assert.equal(obj.evictions, 1)
    assert.equal(await obj.storage.get('bal'), undefined) // Rolled back!
    await obj.call((o) => o.transfer(false))
    assert.equal(await obj.storage.get('bal'), 60)
  })

  // 2. Idempotent DO counter under ambiguous RPC resets + evictions + linearizability check
  const counterModel = {
    init: () => ({ seen: [], count: 0 }),
    step: (state, op) => {
      if (op.type === 'inc') {
        if (state.seen.includes(op.reqId)) return [state, state.count]
        const nextCount = state.count + 1
        return [
          { seen: [...state.seen, op.reqId].sort((a, b) => a.localeCompare(b)), count: nextCount },
          nextCount,
        ]
      }
      return [state, state.count]
    },
  }

  await campaign({ seed: 0, runs: 60, healAt: 100 }, (sim) => {
    const history = new HistoryRecorder(() => sim.now)
    const obj = simulatedDurableObject(
      sim,
      'counter',
      (ctx) => ({
        async increment(reqId) {
          return ctx.storage.transaction(async (txn) => {
            const seen = (await txn.get(`seen:${reqId}`)) ?? false
            const current = (await txn.get('count')) ?? 0
            if (seen) return current
            const next = current + 1
            await txn.put(`seen:${reqId}`, true)
            await txn.put('count', next)
            return next
          })
        },
        async read() {
          return (await ctx.storage.get('count')) ?? 0
        },
      }),
      {
        rpcReset: sim.profile.doReset,
        rpcAmbiguous: sim.profile.ambiguous,
        evictAfterCall: sim.profile.eviction,
      },
    )

    for (let i = 0; i < 4; i++) {
      const reqId = `req-${i}`
      const attempt = () => {
        sim.after(sim.int(10), `call ${reqId}`, async () => {
          const call = history.invoke(reqId, { type: 'inc', reqId })
          try {
            const val = await obj.call((o) => o.increment(reqId))
            call.ok(val)
          } catch (err) {
            if (err instanceof DurableObjectResetError && err.ambiguous) {
              sim.sometimes('saw ambiguous DO reset', true)
              call.indeterminate(err)
            } else {
              call.fail(err)
            }
            // Retry idempotently after backoff
            sim.after(20, `retry ${reqId}`, attempt)
          }
        })
      }
      attempt()
    }

    sim.onIdle('linearizable and converged', async () => {
      const readCall = history.invoke('verifier', { type: 'read' })
      const finalCount = await obj.call((o) => o.read())
      readCall.ok(finalCount)
      sim.always('all 4 increments applied exactly once', finalCount === 4, { finalCount })
      const check = history.check(counterModel)
      sim.always(
        'history is linearizable',
        check.ok,
        check.ok ? undefined : { reason: check.reason },
      )
    })
  })
})

test('simulatedDurableObject blockConcurrencyWhile prevents re-entrancy race that unguarded code loses', async () => {
  async function runRace(guarded) {
    let finalBalance = 0
    await simulate({ seed: 2 }, (sim) => {
      const obj = simulatedDurableObject(sim, 'wallet', (ctx) => ({
        async deposit(checkpoint) {
          const run = async () => {
            const cur = (await ctx.storage.get('bal')) ?? 0
            await checkpoint(1, 'external fetch inside DO')
            await ctx.storage.put('bal', cur + 1)
          }
          if (guarded) return ctx.blockConcurrencyWhile(run)
          return run()
        },
      }))

      for (const name of ['c1', 'c2']) {
        client(sim, name, async (checkpoint) => {
          await obj.call((o) => o.deposit(checkpoint))
        })
      }
      sim.onIdle('check balance', async () => {
        finalBalance = (await obj.storage.get('bal')) ?? 0
      })
    })
    return finalBalance
  }

  assert.equal(await runRace(false), 1, 'unguarded DO loses update across external await')
  assert.equal(await runRace(true), 2, 'blockConcurrencyWhile serializes concurrent callers')
})

test('simulatedWaitUntil drops tasks under chaos and simulatedFetch injects ambiguous writes', async () => {
  // waitUntil drop
  let ran = 0
  await simulate({ seed: 1 }, (sim) => {
    const wu = simulatedWaitUntil(sim, { drop: 1 })
    wu.waitUntil('audit-log', () => {
      ran++
    })
    sim.onIdle('dropped', () => {
      assert.equal(ran, 0)
      assert.equal(wu.dropped, 1)
    })
  })

  // simulatedFetch ambiguous write: remote state mutated even though caller got an error
  let charges = 0
  await simulate({ seed: 1 }, async (sim) => {
    const fetchApi = simulatedFetch(
      sim,
      async () => {
        charges++
        return new Response('charged', { status: 200 })
      },
      { ambiguousRate: 1 },
    )
    await assert.rejects(
      fetchApi('https://stripe.test/charge', { method: 'POST' }),
      /connection lost/,
    )
    assert.equal(charges, 1, 'remote API executed before connection dropped')
  })
})
