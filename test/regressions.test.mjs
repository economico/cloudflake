import assert from 'node:assert/strict'
import test from 'node:test'
import {
  Prng,
  SimulationFailure,
  campaign,
  simulate,
  simulatedAlarm,
  simulatedDurableObject
} from 'cloudflake'

test('seeds at or above 2^32 do not alias smaller seeds', async () => {
  assert.notEqual(new Prng(2 ** 32 + 7).nextUint32(), new Prng(7).nextUint32())
  assert.equal(new Prng(7).nextUint32(), new Prng(7).nextUint32())

  const seen = []
  await campaign({ seed: 2 ** 32, runs: 2 }, sim => { seen.push(sim.seed) })
  assert.deepEqual(seen, [2 ** 32, 2 ** 32 + 1])
})

test('swarm bounds below a fault floor disable the fault instead of throwing', async () => {
  for (let seed = 0; seed < 200; seed++) {
    await simulate(
      { seed, swarm: { maxKvPropagationDelayMs: 0, maxPartitionDurationMs: 10 } },
      sim => {
        assert.equal(sim.profile.kvPropagationDelayMs, 0)
        assert.ok(sim.profile.partitionDurationMs <= 10)
      }
    )
  }
})

test('drop entries keep their own slot in the failure trace', async () => {
  await assert.rejects(
    simulate({ seed: 1 }, sim => {
      sim.deliver('ack', () => {}, { drop: 1 })
      sim.after(5, 'boom', () => { throw new Error('boom') })
    }),
    e => e instanceof SimulationFailure && e.trace.some(s => s.label === 'ack [dropped]')
  )
})

test('blockConcurrencyWhile in a DO constructor holds back the first call', async () => {
  await simulate({ seed: 1 }, async sim => {
    const obj = simulatedDurableObject(sim, 'counter', ctx => {
      const o = { count: undefined }
      ctx.blockConcurrencyWhile(async () => {
        o.count = (await ctx.storage.get('n')) ?? 0
      })
      return o
    })
    await obj.storage.put('n', 5)
    assert.equal(await obj.call(o => ++o.count), 6)
  })
})

test('a call blocked on the input gate outside a client reports a deadlock instead of hanging', async () => {
  await assert.rejects(
    simulate({ seed: 1 }, sim => {
      const obj = simulatedDurableObject(sim, 'gate', ctx => ({ ctx }))
      sim.after(1, 'lock gate until t=11', async () => {
        obj.call(o => o.ctx.blockConcurrencyWhile(() => new Promise(r => sim.after(10, 'release', r))))
        // Let the background call boot the instance and take the gate before the next event.
        await new Promise(r => setImmediate(r))
      })
      sim.after(2, 'blocked call', () => obj.call(() => 1))
    }),
    e => e instanceof SimulationFailure && /deadlock/.test(e.cause.message)
  )
})

test('DO transactions keep writes from invocations interleaved before commit', async () => {
  await simulate({ seed: 1 }, async sim => {
    const obj = simulatedDurableObject(sim, 'txn', () => ({}))
    let release
    const paused = new Promise(r => { release = r })
    const txn = obj.storage.transaction(async t => {
      await t.get('x')
      await paused
      await t.put('x', 1)
      await t.delete('gone')
    })
    await obj.storage.put('y', 1)
    await obj.storage.put('gone', 1)
    release()
    await txn
    assert.equal(await obj.storage.get('x'), 1)
    assert.equal(await obj.storage.get('y'), 1, 'interleaved put survives the commit')
    assert.equal(await obj.storage.get('gone'), undefined)

    await assert.rejects(obj.storage.transaction(async t => {
      await t.put('x', 2)
      throw new Error('rollback')
    }))
    assert.equal(await obj.storage.get('x'), 1)
  })
})

test('alarm retries do not run after the alarm is deleted or replaced', async () => {
  const deleted = []
  await simulate({ seed: 1 }, async sim => {
    const slot = simulatedAlarm(sim, 'a', async info => {
      deleted.push(info.retryCount)
      await slot.deleteAlarm()
      throw new Error('boom')
    }, { maxRetries: 3 })
    await slot.setAlarm(10)
  })
  assert.deepEqual(deleted, [0])

  const replaced = []
  await simulate({ seed: 1 }, async sim => {
    const slot = simulatedAlarm(sim, 'b', async info => {
      replaced.push([sim.now, info.retryCount])
      if (sim.now === 10) {
        await slot.setAlarm(1000)
        throw new Error('boom')
      }
    }, { maxRetries: 3 })
    await slot.setAlarm(10)
  })
  assert.deepEqual(replaced, [[10, 0], [1000, 0]])
})

test('faults disabled during minimization do not happen', async () => {
  await simulate({ seed: 1, disabledFaults: new Set([0]) }, async sim => {
    const obj = simulatedDurableObject(sim, 'm', () => ({}))
    await obj.call(() => {})
    assert.equal(obj.evict(), false)
    assert.equal(obj.active, true)
    assert.equal(obj.evictions, 0)
  })

  const fired = []
  await assert.rejects(
    simulate({ seed: 1, disabledFaults: new Set([0]) }, async sim => {
      const slot = simulatedAlarm(sim, 'r', async info => {
        fired.push(info.retryCount)
        throw new Error('boom')
      }, { maxRetries: 3 })
      await slot.setAlarm(10)
    }),
    e => e instanceof SimulationFailure && e.cause.message === 'boom'
  )
  assert.deepEqual(fired, [0], 'disabled alarm_retry is not scheduled')
})
