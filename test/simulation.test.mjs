import assert from 'node:assert/strict'
import { test } from 'vite-plus/test'
import { campaign, simulate, simulatedQueue, SimulationFailure } from 'cloudflake'

test('matches replay and explores different event orders across seeds', async () => {
  async function schedule(seed) {
    const trace = []
    await simulate({ seed, onStep: (e) => trace.push(e) }, (sim) => {
      for (let i = 0; i < 100; i++) sim.after(i % 4, String(i), () => {})
    })
    return trace
  }
  const trace = await schedule(0)
  assert.deepEqual(trace, await schedule(0))
  assert.notDeepEqual(trace, await schedule(1))
  assert.deepEqual(
    trace.map((s) => s.time),
    trace.map((s) => s.time).sort((a, b) => a - b),
  )
  assert.equal(new Set(trace.map((s) => s.id)).size, 100)
})

test('finds and replays a lost-update race across explicit read/write boundaries', async () => {
  function race(sim) {
    let balance = 0
    for (let i = 0; i < 2; i++)
      sim.after(0, 'read ' + i, () => {
        const observed = balance
        sim.after(0, 'write ' + i, () => {
          balance = observed + 1
        })
      })
    sim.after(1, 'both increments committed', () => assert.equal(balance, 2))
  }
  const options = { seed: 0, runs: 64, traceLimit: 3 }
  const failure = await campaign(options, race).then(
    () => assert.fail(),
    (e) => e,
  )
  assert.ok(failure instanceof SimulationFailure)
  assert.equal(failure.trace.length, 3)
  assert.equal(failure.phase, 'both increments committed')
  await assert.rejects(simulate({ ...options, seed: failure.seed }, race), (e) => {
    assert.deepEqual(e.trace, failure.trace)
    assert.equal(e.cause.message, failure.cause.message)
    return true
  })
})

test('queue retries use virtual time, stable IDs and isolated payloads', async () => {
  const seen = []
  await simulate({ seed: 4 }, async (sim) => {
    const producer = simulatedQueue(sim, 'effects', ({ messages: [m] }) => {
      seen.push([m.id, m.attempts, m.body.cents, sim.now])
      m.body.cents = 99
      if (m.attempts === 1) m.retry({ delaySeconds: 3600 })
    })
    const payment = { cents: 30 }
    await producer.send(payment, { delaySeconds: 2 })
    payment.cents = 15
  })
  assert.deepEqual(seen, [
    ['effects-0', 1, 30, 2000],
    ['effects-0', 2, 30, 3602000],
  ])
})

test('drop and duplicate are explicit and budget fences runaway timers', async () => {
  let n = 0
  const action = () => {
    n++
  }
  await simulate({ seed: 1 }, (sim) => {
    sim.deliver('ack', action, { drop: 1 })
    sim.deliver('payment', action, { duplicate: 1 })
    sim.invariant('at most two deliveries', () => assert.ok(n <= 2))
  })
  assert.equal(n, 2)
  await assert.rejects(
    simulate({ seed: 2, maxSteps: 5 }, (sim) => {
      const timer = () => sim.after(0, 'recurring', timer)
      timer()
    }),
    (e) => e.steps === 5 && e.phase === 'event budget',
  )
})

test('setup and initial invariant failures carry the seed', async () => {
  await assert.rejects(
    simulate({ seed: 42 }, (sim) => {
      sim.invariant('initial', () => assert.fail('bad state'))
    }),
    (e) => e.seed === 42 && e.steps === 0 && e.phase === 'invariant initial',
  )
})

test('replacing, cancelling and rearming an alarm only advances time for live events', async () => {
  const { simulatedAlarm } = await import('cloudflake')
  const wakes = []
  const result = await simulate({ seed: 5, startTime: 10, maxSteps: 2 }, async (sim) => {
    const slot = simulatedAlarm(sim, 'book', async () => {
      assert.equal(await slot.getAlarm(), null)
      wakes.push(sim.now)
      if (wakes.length === 1) await slot.setAlarm(new Date(20))
    })
    await slot.setAlarm(500)
    await slot.deleteAlarm()
    await slot.setAlarm(1000)
    await slot.setAlarm(0)
  })
  assert.deepEqual(wakes, [10, 20])
  assert.equal(result.time, 20)
})

test('a failed invariant releases a parked client', async () => {
  const { client } = await import('cloudflake')
  let parked = false
  let released = false
  await assert.rejects(
    simulate({ seed: 1 }, (sim) => {
      client(sim, 'client', async (pause) => {
        parked = true
        try {
          await pause(2, 'resume')
        } finally {
          released = true
        }
      })
      sim.invariant('stop after pause', () => assert.equal(parked, false))
    }),
    (e) => e.phase === 'invariant stop after pause',
  )
  assert.ok(released)
})
