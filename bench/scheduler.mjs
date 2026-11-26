// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

import assert from 'node:assert/strict'
import { simulate } from 'cloudflake'
const width = 256
const total = 1_000_000
async function measure(seed, events) {
  let dispatched = 0
  let allocated = 0
  const start = performance.now()
  const result = await simulate({ seed, maxSteps: events, traceLimit: 32 }, (sim) => {
    const action = () => {
      dispatched++
      if (allocated < events) add()
    }
    const add = () => {
      allocated++
      sim.after(sim.int(1000), 'delivery', action)
    }
    for (let i = 0; i < Math.min(events, width); i++) add()
  })
  const elapsed = performance.now() - start
  assert.equal(dispatched, events)
  assert.equal(result.steps, events)
  return Math.round((events * 1000) / elapsed)
}
await measure(0, 100_000)
const rates = []
for (let i = 1; i <= 5; i++) rates.push(await measure(i, total))
console.log(
  JSON.stringify({
    node: process.version,
    eventsPerTrial: total,
    pending: width,
    rates,
    medianStepsPerSecond: [...rates].sort((a, b) => a - b)[2],
  }),
)
