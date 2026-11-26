// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

import assert from 'node:assert/strict'
import { simulateWorkers } from 'cloudflake/miniflare'

const script = `
import { DurableObject } from 'cloudflare:workers'
export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.sql.exec('CREATE TABLE IF NOT EXISTS increments (id TEXT PRIMARY KEY)')
  }
  increment(id) {
    this.sql.exec('INSERT OR IGNORE INTO increments VALUES (?)', id)
  }
  count() {
    return this.sql.exec('SELECT count(*) AS n FROM increments').one().n
  }
}
export default {
  async fetch(request, env) {
    const counter = env.COUNTERS.getByName('shared')
    if (request.method === 'POST') await counter.increment(await request.text())
    return Response.json(await counter.count())
  }
}`

const result = await simulateWorkers(
  { seed: Number(process.argv[2] ?? 1) },
  () => ({
    name: 'counter',
    modules: true,
    script,
    compatibilityDate: '2026-07-01',
    unsafeInspectDurableObjects: true,
    durableObjects: { COUNTERS: { className: 'Counter', useSQLite: true } },
  }),
  async (sim, runtime) => {
    const url = 'https://counter.test/'
    const read = async () => {
      const response = await runtime.dispatchFetch(url)
      assert.equal(response.status, 200)
      return response.json()
    }
    assert.equal(await read(), 0)
    for (const id of ['a', 'b', 'c']) {
      sim.deliver(
        'increment ' + id,
        async () => {
          const response = await runtime.dispatchFetch(url, { method: 'POST', body: id })
          assert.equal(response.status, 200)
          await response.json()
        },
        { maxDelay: 5, duplicate: 1 },
      )
    }
    sim.after(sim.int(6), 'evict counter', () =>
      runtime.unsafeEvictDurableObject('counter', 'Counter', { name: 'shared' }),
    )
    sim.after(6, 'each increment counted once', async () => assert.equal(await read(), 3))
  },
)
console.log(result)
