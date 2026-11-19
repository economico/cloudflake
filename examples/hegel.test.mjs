import assert from 'node:assert/strict'
import test from 'node:test'
import { testAsync } from '@hegeldev/hegel'
import * as g from '@hegeldev/hegel/generators'
import { simulate, simulatedQueue } from 'cloudflake'
import { simulateWorkers } from 'cloudflake/miniflare'

const seeds = g.integers({ minValue: 0, maxValue: 0xffffffff })

test('queue redelivery preserves the sum of distinct payments', () =>
  testAsync(async tc => {
    const amounts = tc.draw(g.arrays(g.integers({ minValue: 1, maxValue: 1000 }), { minSize: 1, maxSize: 12 }))
    const seed = tc.draw(seeds)
    await simulate({ seed }, async sim => {
      const seen = new Set()
      let balance = 0
      const queue = simulatedQueue(sim, 'payments', ({ messages: [m] }) => {
        if (seen.has(m.id)) return
        seen.add(m.id)
        balance += m.body
      }, { duplicate: 1 })
      for (const amount of amounts) await queue.send(amount)
      sim.after(1, 'settled balance', () =>
        assert.equal(balance, amounts.reduce((a, b) => a + b, 0)))
    })
  }, { testCases: 100 }))

// A named service entrypoint implements the AI binding's run(model, input)
// method in workerd. The scenario supplies the responses for each document.
const provider = `
import { WorkerEntrypoint } from 'cloudflare:workers'
export class ScriptedAI extends WorkerEntrypoint {
  run(model, { prompt }) {
    const outcome = this.env.OUTCOMES[Number(prompt)]
    if (outcome === 'unavailable') throw new Error('temporary provider failure')
    if (outcome === 'malformed') return { response: { label: 17 } }
    return { response: { label: outcome } }
  }
}
export default { fetch() { return new Response() } }`

const classifier = `
export default { async fetch(req, env) {
  const id = new URL(req.url).pathname.slice(1)
  let result
  try { result = await env.AI.run('classifier', { prompt: id }) }
  catch { return new Response(null, { status: 503 }) }
  const label = result?.response?.label
  if (!['invoice', 'receipt'].includes(label)) return new Response(null, { status: 422 })
  await env.DB.prepare('INSERT OR IGNORE INTO labels VALUES (?, ?)').bind(id, label).run()
  return new Response(null, { status: 204 })
} }`

test('only valid model responses reach D1 across duplicate requests', () =>
  testAsync(async tc => {
    const extras = tc.draw(g.arrays(g.sampledFrom(['invoice', 'receipt', 'malformed', 'unavailable']), { maxSize: 5 }))
    const outcomes = ['invoice', 'malformed', 'unavailable', ...extras]
    const seed = tc.draw(seeds)
    await simulateWorkers({ seed }, () => ({ workers: [
      { name: 'classifier', modules: true, script: classifier, compatibilityDate: '2026-07-01',
        d1Databases: ['DB'], serviceBindings: { AI: { name: 'provider', entrypoint: 'ScriptedAI' } } },
      { name: 'provider', modules: true, script: provider, compatibilityDate: '2026-07-01',
        bindings: { OUTCOMES: outcomes } }
    ] }), async (sim, runtime) => {
      const db = await runtime.getD1Database('DB', 'classifier')
      await db.exec('CREATE TABLE labels (id TEXT PRIMARY KEY, label TEXT)')
      outcomes.forEach((outcome, id) => {
        sim.deliver('classify ' + id, async () => {
          const response = await runtime.dispatchFetch('https://classifier/' + id)
          assert.equal(response.status, outcome === 'malformed' ? 422 : outcome === 'unavailable' ? 503 : 204)
        }, { duplicate: 1, maxDelay: 4 })
      })
      sim.after(5, 'persisted classifications', async () => {
        const rows = await db.prepare('SELECT id,label FROM labels ORDER BY id').raw()
        assert.deepEqual(rows, outcomes.flatMap((label, i) =>
          ['invoice', 'receipt'].includes(label) ? [[String(i), label]] : []))
      })
    })
  }, { testCases: 8 }))
