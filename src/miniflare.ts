import { simulatedQueue } from './queue.js'
import { Miniflare, type MiniflareOptions } from 'miniflare'
import { simulate, type Simulation, type Options } from './index.js'

/** One local deployment per simulation. The options factory can bind test doubles
 * to the seed's virtual clock and random source. */
export async function simulateWorkers(
  options: Options,
  deployment: (sim: Simulation) => MiniflareOptions,
  scenario: (sim: Simulation, runtime: Miniflare) => void | Promise<void>
) {
  let runtime: Miniflare | undefined
  try {
    return await simulate(options, async sim => {
      runtime = new Miniflare({ cf: false, ...deployment(sim) })
      await runtime.ready
      await scenario(sim, runtime)
    })
  } finally {
    await runtime?.dispose()
  }
}


/** Drive a real queue handler from the virtual calendar and honor its ack/retry
 * response. Each synthetic batch has one message. */
export async function workerQueue<T>(sim: Simulation, runtime: Miniflare, workerName: string,
  queueName: string, options?: Parameters<typeof simulatedQueue<T>>[3]) {
  const worker = await runtime.getWorker(workerName)
  return simulatedQueue<T>(sim, queueName, async ({messages: [message]}) => {
    const { id, timestamp, attempts, body } = message!
    const response = await worker.queue(queueName, [{ id, timestamp, attempts, body }])
    if (response.explicitAcks.includes(id)) return
    const retry = response.retryMessages.find((entry: { msgId: string }) => entry.msgId === id)
    if (retry || response.retryBatch.retry || response.outcome !== 'ok')
      message!.retry({ delaySeconds: retry?.delaySeconds ?? response.retryBatch.delaySeconds })
  }, options)
}
