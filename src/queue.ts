// A producer binding for dependency-injected workers. All deliveries and retries
// are ordinary simulator events, so tests never wait for a broker or a timer.
import type { Simulation } from './index.js'

export type Delivery<T> = {
  readonly id: string
  readonly timestamp: Date
  readonly attempts: number
  readonly body: T
  ack(): void
  retry(options?: { delaySeconds?: number }): void
}

type SendOptions = { delaySeconds?: number }
type Consumer<T> = (batch: {
  queue: string
  messages: Delivery<T>[]
  ackAll(): void
  retryAll(options?: SendOptions): void
}) => void | Promise<void>
type QueueOptions<T> = {
  maxRetries?: number
  retryDelaySeconds?: number
  duplicate?: number
  onExhausted?: (message: Delivery<T>) => void | Promise<void>
}

export function simulatedQueue<T>(sim: Simulation, queue: string, consume: Consumer<T>, options: QueueOptions<T> = {}) {
  let sequence = 0
  const limit = options.maxRetries ?? 3
  if (!Number.isSafeInteger(limit) || limit < 0) throw new RangeError('invalid maxRetries')
  const millis = (seconds = 0) => {
    if (!Number.isSafeInteger(seconds) || seconds < 0 || !Number.isSafeInteger(seconds * 1000))
      throw new RangeError('invalid queue delay')
    return seconds * 1000
  }
  const retryDelay = millis(options.retryDelaySeconds)
  const duplicate = options.duplicate ?? 0
  if (!(duplicate >= 0 && duplicate <= 1)) throw new RangeError('invalid duplicate probability')
  function enqueue(body: T, delay: number) {
    const id = queue + '-' + sequence++
    const timestamp = sim.now
    const deliver = async (attempts: number) => {
      let decision: 'ack' | 'retry' | undefined
      let nextDelay = retryDelay
      const message: Delivery<T> = {
        id, timestamp: new Date(timestamp), attempts, body: structuredClone(body),
        ack() { decision ??= 'ack' },
        retry(details) {
          if (decision) return
          nextDelay = details?.delaySeconds === undefined ? retryDelay : millis(details.delaySeconds)
          decision = 'retry'
        }
      }
      try {
        await consume({ queue, messages: [message], ackAll: message.ack, retryAll: message.retry })
      } catch (error) {
        if (!decision && attempts > limit && !options.onExhausted) throw error
        decision ??= 'retry'
      }
      if (decision !== 'retry') return
      if (attempts <= limit) {
        sim.after(nextDelay, id + ' retry ' + (attempts + 1), () => deliver(attempts + 1))
      } else if (options.onExhausted) {
        await options.onExhausted(message)
      } else {
        throw new Error(id + ' exhausted retries')
      }
    }
    sim.after(delay, id + ' ready', () =>
      sim.deliver(id, () => deliver(1), { duplicate }))
  }
  return {
    async send(body: T, options?: SendOptions) {
      enqueue(structuredClone(body), millis(options?.delaySeconds))
    },
    async sendBatch(messages: { body: T; delaySeconds?: number }[], options?: SendOptions) {
      const batch = messages.map(m => ({
        body: structuredClone(m.body), delay: millis(m.delaySeconds ?? options?.delaySeconds)
      }))
      for (const m of batch) enqueue(m.body, m.delay)
    }
  }
}
