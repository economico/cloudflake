// Realistic Cloudflare Queues simulator: single- or multi-message batches,
// partial batch ack/retry, delivery jitter/reordering, at-least-once duplicates,
// bounded retries, and optional dead-letter queue routing.
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

export type QueueBatch<T> = {
  queue: string
  messages: Delivery<T>[]
  ackAll(): void
  retryAll(options?: SendOptions): void
}

type Consumer<T> = (batch: QueueBatch<T>) => void | Promise<void>

export type QueueOptions<T> = {
  maxRetries?: number
  retryDelaySeconds?: number
  duplicate?: number
  maxDuplicates?: number
  /** Network/broker delivery jitter in virtual ms (enables out-of-order delivery across sends). */
  maxDelay?: number
  /** Maximum messages per consumer batch (default 1; set >1 to test partial batch retries). */
  maxBatchSize?: number
  /** Wait window in virtual ms to accumulate a batch before flushing (default 0). */
  maxBatchTimeoutMs?: number
  onExhausted?: (message: Delivery<T>) => void | Promise<void>
  deadLetterQueue?: { send(body: T): Promise<void> }
}

type PendingItem<T> = {
  id: string
  timestamp: number
  attempts: number
  body: T
}

export function simulatedQueue<T>(
  sim: Simulation,
  queue: string,
  consume: Consumer<T>,
  options: QueueOptions<T> = {},
) {
  let sequence = 0
  const limit = options.maxRetries ?? 3
  if (!Number.isSafeInteger(limit) || limit < 0) throw new RangeError('invalid maxRetries')
  const maxBatchSize = options.maxBatchSize ?? 1
  if (!Number.isSafeInteger(maxBatchSize) || maxBatchSize < 1)
    throw new RangeError('invalid maxBatchSize')
  const maxBatchTimeoutMs = options.maxBatchTimeoutMs ?? 0
  if (!Number.isSafeInteger(maxBatchTimeoutMs) || maxBatchTimeoutMs < 0) {
    throw new RangeError('invalid maxBatchTimeoutMs')
  }
  const maxDelay = options.maxDelay ?? 0
  if (!Number.isSafeInteger(maxDelay) || maxDelay < 0) throw new RangeError('invalid maxDelay')

  const millis = (seconds = 0) => {
    if (!Number.isSafeInteger(seconds) || seconds < 0 || !Number.isSafeInteger(seconds * 1000)) {
      throw new RangeError('invalid queue delay')
    }
    return seconds * 1000
  }
  const retryDelay = millis(options.retryDelaySeconds)
  const duplicate = options.duplicate ?? 0
  if (!(duplicate >= 0 && duplicate <= 1)) throw new RangeError('invalid duplicate probability')
  const maxDuplicates = options.maxDuplicates ?? 1

  const buffer: PendingItem<T>[] = []
  let cancelFlush: (() => void) | undefined

  const deliverBatch = async (items: PendingItem<T>[]) => {
    const states = items.map((item) => {
      let decision: 'ack' | 'retry' | undefined
      let nextDelay = retryDelay
      const message: Delivery<T> = {
        id: item.id,
        timestamp: new Date(item.timestamp),
        attempts: item.attempts,
        body: structuredClone(item.body),
        ack() {
          decision ??= 'ack'
        },
        retry(details) {
          if (decision) return
          nextDelay =
            details?.delaySeconds === undefined ? retryDelay : millis(details.delaySeconds)
          decision = 'retry'
        },
      }
      return {
        item,
        message,
        get decision() {
          return decision
        },
        set decision(d: 'ack' | 'retry' | undefined) {
          decision = d
        },
        get nextDelay() {
          return nextDelay
        },
      }
    })

    const batch: QueueBatch<T> = {
      queue,
      messages: states.map((s) => s.message),
      ackAll() {
        for (const s of states) s.message.ack()
      },
      retryAll(opts) {
        for (const s of states) s.message.retry(opts)
      },
    }

    let thrown: unknown
    let didThrow = false
    try {
      await consume(batch)
    } catch (error) {
      didThrow = true
      thrown = error
      for (const s of states) {
        if (!s.decision) s.decision = 'retry'
      }
    }

    for (const s of states) {
      if (s.decision !== 'retry') continue
      if (s.item.attempts <= limit) {
        const nextItem: PendingItem<T> = {
          ...s.item,
          attempts: s.item.attempts + 1,
        }
        sim.after(
          s.nextDelay,
          `${s.item.id} retry ${nextItem.attempts}`,
          () => deliverBatch([nextItem]),
          { kind: 'queue-retry', actor: queue },
        )
      } else if (options.deadLetterQueue) {
        await options.deadLetterQueue.send(structuredClone(s.item.body))
      } else if (options.onExhausted) {
        await options.onExhausted(s.message)
      } else {
        throw didThrow ? thrown : new Error(s.item.id + ' exhausted retries')
      }
    }
  }

  const flushBuffer = () => {
    cancelFlush?.()
    cancelFlush = undefined
    if (buffer.length === 0) return
    const batch = buffer.splice(0, maxBatchSize)
    const label =
      batch.length === 1 ? batch[0]!.id : `${queue} batch(${batch.map((b) => b.id).join(',')})`
    sim.deliver(label, () => deliverBatch(batch), {
      duplicate,
      maxDuplicates,
      maxDelay,
      actor: queue,
    })
    if (buffer.length > 0) scheduleFlush()
  }

  const scheduleFlush = () => {
    if (buffer.length >= maxBatchSize) {
      flushBuffer()
      return
    }
    if (!cancelFlush) {
      cancelFlush = sim.after(maxBatchTimeoutMs, `${queue} flush`, flushBuffer, {
        kind: 'queue-flush',
        actor: queue,
      })
    }
  }

  function enqueue(body: T, delay: number) {
    const id = queue + '-' + sequence++
    const timestamp = sim.now
    const item: PendingItem<T> = { id, timestamp, attempts: 1, body }
    if (maxBatchSize === 1 && maxBatchTimeoutMs === 0) {
      sim.after(
        delay,
        id + ' ready',
        () =>
          sim.deliver(id, () => deliverBatch([item]), {
            duplicate,
            maxDuplicates,
            maxDelay,
            actor: queue,
          }),
        { kind: 'queue-ready', actor: queue },
      )
    } else {
      sim.after(
        delay,
        id + ' ready',
        () => {
          buffer.push(item)
          scheduleFlush()
        },
        { kind: 'queue-ready', actor: queue },
      )
    }
  }

  return {
    async send(body: T, sendOptions?: SendOptions) {
      enqueue(structuredClone(body), millis(sendOptions?.delaySeconds))
    },
    async sendBatch(messages: { body: T; delaySeconds?: number }[], sendOptions?: SendOptions) {
      const batch = messages.map((m) => ({
        body: structuredClone(m.body),
        delay: millis(m.delaySeconds ?? sendOptions?.delaySeconds),
      }))
      for (const m of batch) enqueue(m.body, m.delay)
    },
  }
}
