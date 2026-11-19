import type { Simulation } from './index.js'

export type AlarmInfo = Readonly<{
  retryCount: number
  isRetry: boolean
}>

export type AlarmOptions = {
  /** Number of retries on throw (Cloudflare Durable Objects retry up to 6 times). Default 0. */
  maxRetries?: number
  /** Base backoff in virtual ms for exponential retry delay (base * 2^retryCount). Default 2000. */
  retryBaseDelayMs?: number
  /** Probability an alarm fires twice (at-least-once execution). Default 0. */
  duplicate?: number
  /** Maximum extra scheduling jitter (virtual ms) added when an alarm fires. Default 0. */
  maxDelay?: number
  /** Called if an alarm throws and exhausts `maxRetries`. If omitted, the thrown error fails the run. */
  onExhausted?: (error: unknown, info: AlarmInfo) => void | Promise<void>
}

/**
 * The single replaceable alarm slot of a simulated Durable Object.
 * Keep the slot with durable test state and resolve the current instance in `fire()`.
 */
export function simulatedAlarm(
  sim: Simulation,
  object: string,
  fire: (alarmInfo: AlarmInfo) => void | Promise<void>,
  options: AlarmOptions = {},
) {
  const maxRetries = options.maxRetries ?? 0
  if (!Number.isSafeInteger(maxRetries) || maxRetries < 0) {
    throw new RangeError('invalid alarm maxRetries')
  }
  const retryBaseDelayMs = options.retryBaseDelayMs ?? 2000
  if (!Number.isSafeInteger(retryBaseDelayMs) || retryBaseDelayMs < 0) {
    throw new RangeError('invalid alarm retryBaseDelayMs')
  }
  const duplicate = options.duplicate ?? 0
  if (!(duplicate >= 0 && duplicate <= 1)) {
    throw new RangeError('invalid alarm duplicate probability')
  }
  const maxDelay = options.maxDelay ?? 0
  if (!Number.isSafeInteger(maxDelay) || maxDelay < 0) {
    throw new RangeError('invalid alarm maxDelay')
  }

  const rng = sim.rngStream(`alarm:${object}`)
  let at: number | null = null
  let cancel: (() => void) | undefined
  let generation = 0
  // Retries and duplicate firings of the current generation, cancelled when the alarm is replaced or deleted.
  const followUps = new Set<() => void>()

  const cancelAll = () => {
    cancel?.()
    cancel = undefined
    for (const c of followUps) c()
    followUps.clear()
  }

  const scheduleFollowUp = (
    delay: number,
    label: string,
    run: () => Promise<void>,
    kind: string,
  ) => {
    const cancelThis: () => void = sim.after(
      delay,
      label,
      () => {
        followUps.delete(cancelThis)
        return run()
      },
      { kind, actor: object },
    )
    followUps.add(cancelThis)
  }

  const execute = async (gen: number, retryCount: number) => {
    if (gen !== generation) return
    const info: AlarmInfo = Object.freeze({ retryCount, isRetry: retryCount > 0 })
    try {
      await fire(info)
    } catch (error) {
      // The handler replaced or deleted its own alarm; the failed one is not retried.
      if (gen !== generation) return
      if (
        retryCount < maxRetries &&
        sim.recordFault('alarm_retry', `${object} alarm retry ${retryCount + 1}`)
      ) {
        const backoff = retryBaseDelayMs * Math.pow(2, retryCount)
        scheduleFollowUp(
          backoff,
          `${object} alarm retry ${retryCount + 1}`,
          () => execute(gen, retryCount + 1),
          'alarm-retry',
        )
        return
      }
      if (options.onExhausted) {
        await options.onExhausted(error, info)
        return
      }
      throw error
    }
  }

  return {
    async getAlarm() {
      return at
    },
    async setAlarm(when: number | Date) {
      const timestamp = Number(when)
      if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
        throw new RangeError('invalid alarm time')
      }
      cancelAll()
      at = timestamp
      const gen = ++generation
      const jitter = maxDelay > 0 && sim.chaosEnabled ? rng.int(maxDelay + 1) : 0
      const delay = Math.max(0, timestamp - sim.now) + jitter
      cancel = sim.after(
        delay,
        object + ' alarm',
        async () => {
          if (gen !== generation) return
          at = null
          cancel = undefined
          if (
            duplicate > 0 &&
            sim.decideFault('alarm_duplicate', duplicate, `${object} alarm`, rng)
          ) {
            const dupDelay = maxDelay > 0 ? rng.int(maxDelay + 1) : 0
            scheduleFollowUp(
              dupDelay,
              `${object} alarm [duplicate]`,
              () => execute(gen, 0),
              'alarm-duplicate',
            )
          }
          await execute(gen, 0)
        },
        { kind: 'alarm', actor: object },
      )
    },
    async deleteAlarm() {
      cancelAll()
      generation++
      at = null
    },
  }
}
