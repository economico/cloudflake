import type { Action, Simulation } from './index.js'

/** The single replaceable alarm slot of a simulated Durable Object. Keep the
 * slot with durable test state and resolve the current instance in fire(). */
export function simulatedAlarm(sim: Simulation, object: string, fire: Action) {
  let at: number | null = null
  let cancel: (() => void) | undefined
  return {
    async getAlarm() { return at },
    async setAlarm(when: number | Date) {
      const timestamp = Number(when)
      if (!Number.isSafeInteger(timestamp) || timestamp < 0) throw new RangeError('invalid alarm time')
      cancel?.()
      at = timestamp
      cancel = sim.after(Math.max(0, timestamp - sim.now), object + ' alarm', async () => {
        at = null
        cancel = undefined
        await fire()
      })
    },
    async deleteAlarm() {
      cancel?.()
      at = null
      cancel = undefined
    }
  }
}
