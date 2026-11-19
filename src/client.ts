import type { Simulation } from './index.js'

export type Checkpoint = (delay: number, label: string) => Promise<void>

/**
 * A sequential client can yield in the middle of an async operation, including
 * from a Miniflare service binding. Run one segment through completion or its
 * next checkpoint before allowing another client to advance.
 */
export function client(sim: Simulation, name: string, task: (checkpoint: Checkpoint) => Promise<void>) {
  let active: ReturnType<typeof Promise.withResolvers<void>> | undefined
  const segment = (resume: () => void) => {
    active = Promise.withResolvers<void>()
    const finished = active.promise
    resume()
    return finished
  }
  const checkpoint: Checkpoint = (delay, label) => {
    if (!active) throw new Error('checkpoint requires an active client segment')
    const next = Promise.withResolvers<void>()
    const signal = sim.signal
    const abort = () => next.reject(signal.reason)
    sim.after(delay, name + ' / ' + label, () => {
      signal.removeEventListener('abort', abort)
      return segment(next.resolve)
    })
    signal.addEventListener('abort', abort, { once: true })
    active.resolve()
    active = undefined
    return next.promise
  }
  sim.after(0, name, () => segment(() => {
    Promise.resolve().then(() => task(checkpoint)).then(
      () => active?.resolve(),
      error => active?.reject(error)
    )
  }))
}
