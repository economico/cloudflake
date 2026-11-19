import type { Simulation } from './index.js'

export type Checkpoint = (delay: number, label: string) => Promise<void>

export type CheckpointHub = {
  /** Spawn a named sequential client and automatically register its checkpoint gate. */
  spawn(
    name: string,
    task: (ctx: { id: string; headers: Record<string, string>; checkpoint: Checkpoint }) => Promise<void>
  ): string
  /**
   * Pause the client identified by `clientId` (or extracted from a Request's
   * `x-cloudflake-client` header). If no client is active for that id, this is a no-op.
   */
  pause(clientIdOrRequest: string | { headers: { get(name: string): string | null } }, delay: number, label: string): Promise<void>
  /** Header name used to propagate client correlation across Worker fetches. */
  readonly headerName: string
}

export const CLIENT_HEADER = 'x-cloudflake-client'

/**
 * A sequential client can yield in the middle of an async operation, including
 * from a Miniflare service binding. Run one segment through completion or its
 * next checkpoint before allowing another client to advance.
 */
export function client(
  sim: Simulation,
  name: string,
  task: (checkpoint: Checkpoint) => Promise<void>
) {
  let active: ReturnType<typeof Promise.withResolvers<void>> | undefined
  let exited = false
  const exitOnce = () => {
    if (exited) return
    exited = true
    sim._exitClient()
  }
  sim._enterClient()

  const segment = (resume: () => void) => {
    active = Promise.withResolvers<void>()
    sim._setActiveClientPark(park)
    const finished = active.promise
    resume()
    return finished
  }

  const park = (registerWake: (wake: () => void) => void, label: string) => {
    if (!active) throw new Error('park requires an active client segment')
    const next = Promise.withResolvers<void>()
    const signal = sim.signal
    const abort = () => next.reject(signal.reason)
    signal.addEventListener('abort', abort, { once: true })
    let woke = false
    registerWake(() => {
      if (woke || signal.aborted) return
      woke = true
      sim.after(
        0,
        name + ' / ' + label,
        () => {
          signal.removeEventListener('abort', abort)
          return segment(next.resolve)
        },
        { kind: 'checkpoint', actor: name }
      )
    })
    sim._setActiveClientPark(undefined)
    active.resolve()
    active = undefined
    return next.promise
  }

  const checkpoint: Checkpoint = (delay, label) => {
    if (!active) throw new Error('checkpoint requires an active client segment')
    const next = Promise.withResolvers<void>()
    const signal = sim.signal
    const abort = () => next.reject(signal.reason)
    sim.after(
      delay,
      name + ' / ' + label,
      () => {
        signal.removeEventListener('abort', abort)
        return segment(next.resolve)
      },
      { kind: 'checkpoint', actor: name }
    )
    signal.addEventListener('abort', abort, { once: true })
    sim._setActiveClientPark(undefined)
    active.resolve()
    active = undefined
    return next.promise
  }

  sim.after(
    0,
    name,
    () =>
      segment(() => {
        Promise.resolve()
          .then(() => task(checkpoint))
          .then(
            () => {
              exitOnce()
              sim._setActiveClientPark(undefined)
              active?.resolve()
            },
            error => {
              exitOnce()
              sim._setActiveClientPark(undefined)
              active?.reject(error)
            }
          )
      }),
    { kind: 'client', actor: name }
  )
}

/**
 * Request-correlated checkpoint hub. Eliminates fragile URL-pathname gate maps when
 * multiple concurrent requests hit the same endpoint in `simulateWorkers`.
 */
export function createCheckpointHub(sim: Simulation): CheckpointHub {
  const gates = new Map<string, Checkpoint>()
  let seq = 0

  return {
    headerName: CLIENT_HEADER,
    spawn(name, task) {
      const id = `${name}-${seq++}`
      client(sim, name, async checkpoint => {
        gates.set(id, checkpoint)
        try {
          await task({
            id,
            headers: { [CLIENT_HEADER]: id },
            checkpoint
          })
        } finally {
          gates.delete(id)
        }
      })
      return id
    },
    async pause(clientIdOrRequest, delay, label) {
      const id =
        typeof clientIdOrRequest === 'string'
          ? clientIdOrRequest
          : clientIdOrRequest.headers.get(CLIENT_HEADER)
      if (!id) return
      const gate = gates.get(id)
      if (!gate) return
      await gate(delay, label)
    }
  }
}
