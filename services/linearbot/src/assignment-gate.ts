/**
 * Paces the assignment turns linearbot starts, and optionally bounds how many
 * of them run at once.
 *
 * A qualifying `Issue` webhook kicks a turn immediately and independently, so a
 * burst starts every turn on the same instant. Bursts are routine: someone
 * hands the bot a batch of issues, Linear redelivers webhooks that failed while
 * the bot or its control plane was down, and the membership-only fallback means
 * redelivered `update` payloads for already-delegated issues qualify as fresh
 * handoffs.
 *
 * Two separate problems come out of that, and this handles them separately.
 *
 * **Alignment.** Turns that start on the same instant hit sandbox admission in
 * the same tick, spawn their sandboxes together, and then issue their first
 * inference call together — the least useful shape for the model serving behind
 * them. Admission is a limit rather than a queue, so a batch that would have
 * fit had it arrived spread out can still lose the check-then-act race, and
 * `runThreadTurn` only retries on the cold-start backoff (250ms, doubling,
 * three attempts), far too short to outlast a sandbox running someone else's
 * work. `staggerMs` spaces consecutive starts so they stop colliding. It delays
 * turns; it never drops or bounds them, and it is on by default.
 *
 * **Fleet share.** `concurrency` is something else entirely: a cap on turns in
 * flight, which only makes sense as this ingress's slice of a sandbox fleet
 * shared with the other ingresses. Slices are meant to oversubscribe — x/y/z
 * across n pods, summing to more than n — so that a quiet githubbot leaves its
 * pods usable rather than idle. That makes it a deployment-wide decision, not
 * something this file can guess, so it defaults to off (0). Left off, the only
 * limit is the fleet's own admission limit, which is also off by default
 * upstream.
 *
 * A bound that is set is a queue, not a rejection: surplus turns wait here for
 * a slot instead of being dropped. Waiting is invisible on the issue, so `run`
 * reports it — see `onQueued`.
 */

export type AssignmentGateOptions = {
  /**
   * Turns allowed in flight at once, as this ingress's share of the sandbox
   * fleet. 0 disables the bound, which is the default: a cap set here that is
   * unrelated to the fleet's real size is a work-in-progress limit nobody asked
   * for. Values below 0 are treated as 0.
   */
  concurrency: number;
  /**
   * Minimum spacing between consecutive turn starts. Each start also waits a
   * random extra delay in `[0, staggerMs)`, so starts scatter instead of
   * landing on an exact grid; adjacent turns can swap order, which is the
   * point. 0 disables the delay.
   */
  staggerMs: number;
  /** Injectable for tests. */
  now?: () => number;
  /** Injectable for tests. */
  random?: () => number;
  /** Injectable for tests. */
  sleep?: (ms: number) => Promise<void>;
};

const defaultSleep = (ms: number): Promise<void> =>
  ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();

export class AssignmentGate {
  private readonly concurrency: number;
  private readonly staggerMs: number;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private active = 0;
  private readonly waiting: Array<() => void> = [];
  /**
   * When the next start is allowed. Held on the instance rather than derived
   * per-request: bursts arrive as independent webhooks, so the schedule only
   * spaces anything if they all reserve against one cursor.
   */
  private nextStartAtMs = 0;

  constructor(options: AssignmentGateOptions) {
    this.concurrency = Math.max(0, Math.floor(options.concurrency));
    this.staggerMs = Math.max(0, Math.floor(options.staggerMs));
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.sleep = options.sleep ?? defaultSleep;
  }

  /** Turns currently in flight. Exposed for tests and diagnostics. */
  get activeCount(): number {
    return this.active;
  }

  /** Turns waiting for a slot. Exposed for tests and diagnostics. */
  get queuedCount(): number {
    return this.waiting.length;
  }

  /**
   * Claims the next start slot and reports how long this arrival waits for it.
   *
   * Synchronous on purpose: the cursor has to move before the caller yields, or
   * a burst arriving within one tick would all read the same slot. Idle time is
   * never banked — the cursor is pulled forward to now — so a quiet bot starts
   * the next assignment immediately.
   */
  reserveDelayMs(): number {
    if (this.staggerMs === 0) return 0;
    const now = this.now();
    const startAtMs = Math.max(now, this.nextStartAtMs);
    this.nextStartAtMs = startAtMs + this.staggerMs;
    return startAtMs - now + Math.floor(this.random() * this.staggerMs);
  }

  /**
   * Runs `task` once a slot is free and its scheduled start comes around.
   *
   * `onQueued` fires — with the number of turns already waiting — when this one
   * has to wait for a slot rather than starting straight away. A queued
   * assignment is otherwise entirely invisible: nothing posts on the issue, its
   * status does not move, and the only evidence the bot took the work is that
   * it is not doing it.
   *
   * The slot is released whatever the task does, including throwing: a turn
   * that fails must not wedge the queue behind it, since the failure modes this
   * exists to handle are exactly the ones that throw.
   */
  async run<T>(
    task: () => Promise<T>,
    onQueued?: (queuedAhead: number) => void,
  ): Promise<T> {
    await this.acquire(onQueued);
    try {
      // Reserve unconditionally so the cursor advances even when this start is
      // due now; only the waiting is skipped.
      const delayMs = this.reserveDelayMs();
      if (delayMs > 0) await this.sleep(delayMs);
      return await task();
    } finally {
      this.release();
    }
  }

  private acquire(onQueued?: (queuedAhead: number) => void): Promise<void> {
    if (this.concurrency === 0 || this.active < this.concurrency) {
      this.active += 1;
      return Promise.resolve();
    }
    const admitted = new Promise<void>((resolve) => {
      this.waiting.push(resolve);
    });
    onQueued?.(this.waiting.length - 1);
    return admitted;
  }

  private release(): void {
    const next = this.waiting.shift();
    if (next) {
      // Hand the slot straight over rather than decrementing and racing: a
      // burst would otherwise let a newly arrived turn overtake a queued one.
      next();
      return;
    }
    this.active -= 1;
  }
}
