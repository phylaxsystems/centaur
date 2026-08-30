import { describe, expect, it } from "bun:test";

import { AssignmentGate } from "../src/assignment-gate";

/** Clock the tests drive by hand, so nothing waits on a real timer. */
function fakeClock(startMs = 1_000) {
  let nowMs = startMs;
  return {
    advance: (ms: number): void => {
      nowMs += ms;
    },
    now: (): number => nowMs,
  };
}

/** Drains the microtask queue; `run` awaits both acquire and the stagger. */
const flush = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0));

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, reject, resolve };
}

describe("AssignmentGate stagger", () => {
  it("spaces a burst that arrives in one tick", async () => {
    const clock = fakeClock();
    const delays: number[] = [];
    const gate = new AssignmentGate({
      concurrency: 0,
      now: clock.now,
      random: () => 0,
      sleep: async (ms) => {
        delays.push(ms);
      },
      staggerMs: 250,
    });

    const started: number[] = [];
    await Promise.all(
      [0, 1, 2, 3].map((index) =>
        gate.run(async () => {
          started.push(index);
        }),
      ),
    );

    // Four assignments must not become four simultaneous admissions. The
    // first is due now and never sleeps; the rest are spaced behind it.
    expect(delays).toEqual([250, 500, 750]);
    // Nothing is capped: unbounded means every turn still runs.
    expect(started.toSorted()).toEqual([0, 1, 2, 3]);
    expect(gate.activeCount).toBe(0);
  });

  it("never banks idle time", () => {
    const clock = fakeClock();
    const gate = new AssignmentGate({
      concurrency: 0,
      now: clock.now,
      random: () => 0,
      staggerMs: 250,
    });

    expect(gate.reserveDelayMs()).toBe(0);
    expect(gate.reserveDelayMs()).toBe(250);
    // A quiet stretch leaves the next assignment starting at once rather than
    // credited with the slots it did not use.
    clock.advance(60_000);
    expect(gate.reserveDelayMs()).toBe(0);
    expect(gate.reserveDelayMs()).toBe(250);
  });

  it("jitters within the spacing so starts miss the grid", () => {
    const gate = new AssignmentGate({
      concurrency: 0,
      now: fakeClock().now,
      random: () => 0.5,
      staggerMs: 250,
    });

    expect(gate.reserveDelayMs()).toBe(125);
    expect(gate.reserveDelayMs()).toBe(375);
  });

  it("treats a zero stagger as no delay", async () => {
    const gate = new AssignmentGate({
      concurrency: 0,
      sleep: async () => {
        throw new Error("should not sleep");
      },
      staggerMs: 0,
    });
    await expect(gate.run(async () => "ran")).resolves.toBe("ran");
  });
});

describe("AssignmentGate fleet share", () => {
  const unbounded = {
    now: fakeClock().now,
    random: () => 0,
    sleep: async () => undefined,
    staggerMs: 0,
  };

  it("does not bound turns at all by default", async () => {
    const gate = new AssignmentGate({ concurrency: 0, ...unbounded });
    const held = deferred();
    const runs = [0, 1, 2, 3, 4, 5].map(() =>
      gate.run(async () => {
        await held.promise;
      }),
    );

    await flush();
    // The regression this guards: a share nobody configured must never become
    // a cluster-wide work-in-progress cap.
    expect(gate.activeCount).toBe(6);
    expect(gate.queuedCount).toBe(0);
    held.resolve();
    await Promise.all(runs);
  });

  it("queues past a configured share and reports the wait", async () => {
    const gate = new AssignmentGate({ concurrency: 2, ...unbounded });
    const first = deferred();
    const started: number[] = [];
    const queuedAhead: number[] = [];
    const runs = [0, 1, 2, 3].map((index) =>
      gate.run(
        async () => {
          started.push(index);
          // Both admitted turns hold their slots, or the queue drains before
          // the assertions below ever see it.
          if (index < 2) await first.promise;
        },
        (ahead) => queuedAhead.push(ahead),
      ),
    );

    await flush();
    expect(started).toEqual([0, 1]);
    expect(gate.queuedCount).toBe(2);
    // Waiting is invisible on the issue unless the gate says so.
    expect(queuedAhead).toEqual([0, 1]);

    first.resolve();
    await Promise.all(runs);
    expect(started.toSorted()).toEqual([0, 1, 2, 3]);
    expect(gate.activeCount).toBe(0);
  });

  it("releases the slot when a turn throws", async () => {
    const gate = new AssignmentGate({ concurrency: 1, ...unbounded });

    await expect(
      gate.run(async () => {
        throw new Error("admission rejected");
      }),
    ).rejects.toThrow("admission rejected");

    // A failed turn must not wedge the queue: the failures this exists for are
    // the ones that throw.
    expect(gate.activeCount).toBe(0);
    await expect(gate.run(async () => "next")).resolves.toBe("next");
  });

  it("preserves arrival order when a slot frees up", async () => {
    const gate = new AssignmentGate({ concurrency: 1, ...unbounded });
    const first = deferred();
    const order: string[] = [];
    const running = gate.run(async () => {
      order.push("first");
      await first.promise;
    });
    await flush();
    const queuedA = gate.run(async () => {
      order.push("queued-a");
    });
    const queuedB = gate.run(async () => {
      order.push("queued-b");
    });

    first.resolve();
    await Promise.all([running, queuedA, queuedB]);
    expect(order).toEqual(["first", "queued-a", "queued-b"]);
  });
});
