import { describe, it, expect } from "vitest";
import { Pending } from "../src/network-adapter/pending";

describe("Pending", () => {
  it("should process operations in order", () => {
    const pending = new Pending();
    const results: number[] = [];

    const seq1 = pending.register();
    const seq2 = pending.register();

    pending.fire(seq2, () => results.push(2));
    pending.fire(seq1, () => results.push(1));

    expect(results).toEqual([1, 2]);
  });

  it("should handle cancelled operations", () => {
    const pending = new Pending();
    const results: number[] = [];

    const seq1 = pending.register();
    const seq2 = pending.register();
    const seq3 = pending.register();

    pending.fire(seq2, () => results.push(2));
    pending.cancel(seq1);
    pending.fire(seq3, () => results.push(3));

    expect(results).toEqual([2, 3]);
  });

  it("should process queued operations after gaps are filled", () => {
    const pending = new Pending();
    const results: number[] = [];

    const seq1 = pending.register();
    const seq2 = pending.register();
    const seq3 = pending.register();
    const seq4 = pending.register();

    // Queue operations out of order
    pending.fire(seq3, () => results.push(3));
    pending.fire(seq4, () => results.push(4));
    pending.fire(seq2, () => results.push(2));

    expect(results).toEqual([]);

    pending.fire(seq1, () => results.push(1));

    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("should handle immediate sequential operations", () => {
    const pending = new Pending();
    const results: number[] = [];

    const seq1 = pending.register();
    pending.fire(seq1, () => results.push(1));

    const seq2 = pending.register();
    pending.fire(seq2, () => results.push(2));

    const seq3 = pending.register();
    pending.fire(seq3, () => results.push(3));

    expect(results).toEqual([1, 2, 3]);
  });
});
