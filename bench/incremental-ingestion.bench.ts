import { describe, bench, beforeAll } from "vitest";
import { ensureWasm, createFreshKeyhive, loadEventBytes } from "./setup.js";

let allEvents: Uint8Array[];

beforeAll(() => {
  ensureWasm();
  allEvents = loadEventBytes();
  console.log(`Loaded ${allEvents.length} events for incremental ingestion benchmark`);
});

describe("Incremental Ingestion", () => {
  bench(
    "single batch: all events at once",
    async () => {
      const kh = await createFreshKeyhive();
      await kh.ingestEventsBytes(allEvents);
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "batches of 5000 events (~3 batches)",
    async () => {
      const kh = await createFreshKeyhive();
      for (let i = 0; i < allEvents.length; i += 5000) {
        await kh.ingestEventsBytes(allEvents.slice(i, i + 5000));
      }
    },
    { iterations: 2, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "batches of 1000 events (~16 batches)",
    async () => {
      const kh = await createFreshKeyhive();
      for (let i = 0; i < allEvents.length; i += 1000) {
        await kh.ingestEventsBytes(allEvents.slice(i, i + 1000));
      }
    },
    { iterations: 2, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "batches of 500 events (~31 batches)",
    async () => {
      const kh = await createFreshKeyhive();
      for (let i = 0; i < allEvents.length; i += 500) {
        await kh.ingestEventsBytes(allEvents.slice(i, i + 500));
      }
    },
    { iterations: 2, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
