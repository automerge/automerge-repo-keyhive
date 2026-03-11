import { describe, bench, beforeAll } from "vitest";
import { ensureWasm, createFreshKeyhive, loadEventBytes } from "./setup.js";

let allEvents: Uint8Array[];

beforeAll(() => {
  ensureWasm();
  allEvents = loadEventBytes();
  console.log(`Loaded ${allEvents.length} events for ingestion benchmark`);
});

describe("Event Ingestion on Startup", () => {
  bench(
    "ingestEventsBytes (all events into fresh keyhive)",
    async () => {
      const kh = await createFreshKeyhive();
      await kh.ingestEventsBytes(allEvents);
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
