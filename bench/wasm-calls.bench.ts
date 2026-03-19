import { describe, bench, beforeAll, beforeEach } from "vitest";
import {
  ensureWasm,
  createKeyhiveFromArchive,
  getPublicAgent,
  loadArchiveBytes,
  loadEventBytes,
  loadKeypair,
  Identifier,
} from "./setup.js";
import type { Keyhive } from "@keyhive/keyhive/slim";

let archiveBytes: Uint8Array;
let allEvents: Uint8Array[];
let keyPair: CryptoKeyPair;
let kh: Keyhive;

beforeAll(async () => {
  ensureWasm();
  archiveBytes = loadArchiveBytes();
  allEvents = loadEventBytes();
  keyPair = await loadKeypair();
  // Create a loaded keyhive for all the per-call benchmarks
  kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
  const stats = await kh.stats();
  console.log(`Loaded keyhive with ${stats.totalOps} totalOps`);
});

describe("WASM per-call costs", () => {
  bench(
    "stats()",
    async () => {
      await kh.stats();
    },
    { iterations: 100, warmupIterations: 10, time: 0, warmupTime: 0 }
  );

  bench(
    "getAgent(publicId)",
    async () => {
      const publicId = Identifier.publicId();
      await kh.getAgent(publicId);
    },
    { iterations: 100, warmupIterations: 10, time: 0, warmupTime: 0 }
  );

  bench(
    "eventHashesForAgent (public)",
    async () => {
      const agent = await getPublicAgent(kh);
      await kh.eventHashesForAgent(agent);
    },
    { iterations: 10, warmupIterations: 2, time: 0, warmupTime: 0 }
  );

  bench(
    "eventsForAgent (public)",
    async () => {
      const agent = await getPublicAgent(kh);
      await kh.eventsForAgent(agent);
    },
    { iterations: 10, warmupIterations: 2, time: 0, warmupTime: 0 }
  );

  bench(
    "eventHashesForAgent + eventsForAgent (the BUG1 diagnostic pair)",
    async () => {
      const agent = await getPublicAgent(kh);
      await kh.eventHashesForAgent(agent);
      await kh.eventsForAgent(agent);
    },
    { iterations: 10, warmupIterations: 2, time: 0, warmupTime: 0 }
  );

  bench(
    "allAgentEvents (OpCache WASM call)",
    async () => {
      await (kh as any).allAgentEvents();
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
