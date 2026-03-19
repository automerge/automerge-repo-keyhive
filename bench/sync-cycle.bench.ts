import { describe, bench, beforeAll } from "vitest";
import {
  ensureWasm,
  createKeyhiveFromArchive,
  getPublicAgent,
  loadArchiveBytes,
  loadEventBytes,
  loadKeypair,
  Identifier,
} from "./setup.js";
import { OpCache } from "../src/network-adapter/op-cache.js";
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
  kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
  const stats = await kh.stats();
  console.log(`Loaded keyhive with ${stats.totalOps} totalOps for sync cycle benchmark`);
});

describe("Simulated Sync Cycle Costs", () => {
  // Simulates what attemptRecovery does: re-ingest all events (as if read from storage)
  bench(
    "attemptRecovery: redundant ingestEventsBytes (simulating storage re-read)",
    async () => {
      await kh.ingestEventsBytes(allEvents);
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  // Non-cached hash lookup path: getAgent + eventHashesForAgent for two peers + public
  bench(
    "hash lookup (non-cached): 2x getAgent + 2x eventHashesForAgent + public",
    async () => {
      const publicId = Identifier.publicId();
      const pubAgent = await kh.getAgent(publicId);
      // Simulate looking up two peers (using public agent as stand-in)
      await kh.eventHashesForAgent(pubAgent!);
      await kh.eventHashesForAgent(pubAgent!);
      // Public hashes
      await kh.eventHashesForAgent(pubAgent!);
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  // Cached path: OpCache refresh + lookup
  bench(
    "hash lookup (cached): OpCache.refresh + getPublicHashes",
    async () => {
      const cache = new OpCache();
      await cache.refresh(kh);
      cache.getPublicHashes();
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  // Full sync cycle without cache: recovery + hash lookups + stats
  bench(
    "full sync cycle (non-cached, with recovery)",
    async () => {
      // 1. attemptRecovery: re-ingest from storage
      await kh.ingestEventsBytes(allEvents);
      // 2. stats check
      await kh.stats();
      // 3. hash lookups for one peer (getAgent + eventHashesForAgent)
      const publicId = Identifier.publicId();
      const pubAgent = await kh.getAgent(publicId);
      await kh.eventHashesForAgent(pubAgent!);
      await kh.eventHashesForAgent(pubAgent!);
      await kh.eventHashesForAgent(pubAgent!);
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  // Full sync cycle with OpCache
  bench(
    "full sync cycle (cached, with recovery)",
    async () => {
      // 1. attemptRecovery: re-ingest from storage
      await kh.ingestEventsBytes(allEvents);
      // 2. stats check
      await kh.stats();
      // 3. OpCache refresh + lookup
      const cache = new OpCache();
      await cache.refresh(kh);
      cache.getPublicHashes();
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
