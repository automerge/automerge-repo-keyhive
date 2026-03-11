import { describe, bench, beforeAll } from "vitest";
import {
  ensureWasm,
  createFreshKeyhive,
  createKeyhiveFromArchive,
  loadArchiveBytes,
  loadEventBytes,
  loadKeypair,
} from "./setup.js";
import { Archive } from "@keyhive/keyhive/slim";

let archiveBytes: Uint8Array;
let allEvents: Uint8Array[];
let keyPair: CryptoKeyPair;

beforeAll(async () => {
  ensureWasm();
  archiveBytes = loadArchiveBytes();
  allEvents = loadEventBytes();
  keyPair = await loadKeypair();
  console.log(
    `Loaded archive (${archiveBytes.length} bytes), ${allEvents.length} events`
  );
});

describe("Compaction", () => {
  bench(
    "full round-trip: ingestArchive + ingestEvents + toArchive + toBytes",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      await kh.ingestEventsBytes(allEvents);
      const archive = await kh.toArchive();
      archive.toBytes();
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "archive ingestion only (tryToKeyhive from archive bytes)",
    async () => {
      await createKeyhiveFromArchive(archiveBytes, keyPair);
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "serialization only (toArchive + toBytes)",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      const archive = await kh.toArchive();
      archive.toBytes();
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
