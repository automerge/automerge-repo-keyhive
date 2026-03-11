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

describe("Redundant Ingestion", () => {
  bench(
    "ingestEventsBytes into fresh keyhive",
    async () => {
      const kh = await createFreshKeyhive();
      await kh.ingestEventsBytes(allEvents);
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "ingestEventsBytes into keyhive that already has the same events (from archive)",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      await kh.ingestEventsBytes(allEvents);
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "ingestArchive into fresh keyhive",
    async () => {
      const kh = await createFreshKeyhive();
      await kh.ingestArchive(new Archive(archiveBytes));
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "ingestArchive into keyhive that already has the same data",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      await kh.ingestArchive(new Archive(archiveBytes));
    },
    { iterations: 5, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
