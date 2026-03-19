import { describe, bench, beforeAll } from "vitest";
import {
  ensureWasm,
  createKeyhiveFromArchive,
  loadArchiveBytes,
  loadEventBytes,
  loadHashes,
  loadKeypair,
} from "./setup.js";
import {
  cborByteString,
  buildCborByteStringArray,
  buildSyncResponseCbor,
} from "../src/network-adapter/cbor-builder.js";
import { OpCache } from "../src/network-adapter/op-cache.js";

let hashes: Uint8Array[];
let allEvents: Uint8Array[];
let archiveBytes: Uint8Array;
let keyPair: CryptoKeyPair;

beforeAll(async () => {
  ensureWasm();
  hashes = loadHashes();
  allEvents = loadEventBytes();
  archiveBytes = loadArchiveBytes();
  keyPair = await loadKeypair();
  console.log(`Loaded ${hashes.length} hashes, ${allEvents.length} events`);
});

describe("CBOR Encoding", () => {
  bench(
    "cborByteString for all hashes",
    () => {
      for (const hash of hashes) {
        cborByteString(hash);
      }
    },
    { iterations: 50, warmupIterations: 5, time: 0, warmupTime: 0 }
  );

  bench(
    "buildCborByteStringArray for all hashes",
    () => {
      const cborHashes = hashes.map((h) => cborByteString(h));
      buildCborByteStringArray(cborHashes);
    },
    { iterations: 50, warmupIterations: 5, time: 0, warmupTime: 0 }
  );

  bench(
    "buildSyncResponseCbor with all hashes + events",
    () => {
      const cborEvents = allEvents.map((ev) => cborByteString(ev));
      buildSyncResponseCbor(hashes, cborEvents, hashes.length, hashes.length);
    },
    { iterations: 50, warmupIterations: 5, time: 0, warmupTime: 0 }
  );
});

describe("OpCache", () => {
  bench(
    "OpCache.refresh (full cache rebuild)",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      const cache = new OpCache();
      await cache.refresh(kh);
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );

  bench(
    "OpCache.getHashesForAgent (lookup after refresh)",
    async () => {
      const kh = await createKeyhiveFromArchive(archiveBytes, keyPair);
      const cache = new OpCache();
      await cache.refresh(kh);
      // Lookup public hashes repeatedly
      for (let i = 0; i < 100; i++) {
        cache.getPublicHashes();
      }
    },
    { iterations: 3, warmupIterations: 1, time: 0, warmupTime: 0 }
  );
});
