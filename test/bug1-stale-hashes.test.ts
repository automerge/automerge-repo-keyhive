import { describe, it, expect, beforeAll } from "vitest";
import {
  initKeyhiveWasm,
} from "../src/index.js";
import {
  Access,
  Archive,
  ChangeId,
  CiphertextStore,
  Identifier,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import { Worker } from "node:worker_threads";
import path from "node:path";
import { fileURLToPath } from "node:url";

beforeAll(() => {
  initKeyhiveWasm();
});

async function createKeyhive() {
  const signer = await Signer.generate();
  const store = CiphertextStore.newInMemory();
  const kh = await Keyhive.init(signer, store, () => {});
  return { kh, signer };
}

async function getPublicAgent(kh: Keyhive) {
  const publicId = Identifier.publicId();
  const agent = await kh.getAgent(publicId);
  if (!agent) throw new Error("Failed to get public agent");
  return agent;
}

async function getHashCount(kh: Keyhive) {
  const agent = await getPublicAgent(kh);
  const hashes = await kh.eventHashesForAgent(agent);
  return hashes.length;
}

async function getEventCount(kh: Keyhive) {
  const agent = await getPublicAgent(kh);
  const events = await kh.eventsForAgent(agent);
  return events.size;
}

async function getEventsBytes(kh: Keyhive): Promise<Uint8Array[]> {
  const agent = await getPublicAgent(kh);
  const events = await kh.eventsForAgent(agent);
  const bytes: Uint8Array[] = [];
  events.forEach((value: Uint8Array) => {
    bytes.push(value);
  });
  return bytes;
}

/**
 * Run keyhive operations in a separate worker thread (separate WASM instance).
 * This avoids the shared-mutex deadlock that occurs when two keyhive instances
 * exist in the same WASM module.
 */
function runInWorker(workerData: any): Promise<any> {
  return new Promise((resolve, reject) => {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const worker = new Worker(
      path.join(__dirname, "bug1-worker.mjs"),
      { workerData }
    );
    worker.on("message", resolve);
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
    });
  });
}

describe("Bug 1: eventHashesForAgent returns stale results after ingestEventsBytes", () => {
  it("hashes and events are consistent before any ingestion", async () => {
    const { kh } = await createKeyhive();
    const changeId = new ChangeId(new Uint8Array([1, 2, 3]));
    const doc = await kh.generateDocument([], changeId, []);

    const publicAgent = await getPublicAgent(kh);
    const readAccess = Access.tryFromString("read");
    await kh.addMember(publicAgent, doc.toMembered(), readAccess!, []);

    const hashes = await getHashCount(kh);
    const events = await getEventCount(kh);
    console.log(`hashes=${hashes}, events=${events}`);
    expect(hashes).toBe(events);
    expect(hashes).toBeGreaterThan(0);
  }, 30000);

  it("hashes consistent after archive round-trip", async () => {
    const { kh, signer } = await createKeyhive();
    const changeId = new ChangeId(new Uint8Array([1, 2, 3]));
    const doc = await kh.generateDocument([], changeId, []);

    const publicAgent = await getPublicAgent(kh);
    const readAccess = Access.tryFromString("read");
    await kh.addMember(publicAgent, doc.toMembered(), readAccess!, []);

    const hashesBefore = await getHashCount(kh);
    const eventsBefore = await getEventCount(kh);
    console.log(`before archive: hashes=${hashesBefore}, events=${eventsBefore}`);

    const archive = await kh.toArchive();
    const archiveBytes = archive.toBytes();

    const store2 = CiphertextStore.newInMemory();
    const archive2 = new Archive(archiveBytes);
    const khRestored = await archive2.tryToKeyhive(store2, signer, () => {});

    const hashesAfter = await getHashCount(khRestored);
    const eventsAfter = await getEventCount(khRestored);
    console.log(`after restore: hashes=${hashesAfter}, events=${eventsAfter}`);
    expect(hashesAfter).toBe(eventsAfter);
    expect(hashesAfter).toBe(hashesBefore);
  }, 30000);

  it("hashes consistent after ingesting foreign events (cross-keyhive)", async () => {
    // Create kh1 with doc + public access
    const { kh: kh1 } = await createKeyhive();
    const changeId = new ChangeId(new Uint8Array([1, 2, 3]));
    const doc = await kh1.generateDocument([], changeId, []);
    const publicAgent = await getPublicAgent(kh1);
    const readAccess = Access.tryFromString("read");
    await kh1.addMember(publicAgent, doc.toMembered(), readAccess!, []);

    const hashesBefore = await getHashCount(kh1);
    const eventsBefore = await getEventCount(kh1);
    console.log(`kh1 before: hashes=${hashesBefore}, events=${eventsBefore}`);

    // Run kh2 in a separate worker thread to avoid shared-WASM mutex deadlock.
    // The worker creates a keyhive with its own doc + public access and returns
    // its events as serialized bytes.
    const workerResult = await runInWorker({ action: "create-and-export" });
    console.log(`worker returned ${workerResult.eventsBytes.length} events`);

    // Ingest kh2's events into kh1
    const foreignEvents = workerResult.eventsBytes.map(
      (b: number[]) => new Uint8Array(b)
    );
    const pending = await kh1.ingestEventsBytes(foreignEvents);
    console.log(`kh1 ingested, ${pending.length} pending`);

    const stats = await kh1.stats();
    console.log(`kh1 totalOps: ${stats.totalOps}`);

    // BUG 1 CHECK
    const hashesAfter = await getHashCount(kh1);
    const eventsAfter = await getEventCount(kh1);
    console.log(`kh1 after ingestion: hashes=${hashesAfter}, events=${eventsAfter}`);

    expect(hashesAfter).toBe(eventsAfter);
    expect(hashesAfter).toBeGreaterThan(hashesBefore);
  }, 60000);

  it("hashes consistent after archive restore, new doc in worker, then ingest back (production scenario)", async () => {
    // Production scenario:
    // 1. kh1 creates doc1 + public access
    // 2. kh1 archives
    // 3. Worker: restores from archive, creates doc2 + public access
    // 4. kh1 ingests worker's events (which include ops for EXISTING doc1 + new doc2)
    // 5. Check eventHashesForAgent on kh1
    //
    // This matches production where SharedWorker and tab share the same keyhive
    // identity (via archive) and add docs independently.

    const { kh: kh1, signer } = await createKeyhive();
    const changeId = new ChangeId(new Uint8Array([1, 2, 3]));
    const doc = await kh1.generateDocument([], changeId, []);
    const publicAgent = await getPublicAgent(kh1);
    const readAccess = Access.tryFromString("read");
    await kh1.addMember(publicAgent, doc.toMembered(), readAccess!, []);

    const hashesBefore = await getHashCount(kh1);
    const eventsBefore = await getEventCount(kh1);
    console.log(`kh1 original: hashes=${hashesBefore}, events=${eventsBefore}`);

    // Archive kh1
    const archive = await kh1.toArchive();
    const archiveBytes = archive.toBytes();

    // Worker: creates its own keyhive, ingests kh1's archive,
    // creates a new doc + public access, returns all events
    const workerResult = await runInWorker({
      action: "ingest-and-check",
      archiveBytes: Array.from(archiveBytes),
    });

    if (workerResult.error) {
      throw new Error(`Worker error: ${workerResult.error}\n${workerResult.stack}`);
    }

    console.log(`worker: before=${workerResult.hashesBefore}h/${workerResult.eventsBefore}e, after=${workerResult.hashesAfter}h/${workerResult.eventsAfter}e, totalOps=${workerResult.totalOpsAfter}`);
    console.log(`worker returning ${workerResult.eventsBytes.length} events`);

    // Ingest worker's events into kh1
    // These events include both kh1's original doc1 ops (duplicates) AND new doc2 ops
    const foreignEvents = workerResult.eventsBytes.map(
      (b: number[]) => new Uint8Array(b)
    );
    const pending = await kh1.ingestEventsBytes(foreignEvents);
    console.log(`kh1 ingested, ${pending.length} pending`);

    const stats = await kh1.stats();
    console.log(`kh1 totalOps: ${stats.totalOps}`);

    // BUG 1 CHECK: kh1 created doc1's delegations locally, then ingested
    // doc2's delegations from worker. The ingested events also include duplicates
    // of doc1's events. eventHashesForAgent should reflect ALL delegations.
    const hashesAfter = await getHashCount(kh1);
    const eventsAfter = await getEventCount(kh1);
    console.log(`kh1 after ingestion: hashes=${hashesAfter}, events=${eventsAfter}`);

    expect(hashesAfter).toBe(eventsAfter);
    expect(hashesAfter).toBeGreaterThan(hashesBefore);
  }, 60000);
});
