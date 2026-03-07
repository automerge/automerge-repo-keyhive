import { workerData, parentPort } from "node:worker_threads";
import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";
import {
  Access,
  Archive,
  ChangeId,
  CiphertextStore,
  Identifier,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";

initFromBase64Wasm(wasmBase64);

async function createAndExport() {
  const signer = await Signer.generate();
  const store = CiphertextStore.newInMemory();
  const kh = await Keyhive.init(signer, store, () => {});

  const changeId = new ChangeId(new Uint8Array([4, 5, 6]));
  const doc = await kh.generateDocument([], changeId, []);

  const publicId = Identifier.publicId();
  const publicAgent = await kh.getAgent(publicId);
  const readAccess = Access.tryFromString("read");
  await kh.addMember(publicAgent, doc.toMembered(), readAccess, []);

  // Get events for public agent
  const events = await kh.eventsForAgent(publicAgent);
  const eventsBytes = [];
  events.forEach((value) => {
    eventsBytes.push(Array.from(value));
  });

  const stats = await kh.stats();
  return {
    eventsBytes,
    totalOps: Number(stats.totalOps),
    hashCount: (await kh.eventHashesForAgent(publicAgent)).length,
    eventCount: events.size,
  };
}

async function ingestAndCheck() {
  // Load kh1's archive (passed from main thread)
  const archiveBytes = new Uint8Array(workerData.archiveBytes);
  // Use a memory signer (different identity from the main thread's kh1,
  // but that's OK — we just need events from a different keyhive instance)
  const signer = Signer.generateMemory();
  const store = CiphertextStore.newInMemory();
  const kh = await Keyhive.init(signer, store, () => {});

  // Ingest kh1's archive
  const archive = new Archive(archiveBytes);
  await kh.ingestArchive(archive);

  const publicId = Identifier.publicId();
  const publicAgent = await kh.getAgent(publicId);

  // Check before ingestion
  const hashesBefore = (await kh.eventHashesForAgent(publicAgent)).length;
  const eventsBefore = (await kh.eventsForAgent(publicAgent)).size;
  const statsBefore = await kh.stats();

  // Create a new doc with public access on the restored keyhive
  const changeId = new ChangeId(new Uint8Array([7, 8, 9]));
  const doc = await kh.generateDocument([], changeId, []);
  const readAccess = Access.tryFromString("read");
  await kh.addMember(publicAgent, doc.toMembered(), readAccess, []);

  // Check after creating new doc
  const hashesAfter = (await kh.eventHashesForAgent(publicAgent)).length;
  const eventsAfter = (await kh.eventsForAgent(publicAgent)).size;
  const statsAfter = await kh.stats();

  // Get all events to send back
  const events = await kh.eventsForAgent(publicAgent);
  const eventsBytes = [];
  events.forEach((value) => {
    eventsBytes.push(Array.from(value));
  });

  return {
    hashesBefore,
    eventsBefore,
    totalOpsBefore: Number(statsBefore.totalOps),
    hashesAfter,
    eventsAfter,
    totalOpsAfter: Number(statsAfter.totalOps),
    eventsBytes,
  };
}

try {
  let result;
  if (workerData.action === "create-and-export") {
    result = await createAndExport();
  } else if (workerData.action === "ingest-and-check") {
    result = await ingestAndCheck();
  }
  parentPort.postMessage(result);
} catch (err) {
  parentPort.postMessage({ error: err.message, stack: err.stack });
}
