// Extract benchmark dataset from a synced peer's keyhive storage.
//
// Usage:
//   pnpm bench:extract --peer-data-dir ~/dev/automerge-keyhive-demo-sync-server/.peer-data-0/keyhive

import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
// @ts-expect-error
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";
import {
  Archive,
  CiphertextStore,
  Identifier,
  Signer,
} from "@keyhive/keyhive/slim";
import fs from "node:fs";
import path from "node:path";

function parseArgs(): { peerDataDir: string } {
  const args = process.argv.slice(2);
  const idx = args.indexOf("--peer-data-dir");
  if (idx === -1 || idx + 1 >= args.length) {
    console.error("Usage: pnpm bench:extract --peer-data-dir <path>");
    process.exit(1);
  }
  return { peerDataDir: args[idx + 1] };
}

async function loadKeypairFromFile(keypairPath: string): Promise<CryptoKeyPair> {
  const raw = fs.readFileSync(keypairPath, "utf-8");
  const { publicKey: pubJwk, privateKey: privJwk } = JSON.parse(raw);
  const publicKey = await crypto.subtle.importKey("jwk", pubJwk, "Ed25519", true, pubJwk.key_ops);
  const privateKey = await crypto.subtle.importKey("jwk", privJwk, "Ed25519", true, privJwk.key_ops);
  return { publicKey, privateKey };
}

async function main() {
  const { peerDataDir } = parseArgs();

  // Init WASM
  initFromBase64Wasm(wasmBase64);

  // Load keypair
  const keypairPath = path.join(peerDataDir, "ac", "tive-key-pair-2");
  if (!fs.existsSync(keypairPath)) {
    console.error(`Keypair not found at ${keypairPath}`);
    process.exit(1);
  }
  const keyPair = await loadKeypairFromFile(keypairPath);
  const signer = await Signer.webCryptoSigner(keyPair);
  console.log("Loaded keypair and created signer");

  // Find archive files
  const archivesDir = path.join(peerDataDir, "ke", "yhive-db", "archives");
  const archiveFiles = fs.existsSync(archivesDir)
    ? fs.readdirSync(archivesDir).filter((f) => !f.startsWith("."))
    : [];
  console.log(`Found ${archiveFiles.length} archive file(s)`);

  if (archiveFiles.length === 0) {
    console.error("No archive files found. Has the peer synced and compacted?");
    process.exit(1);
  }

  // Load first archive and create keyhive
  const firstArchiveBytes = fs.readFileSync(path.join(archivesDir, archiveFiles[0]));
  const firstArchive = new Archive(new Uint8Array(firstArchiveBytes));
  const store = CiphertextStore.newInMemory();
  const kh = await firstArchive.tryToKeyhive(store, signer, () => {});
  console.log("Created keyhive from first archive");

  // Ingest additional archives
  for (let i = 1; i < archiveFiles.length; i++) {
    const bytes = fs.readFileSync(path.join(archivesDir, archiveFiles[i]));
    await kh.ingestArchive(new Archive(new Uint8Array(bytes)));
    console.log(`Ingested archive ${i + 1}/${archiveFiles.length}`);
  }

  // Find and ingest loose events
  const opsDir = path.join(peerDataDir, "ke", "yhive-db", "ops");
  const opFiles = fs.existsSync(opsDir)
    ? fs.readdirSync(opsDir).filter((f) => !f.startsWith("."))
    : [];
  console.log(`Found ${opFiles.length} loose event file(s)`);

  if (opFiles.length > 0) {
    const eventsToIngest = opFiles.map((f) =>
      new Uint8Array(fs.readFileSync(path.join(opsDir, f)))
    );
    await kh.ingestEventsBytes(eventsToIngest);
    console.log(`Ingested ${opFiles.length} loose events`);
  }

  // Get public agent
  const publicId = Identifier.publicId();
  const agent = await kh.getAgent(publicId);
  if (!agent) {
    console.error("Failed to get public agent");
    process.exit(1);
  }

  // Get hashes and events
  const hashes = await kh.eventHashesForAgent(agent);
  const events = await kh.eventsForAgent(agent);
  console.log(`Event hashes: ${hashes.length}, Events: ${events.size}`);

  // Write output files
  const outDir = path.join(import.meta.dirname!, "..", "bench", "data");
  fs.mkdirSync(outDir, { recursive: true });

  // archive.bin
  fs.writeFileSync(path.join(outDir, "archive.bin"), firstArchiveBytes);
  console.log(`Wrote archive.bin (${firstArchiveBytes.length} bytes)`);

  // keypair.json — copy the raw keypair file
  const keypairJson = fs.readFileSync(keypairPath, "utf-8");
  fs.writeFileSync(path.join(outDir, "keypair.json"), keypairJson);
  console.log("Wrote keypair.json");

  // events.bin — length-prefixed format: [4-byte BE length][event bytes]...
  const eventByteArrays: Uint8Array[] = [];
  events.forEach((value: Uint8Array) => {
    eventByteArrays.push(value);
  });

  let totalEventsSize = 0;
  for (const ev of eventByteArrays) {
    totalEventsSize += 4 + ev.length;
  }
  const eventsBuf = new Uint8Array(totalEventsSize);
  const eventsView = new DataView(eventsBuf.buffer);
  let offset = 0;
  for (const ev of eventByteArrays) {
    eventsView.setUint32(offset, ev.length, false); // big-endian
    eventsBuf.set(ev, offset + 4);
    offset += 4 + ev.length;
  }
  fs.writeFileSync(path.join(outDir, "events.bin"), eventsBuf);
  console.log(`Wrote events.bin (${eventByteArrays.length} events, ${totalEventsSize} bytes)`);

  // hashes.bin — concatenated 32-byte hash arrays
  const hashesBuf = new Uint8Array(hashes.length * 32);
  for (let i = 0; i < hashes.length; i++) {
    hashesBuf.set(hashes[i], i * 32);
  }
  fs.writeFileSync(path.join(outDir, "hashes.bin"), hashesBuf);
  console.log(`Wrote hashes.bin (${hashes.length} hashes, ${hashesBuf.length} bytes)`);

  console.log("\nExtraction complete!");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
