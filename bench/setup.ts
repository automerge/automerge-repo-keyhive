import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
// @ts-expect-error
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";
import {
  Archive,
  CiphertextStore,
  Identifier,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import fs from "node:fs";
import path from "node:path";

const DATA_DIR = path.join(import.meta.dirname!, "data");

let wasmReady = false;

export function ensureWasm(): void {
  if (!wasmReady) {
    initFromBase64Wasm(wasmBase64);
    wasmReady = true;
  }
}

export function loadArchiveBytes(): Uint8Array {
  return new Uint8Array(fs.readFileSync(path.join(DATA_DIR, "archive.bin")));
}

export function loadEventBytes(): Uint8Array[] {
  const buf = fs.readFileSync(path.join(DATA_DIR, "events.bin"));
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const events: Uint8Array[] = [];
  let offset = 0;
  while (offset < buf.length) {
    const len = view.getUint32(offset, false); // big-endian
    offset += 4;
    events.push(new Uint8Array(buf.buffer, buf.byteOffset + offset, len));
    offset += len;
  }
  return events;
}

export function loadHashes(): Uint8Array[] {
  const buf = fs.readFileSync(path.join(DATA_DIR, "hashes.bin"));
  const hashes: Uint8Array[] = [];
  for (let i = 0; i < buf.length; i += 32) {
    hashes.push(new Uint8Array(buf.buffer, buf.byteOffset + i, 32));
  }
  return hashes;
}

export async function loadKeypair(): Promise<CryptoKeyPair> {
  const raw = fs.readFileSync(path.join(DATA_DIR, "keypair.json"), "utf-8");
  const { publicKey: pubJwk, privateKey: privJwk } = JSON.parse(raw);
  const publicKey = await crypto.subtle.importKey("jwk", pubJwk, "Ed25519", true, pubJwk.key_ops);
  const privateKey = await crypto.subtle.importKey("jwk", privJwk, "Ed25519", true, privJwk.key_ops);
  return { publicKey, privateKey };
}

export async function createFreshKeyhive(): Promise<Keyhive> {
  const signer = await Signer.generate();
  const store = CiphertextStore.newInMemory();
  return Keyhive.init(signer, store, () => {});
}

export async function createKeyhiveFromArchive(
  archiveBytes: Uint8Array,
  keyPair: CryptoKeyPair
): Promise<Keyhive> {
  const signer = await Signer.webCryptoSigner(keyPair);
  const store = CiphertextStore.newInMemory();
  const archive = new Archive(archiveBytes);
  return archive.tryToKeyhive(store, signer, () => {});
}

export async function getPublicAgent(kh: Keyhive) {
  const publicId = Identifier.publicId();
  const agent = await kh.getAgent(publicId);
  if (!agent) throw new Error("Failed to get public agent");
  return agent;
}
