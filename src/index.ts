import packageJson from "../package.json" with { type: "json" };
import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
// @ts-expect-error
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";

console.log(`automerge-repo-keyhive version: ${packageJson.version}`);

export function initKeyhiveWasm(): void {
  initFromBase64Wasm(wasmBase64);
}

export { Active } from "./keyhive/active.js";
export { addMemberToDoc, revokeMemberFromDoc } from "./keyhive/doc.js";
export { KeyhiveEventEmitter } from "./keyhive/emitter.js";
export {
  AutomergeRepoKeyhive,
  docIdFromAutomergeUrl,
  initializeAutomergeRepoKeyhive,
} from "./keyhive/keyhive.js";
export { KeyhiveNetworkAdapter } from "./network-adapter/network-adapter.js";
export { SyncServer } from "./sync-server.js";
export {
  peerIdFromSigner,
  uint8ArrayToHex,
  verifyingKeyPeerIdWithoutSuffix,
} from "./utilities.js";

// Re-export all keyhive types so consumers use the same WASM instance
export * from "@keyhive/keyhive/slim";
