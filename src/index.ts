import packageJson from "../package.json" with { type: "json" };
import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
// @ts-expect-error
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";

console.warn(`automerge-repo-keyhive version: ${packageJson.version}`);

export const MODULE_INSTANCE_ID = Math.random().toString(36).slice(2);
console.warn(`[AMRepoKeyhive] Module instance ID: ${MODULE_INSTANCE_ID}`);

let wasmInitialized = false;

export function initKeyhiveWasm(): void {
  console.log(
    `[AMRepoKeyhive] initKeyhiveWasm called on instance ${MODULE_INSTANCE_ID}, already initialized: ${wasmInitialized}`
  );
  if (wasmInitialized) {
    return;
  }
  wasmInitialized = true;
  initFromBase64Wasm(wasmBase64);
  console.log(
    `[AMRepoKeyhive] WASM initialized on instance ${MODULE_INSTANCE_ID}`
  );
}

export function isWasmInitialized(): boolean {
  return wasmInitialized;
}

export { Active } from "./keyhive/active.js";
export { KeyhiveEventEmitter } from "./keyhive/emitter.js";
export { AutomergeRepoKeyhive, AutomergeRepoKeyhiveRust } from "./keyhive/automerge-repo-keyhive.js"
export {
  docIdFromAutomergeUrl,
  initializeAutomergeRepoKeyhive,
  initializeAutomergeRepoKeyhiveRust,
  KeyhiveStorage,
} from "./keyhive/keyhive.js";
export { KeyhiveNetworkAdapter } from "./network-adapter/network-adapter.js";
export {
  KeyhiveRustAdapter,
  encodeSukFrame,
  decodeSukFrame,
  isSukFrame,
  SUK_SCHEMA,
  encodeSignedMessage,
  decodeSignedMessage,
  encodeRustKeyhiveMessage,
  decodeRustKeyhiveMessage,
  peerIdFromRust,
  peerIdToRust,
  SukFrameError,
} from "./network-adapter/rust-transport/index.js";
export type {
  KeyhiveRustAdapterOptions,
  KeyhiveMessageType,
  RustEncodeInput,
  RustDecodeOutput,
  RustPeerId,
  RustSignedMessage,
} from "./network-adapter/rust-transport/index.js";
export { SyncServer } from "./sync-server.js";
export {
  peerIdFromSigner,
  uint8ArrayToHex,
  verifyingKeyPeerIdWithoutSuffix,
} from "./utilities.js";

// Re-export all keyhive types, with a compatibility shim for Access
// that maps old access level names ("pull"/"write") to new ones ("relay"/"edit").
export * from "@keyhive/keyhive/slim";

import { Access as _Access } from "@keyhive/keyhive/slim";

const ACCESS_COMPAT: Record<string, string> = {
  pull: "relay",
  write: "edit",
};

// Wraps the WASM Access class to accept old access level names.
export const Access = new Proxy(_Access, {
  get(target, prop, receiver) {
    if (prop === "tryFromString") {
      return (s: string) => target.tryFromString(ACCESS_COMPAT[s] ?? s);
    }
    return Reflect.get(target, prop, receiver);
  },
});
