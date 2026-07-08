import { log } from "./logging.js";
import { initFromBase64Wasm } from "@keyhive/keyhive/slim";
// @ts-expect-error
import { wasmBase64 } from "@keyhive/keyhive/keyhive_wasm.base64.js";

let wasmInitialized = false;

/**
 * Initialize the keyhive WASM module. Idempotent. The init functions call
 * this automatically; call it directly only when using keyhive WASM types
 * (e.g. `ContactCard`, `Access`) before initializing a hive.
 */
export function initKeyhiveWasm(): void {
  if (wasmInitialized) {
    return;
  }
  wasmInitialized = true;
  initFromBase64Wasm(wasmBase64);
  log.debug("[AMRepoKeyhive] WASM initialized");
}

export function isWasmInitialized(): boolean {
  return wasmInitialized;
}
