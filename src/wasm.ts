import { log } from "./logging.js";
// @ts-expect-error (dist/index.d.ts omits initSync and the Init* types; a
// wasm-bodge limitation noted in keyhive's README)
import { initSync } from "@keyhive/keyhive/slim";
// @ts-expect-error (the generated base64 wasm module ships no type declarations)
import { wasmBase64 } from "@keyhive/keyhive/wasm-base64";

let wasmInitialized = false;

/**
 * Initialize the keyhive WASM module. Idempotent. The init functions call
 * this automatically. Call it directly only when using keyhive WASM types
 * (e.g. `ContactCard`, `Access`) before initializing a hive.
 */
export function initKeyhiveWasm(): void {
  if (wasmInitialized) {
    return;
  }
  wasmInitialized = true;
  const bytes = Uint8Array.from(atob(wasmBase64), (c) => c.charCodeAt(0));
  initSync({ module: bytes });
  log.debug("[AMRepoKeyhive] WASM initialized");
}

export function isWasmInitialized(): boolean {
  return wasmInitialized;
}
