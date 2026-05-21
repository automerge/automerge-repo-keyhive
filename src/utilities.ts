import { Signer } from "@keyhive/keyhive/slim";
import { peerIdFromVerifyingKey } from "./network-adapter/messages.js";
import { PeerId } from "@automerge/automerge-repo/slim";
import { Agent, Identifier, Keyhive } from "@keyhive/keyhive/slim";

export function peerIdFromSigner(signer: Signer, suffix: string = ""): PeerId {
  return peerIdFromVerifyingKey(signer.verifyingKey, suffix);
}

export function keyhiveIdentifierFromPeerId(peerId: PeerId): Identifier {
  const peerIdPrefix = verifyingKeyPeerIdWithoutSuffix(peerId);
  try {
    const verifyingKeyBytes = Uint8Array.from(atob(peerIdPrefix), (c) =>
      c.charCodeAt(0)
    );
    return new Identifier(verifyingKeyBytes);
  } catch (error) {
    throw new Error(`Failed to decode peer ID: ${peerId}`, { cause: error });
  }
}

export function verifyingKeyPeerIdWithoutSuffix(peerId: PeerId): PeerId {
  return peerId.split("-")[0] as PeerId;
}

export function uint8ArrayToHex(arr: Uint8Array): string {
  return Array.from(arr)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// WASM errors have a toError() method that returns a proper JS Error.
// This unwraps them if present, otherwise returns the original error.
export function unwrapWasmError(error: unknown): unknown {
  if (error && typeof error === "object" && "toError" in error && typeof (error as any).toError === "function") {
    return (error as any).toError();
  }
  return error;
}

export function arraysEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

export function hexToUint8Array(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return bytes;
}

export async function getEventsForAgent(
  keyhive: Keyhive,
  agent: Agent,
): Promise<Map<Uint8Array, Uint8Array>> {
  // WASM binding types this as Map<Uint8Array, any>; values are always event bytes
  return await keyhive.eventsForAgent(agent) as Map<Uint8Array, Uint8Array>;
}

// Returns event hashes for an agent as Map<hashString, hashBytes>
export async function getEventHashesForAgent(
  keyhive: Keyhive,
  agent: Agent,
): Promise<Map<string, Uint8Array>> {
  const hashMap = new Map<string, Uint8Array>();

  // Get relevant membership + prekey + CGKA hashes for the agent
  const eventHashes: Uint8Array[] = await keyhive.eventHashesForAgent(agent);
  for (const hash of eventHashes) {
    hashMap.set(hash.toString(), hash);
  }

  // Get the agent's own prekey hashes
  const keyOpHashes: Uint8Array[] = await agent.keyOpHashes();
  for (const hash of keyOpHashes) {
    hashMap.set(hash.toString(), hash);
  }

  return hashMap;
}
