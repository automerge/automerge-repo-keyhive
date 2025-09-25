import { Signer } from "@keyhive/keyhive/slim";
import { peerIdFromVerifyingKey } from "./network-adapter/messages.js";
import { PeerId } from "@automerge/automerge-repo/slim";

export function peerIdFromSigner(signer: Signer, suffix: string = ""): PeerId {
  return peerIdFromVerifyingKey(signer.verifyingKey, suffix);
}

export function verifyingKeyPeerIdWithoutSuffix(peerId: PeerId): PeerId {
  return peerId.split("-")[0] as PeerId;
}

export function uint8ArrayToHex(arr: Uint8Array): string {
  return Array.from(arr)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function hexToUint8Array(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return bytes;
}
