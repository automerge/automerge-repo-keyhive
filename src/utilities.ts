import { Signer } from "@keyhive/keyhive/slim";
import { peerIdFromVerifyingKey } from "./network-adapter/messages.js";
import { PeerId } from "@automerge/automerge-repo/slim";
import { Identifier, Keyhive } from "@keyhive/keyhive/slim";

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

export function hexToUint8Array(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return bytes;
}

export async function getMembershipOpsForPeer(
  keyhive: Keyhive,
  peerId: PeerId
): Promise<Map<Uint8Array, any> | null> {
  const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
  const agent = await keyhive.getAgent(keyhiveId);
  if (!agent) {
    // FIXME: Remove this warning?
    console.warn(`[AMRepoKeyhive] No agent found for peer ${peerId}`);
    return null;
  }
  return await keyhive.eventsForAgent(agent);
}
