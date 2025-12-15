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

export async function getEventsForPeer(
  keyhive: Keyhive,
  peerId: PeerId
): Promise<Map<Uint8Array, any> | null> {
  const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
  const agent = await keyhive.getAgent(keyhiveId);
  if (!agent) {
    return null;
  }
  const eventsForPeer = await keyhive.eventsForAgent(agent);
  const publicAgent = await keyhive.getAgent(Identifier.publicId());
  if (publicAgent) {
    const eventsForPublic = await keyhive.eventsForAgent(publicAgent);
    for (const [hash, event] of eventsForPublic.entries()) {
      eventsForPeer.set(hash, event);
    }
  }
  return eventsForPeer;
}

// Get the intersection of events that both peers can access, and union
// with their prekeys
export async function getEventsForPeerPair(
  keyhive: Keyhive,
  peerA: PeerId,
  peerB: PeerId
): Promise<Map<Uint8Array, any> | null> {
  const eventsForA = await getEventsForPeer(keyhive, peerA);
  const eventsForB = await getEventsForPeer(keyhive, peerB);

  if (!eventsForA || !eventsForB) {
    return null;
  }

  const peerBHashStrings = new Set<string>();
  for (const hash of eventsForB.keys()) {
    peerBHashStrings.add(hash.toString());
  }

  const result = new Map<Uint8Array, any>();
  const resultStrings = new Set<string>();

  // Add the intersection of hashes to results
  for (const [hash, event] of eventsForA.entries()) {
    const hashString = hash.toString();
    if (peerBHashStrings.has(hashString)) {
      resultStrings.add(hashString);
      result.set(hash, event);
    }
  }

  // Add prekeys for both peers
  for (const peerId of [peerA, peerB]) {
    const agent = await keyhive.getAgent(keyhiveIdentifierFromPeerId(peerId));
    if (agent) {
      for (const [hash, event] of (await agent.keyOps()).entries()) {
        const hashString = hash.toString();
        if (!resultStrings.has(hashString)) {
          resultStrings.add(hashString);
          result.set(hash, event);
        }
      }
    }
  }

  return result;
}
