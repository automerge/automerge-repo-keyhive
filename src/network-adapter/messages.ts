import { type PeerId } from "@automerge/automerge-repo/slim";

import { Signed, Keyhive } from "@keyhive/keyhive/slim";
import { verifyingKeyPeerIdWithoutSuffix } from "../utilities.js";

export async function signData(
  keyhive: Keyhive,
  data: Uint8Array
): Promise<Uint8Array> {
  try {
    const signed = await keyhive.trySign(data);
    return signed.toBytes();
  } catch (error) {
    console.error("[Adapter] Error during signing:", error);
    throw error;
  }
}

// Verifies the provided data has a valid signature. Returns a `Signed` if so and `undefined` if not.
export function verifyData(
  peerId: PeerId,
  data: Uint8Array
): Signed | undefined {
  try {
    const signed = Signed.fromBytes(data);
    const verifyingKeyPeerId = verifyingKeyPeerIdWithoutSuffix(peerId);
    if (peerIdFromSigned(signed) !== verifyingKeyPeerId) {
      console.log("[Adapter] Peer id on Signed does not match provided peer id");
      console.debug("[Adapter] Expected: " + peerId);
      console.debug("[Adapter] Found: " + peerIdFromSigned(signed));
      return undefined;
    }

    if (signed.verify()) {
      return signed;
    } else {
      return undefined;
    }
  } catch (error) {
    console.error("[Adapter] Failed to verify signed data:", error);
    return undefined;
  }
}

function peerIdFromSigned(signed: Signed, suffix: string = ""): PeerId {
  return peerIdFromVerifyingKey(signed.verifyingKey, suffix);
}

export function peerIdFromVerifyingKey(
  verifyingKey: Uint8Array,
  suffix: string = ""
): PeerId {
  let peerId = btoa(String.fromCharCode(...verifyingKey));
  if (suffix !== "") {
    peerId = peerId + "-" + suffix;
  }
  return peerId as PeerId;
}
