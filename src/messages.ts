import { type PeerId } from "@automerge/automerge-repo/slim";

import { Signer, Signed } from "@keyhive/keyhive"

export async function signData(
  signer: Signer,
  _peerId: PeerId,
  data: Uint8Array,
): Promise<Uint8Array> {
  try {
    const signed = await signer.trySign(data);
    return signed.toBytes();
  } catch (error) {
    console.error("Error during signing:", error);
    throw error;
  }
}

// Verifies the provided data has a valid signature. Returns a `Signed` if so and `undefined` if not.
export function verifyData(
  peerId: PeerId,
  data: Uint8Array,
): Signed | undefined {
  const signed = Signed.fromBytes(data);
  if (peerIdFromSigned(signed) !== peerId) {
    console.log("Peer id on Signed does not match provided peer id");
    console.log("Expected: " + peerId);
    console.log("Found: " + peerIdFromSigned(signed));
    return undefined;
  }

  if (signed.verify()) {
    return signed;
  } else {
    return undefined;
  }
}

function peerIdFromSigned(signed: Signed): PeerId {
  return peerIdFromVerifyingKey(signed.verifyingKey);
}

export function peerIdFromVerifyingKey(verifyingKey: Uint8Array): PeerId {
  return btoa(String.fromCharCode(...verifyingKey)) as PeerId;
}
