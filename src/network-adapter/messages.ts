import { type PeerId } from "@automerge/automerge-repo/slim";
import { encode, decode } from "cbor-x";

import { ContactCard, Signed, Keyhive } from "@keyhive/keyhive/slim";
import { verifyingKeyPeerIdWithoutSuffix } from "../utilities.js";

export type KeyhiveMessageData = {
  contactCard: ContactCard;
  signed: Signed;
};

function encodeKeyhiveMessageData(msg: KeyhiveMessageData): Uint8Array {
  const contactCardJson = msg.contactCard.toJson();
  const signedBytes = msg.signed.toBytes();

  return encode({
    contactCard: contactCardJson,
    signed: signedBytes,
  });
}

export function decodeKeyhiveMessageData(
  encoded: Uint8Array
): KeyhiveMessageData {
  const decoded = decode(encoded) as {
    contactCard: string;
    signed: Uint8Array;
  };

  const contactCard = ContactCard.fromJson(decoded.contactCard);
  const signed = Signed.fromBytes(decoded.signed);

  return {
    contactCard,
    signed,
  };
}

export async function signData(
  keyhive: Keyhive,
  data: Uint8Array
): Promise<Uint8Array> {
  try {
    const signed = await keyhive.trySign(data);
    const contactCard = await keyhive.contactCard();
    return encodeKeyhiveMessageData({
      contactCard,
      signed,
    });
  } catch (error) {
    console.error("[AMRepoKeyhive] Error during signing:", error);
    throw error;
  }
}

// Verifies the provided data has a valid signature. Returns a `Signed` if so and `undefined` if not.
export function verifyData(
  peerId: PeerId,
  data: Uint8Array
): KeyhiveMessageData | undefined {
  try {
    const keyhiveMessageData = decodeKeyhiveMessageData(data);
    const verifyingKeyPeerId = verifyingKeyPeerIdWithoutSuffix(peerId);
    if (peerIdFromSigned(keyhiveMessageData.signed) !== verifyingKeyPeerId) {
      console.log(
        "[AMRepoKeyhive] Peer id on Signed does not match provided peer id"
      );
      console.debug("[AMRepoKeyhive] Expected: " + peerId);
      console.debug(
        "[AMRepoKeyhive] Found: " + peerIdFromSigned(keyhiveMessageData.signed)
      );
      return undefined;
    }

    if (keyhiveMessageData.signed.verify()) {
      return keyhiveMessageData;
    } else {
      return undefined;
    }
  } catch (error) {
    console.error("[AMRepoKeyhive] Failed to verify signed data:", error);
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
