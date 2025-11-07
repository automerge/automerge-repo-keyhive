import { type PeerId } from "@automerge/automerge-repo/slim";
import { encode, decode } from "cbor-x";

import { ContactCard, Signed, Keyhive } from "@keyhive/keyhive/slim";
import { verifyingKeyPeerIdWithoutSuffix } from "../utilities.js";

export type KeyhiveMessageData = {
  contactCard?: ContactCard;
  signed: Signed;
};

function encodeKeyhiveMessageData(msg: KeyhiveMessageData): Uint8Array {
  const contactCardJson = msg.contactCard ? msg.contactCard.toJson() : "";
  const signedBytes = msg.signed.toBytes();

  return encode({
    contactCard: contactCardJson,
    signed: signedBytes,
  });
}

export function decodeKeyhiveMessageData(
  encoded: Uint8Array
): KeyhiveMessageData | undefined {
  try {
    const decoded = decode(encoded) as {
      contactCard: string;
      signed: Uint8Array;
    };

    const contactCard =
      decoded.contactCard === ""
        ? undefined
        : ContactCard.fromJson(decoded.contactCard);
    const signed = Signed.fromBytes(decoded.signed);

    return {
      contactCard,
      signed,
    };
  } catch (error) {
    console.error(
      "[AMRepoKeyhive] Failed to decode keyhive message data:",
      error
    );
    return undefined;
  }
}

export async function signData(
  keyhive: Keyhive,
  data: Uint8Array,
  includeContactCard: boolean
): Promise<Uint8Array> {
  try {
    const signed = await keyhive.trySign(data);
    const contactCard = includeContactCard
      ? await keyhive.contactCard()
      : undefined;
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
export function verifyData(peerId: PeerId, data: KeyhiveMessageData): boolean {
  try {
    const verifyingKeyPeerId = verifyingKeyPeerIdWithoutSuffix(peerId);
    if (peerIdFromSigned(data.signed) !== verifyingKeyPeerId) {
      console.log(
        "[AMRepoKeyhive] Peer id on Signed does not match provided peer id"
      );
      console.debug("[AMRepoKeyhive] Expected: " + peerId);
      console.debug("[AMRepoKeyhive] Found: " + peerIdFromSigned(data.signed));
      return false;
    }

    if (data.signed.verify()) {
      return true;
    } else {
      return false;
    }
  } catch (error) {
    console.error("[AMRepoKeyhive] Failed to verify signed data:", error);
    return false;
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
