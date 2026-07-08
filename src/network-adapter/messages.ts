import { log } from "../logging.js";
import { type Message, type PeerId } from "@automerge/automerge-repo/slim";
import { encode, decode } from "cbor-x";

import { ContactCard, Signed, Keyhive } from "@keyhive/keyhive/slim";
import { verifyingKeyPeerIdWithoutSuffix } from "../utilities.js";

export type KeyhiveMessageData = {
  contactCard?: ContactCard;
  signed: Signed;
};

export type KeyhiveMessage = {
  msg: Message;
  data: KeyhiveMessageData;
};

export function encodeKeyhiveMessageData(msg: KeyhiveMessageData): Uint8Array {
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

    if (decoded.contactCard !== "") {
      log.debug(
        "[AMRepoKeyhive] decodeKeyhiveMessageData: parsing contact card from message"
      );
    }
    const contactCard =
      decoded.contactCard === ""
        ? undefined
        : ContactCard.fromJson(decoded.contactCard);
    const signed = Signed.fromBytes(decoded.signed);

    return {
      contactCard,
      signed,
    };
  } catch {
    // Non-keyhive messages are expected to fail decoding here.
    return undefined;
  }
}

export async function signData(
    keyhive: Keyhive,
    data: Uint8Array,
    contactCard ?: ContactCard
  ): Promise < Uint8Array > {
    try {
      const signed = await keyhive.trySign(data);
      return encodeKeyhiveMessageData({
        contactCard,
        signed,
      });
    } catch(error) {
      log.error("[AMRepoKeyhive] Error during signing:", error);
      throw error;
    }
  }

// Verifies the provided data has a valid signature.
export function verifyData(peerId: PeerId, data: KeyhiveMessageData): boolean {
  try {
    const verifyingKeyPeerId = verifyingKeyPeerIdWithoutSuffix(peerId);
    if (peerIdFromSigned(data.signed) !== verifyingKeyPeerId) {
      log.debug(
        "[AMRepoKeyhive] Peer id on Signed does not match provided peer id"
      );
      log.debug("[AMRepoKeyhive] Expected: " + peerId);
      log.debug("[AMRepoKeyhive] Found: " + peerIdFromSigned(data.signed));
      return false;
    }

    if (data.signed.verify()) {
      return true;
    } else {
      return false;
    }
  } catch (error) {
    log.error("[AMRepoKeyhive] Failed to verify signed data:", error);
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
