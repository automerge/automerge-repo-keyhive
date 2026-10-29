import { log } from "../logging.js";
import { type Message, type PeerId } from "@automerge/automerge-repo/slim";
import { encode } from "cbor-x";
import { decodeCbor } from "./cbor-decode.js";

import { ContactCard, Keyhive, SignedBytes } from "@keyhive/keyhive/slim";
import { verifyingKeyPeerIdWithoutSuffix } from "../utilities.js";

export type KeyhiveMessageData = {
  contactCard?: ContactCard;
  signed: SignedBytes;
};

export type KeyhiveMessage = {
  msg: Message;
  data: KeyhiveMessageData;
};

export function encodeKeyhiveMessageData(msg: KeyhiveMessageData): Uint8Array {
  const contactCardBytes = msg.contactCard
    ? msg.contactCard.toBytes()
    : new Uint8Array();
  const signedBytes = msg.signed.toBytes();

  return encode({
    contactCard: contactCardBytes,
    signed: signedBytes,
  });
}

export function decodeKeyhiveMessageData(
  encoded: Uint8Array
): KeyhiveMessageData | undefined {
  try {
    const decoded = decodeCbor(encoded) as {
      contactCard: Uint8Array;
      signed: Uint8Array;
    };

    if (decoded.contactCard.length > 0) {
      log.debug(
        "[AMRepoKeyhive] decodeKeyhiveMessageData: parsing contact card from message"
      );
    }
    const contactCard =
      decoded.contactCard.length === 0
        ? undefined
        : ContactCard.fromBytes(decoded.contactCard);
    const signed = SignedBytes.fromBytes(decoded.signed);

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
  contactCard?: ContactCard
): Promise<Uint8Array> {
  try {
    const signed = await keyhive.trySign(data);
    return encodeKeyhiveMessageData({
      contactCard,
      signed,
    });
  } catch (error) {
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

function peerIdFromSigned(signed: SignedBytes, suffix: string = ""): PeerId {
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
