import { Signer } from "@keyhive/keyhive";
import { peerIdFromVerifyingKey } from "./messages.js";
import { PeerId } from "@automerge/automerge-repo/slim";

export function peerIdFromSigner(signer: Signer): PeerId {
  return peerIdFromVerifyingKey(signer.verifyingKey);
}
