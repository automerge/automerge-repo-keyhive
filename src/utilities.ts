import { Signer } from "@keyhive/keyhive"
import { peerIdFromVerifyingKey } from "./messages.js"
import { PeerId } from "@automerge/automerge-repo/slim"

export function peerIdFromSigner(signer: Signer, suffix: string = ""): PeerId {
  return peerIdFromVerifyingKey(signer.verifyingKey, suffix)
}

export function verifyingKeyPeerIdWithoutSuffix(peerId: PeerId): PeerId {
  return peerId.split('-')[0] as PeerId
}
