import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim"
import { Signer } from "@keyhive/wasm"

import { signData, verifyData } from "./messages.js"
import { Pending } from "./pending.js"

export class KeyhiveClientAdapter extends NetworkAdapter {
  private pending = new Pending()

  constructor(
    private networkAdapter: NetworkAdapter,
    private signer: Signer,
    // TODO: Replace with dynamic configuration
    private hardcodedSyncServerPeerId: PeerId,
  ) {
    super()

    networkAdapter.on("message", (msg) => {
      this.receiveMessage(msg)
    })

    networkAdapter.on("peer-candidate", (payload) => {
      this.emit("peer-candidate", payload)
    })

    networkAdapter.on("peer-disconnected", (payload) => {
      this.emit("peer-disconnected", payload)
    })
  }

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    this.peerId = peerId
    this.peerMetadata = peerMetadata
    this.networkAdapter.connect(peerId, peerMetadata)
  }

  isReady(): boolean {
    return this.networkAdapter.isReady()
  }

  whenReady(): Promise<void> {
    return this.networkAdapter.whenReady()
  }

  disconnect(): void {
    this.networkAdapter.disconnect()
  }

  send(message: Message): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!")
    }
    if ("data" in message && message.data !== undefined) {
      const seqNumber = this.pending.register()
      void signData(this.signer, this.peerId, message.data).then(
        (signedData: Uint8Array) => {
          this.pending.fire(seqNumber, () => {
            message.data = signedData
            this.networkAdapter.send(message)
          })
        },
      )
    } else {
      this.networkAdapter.send(message)
    }
  }

  receiveMessage(message: Message): void {
    try {
      if (message.senderId !== this.hardcodedSyncServerPeerId) {
        console.log("Unknown sync server. Ignoring message!")
        return
      }
      if ("data" in message && message.data !== undefined) {
        const maybeSigned = verifyData(message.senderId, message.data)
        if (maybeSigned) {
          message.data = maybeSigned.payload
          this.emit("message", message)
        } else {
          console.log("Signed message could not be verified!")
        }
      } else {
        this.emit("message", message)
      }
    } catch (e) {
      console.error("Could not decode signed message:", e)
      return
    }
  }
}
