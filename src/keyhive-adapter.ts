import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim"
import { Keyhive, Signer } from "@keyhive/keyhive"

import { signData, verifyData } from "./messages.js"
import { Pending } from "./pending.js"

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending()
  private peers: Set<PeerId> = new Set()

  constructor(
    private networkAdapter: NetworkAdapter,
    private signer: Signer,
    // TODO: Replace with dynamic configuration
    private hardcodedRemoteId: PeerId | null = null,
  ) {
    super()

    networkAdapter.on("message", (msg) => {
      this.receiveMessage(msg)
    })

    networkAdapter.on("peer-candidate", (payload) => {
      this.emit("peer-candidate", payload)
      this.peers.add(payload.peerId)
    })

    networkAdapter.on("peer-disconnected", (payload) => {
      this.emit("peer-disconnected", payload)
      this.peers.delete(payload.peerId)
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
      if (message.type === "keyhive") {
        if (message.targetId == this.peerId) {
          const originalSenderId = message.senderId
          message.senderId = this.peerId
          for (const targetId of this.peers) {
            if (targetId === originalSenderId || targetId === this.peerId) {
              continue
            }
            message.targetId = targetId
            this.signAndSend(message)
          }
        } else {
          this.signAndSend(message)
        }
      } else {
        this.signAndSend(message)
      }
    } else {
      this.networkAdapter.send(message)
    }
  }

  private signAndSend(message: Message): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!")
    }
    if (!("data" in message && message.data !== undefined)) {
      throw new Error("Data is expected for message to sign")
    }
    const seqNumber = this.pending.register()
    void signData(this.signer, this.peerId, message.data).then(
      (signedData: Uint8Array) => {
        this.pending.fire(seqNumber, () => {
          message.data = signedData
          this.networkAdapter.send(message)
        })
      },
    )
  }

  receiveMessage(message: Message): void {
    try {
      if (
        this.hardcodedRemoteId &&
        message.senderId !== this.hardcodedRemoteId
      ) {
        console.log("Unknown remote peer. Ignoring message!")
        return
      }
      if ("data" in message && message.data !== undefined) {
        const maybeSigned = verifyData(message.senderId, message.data)
        if (maybeSigned) {
          message.data = maybeSigned.payload
          if (message.type === "keyhive") {
            // TODO: Extend supported events and remove `(this as any)`
            (this as any).emit("keyhive", message)
          } else {
            this.emit("message", message)
          }
        } else {
          console.log("Signed message could not be verified!")
        }
      } else {
        if (message.type === "request-keyhive") {
          // TODO: Extend supported events and remove `(this as any)`
          (this as any).emit("request-keyhive", message)
        } else {
          this.emit("message", message)
        }
      }
    } catch (e) {
      console.error("Could not decode signed message:", e)
      return
    }
  }

  syncKeyhive(keyhive: Keyhive): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!")
    }
    const archiveBytes = keyhive.toArchive().toBytes()
    for (const targetId of this.peers) {
      const message = {
        type: "keyhive",
        senderId: this.peerId,
        targetId: targetId,
        data: archiveBytes,
      }
      this.send(message)
    }
  }

  requestKeyhive(): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!")
    }
    for (const targetId of this.peers) {
      const message = {
        type: "request-keyhive",
        senderId: this.peerId,
        targetId: targetId,
      }
      this.send(message)
    }
  }
}
