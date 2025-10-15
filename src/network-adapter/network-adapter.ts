import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
  StorageAdapterInterface,
} from "@automerge/automerge-repo/slim";
import { Keyhive } from "@keyhive/keyhive/slim";

import { signData, verifyData } from "./messages.js";
import { Pending } from "./pending.js";
import { saveKeyhiveWithHash } from "../keyhive/keyhive.js";

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending();
  private peers: Set<PeerId> = new Set();

  constructor(
    private networkAdapter: NetworkAdapter,
    private keyhive: Keyhive,
    private storage: StorageAdapterInterface,
    // TODO: Replace with dynamic configuration
    private hardcodedRemoteId: PeerId | null = null
  ) {
    super();

    // Polling for keyhive updates
    setInterval(this.requestKeyhive.bind(this), 15000);

    networkAdapter.on("message", (msg) => {
      this.receiveMessage(msg);
    });

    networkAdapter.on("peer-candidate", (payload) => {
      this.emit("peer-candidate", payload);
      this.peers.add(payload.peerId);
    });

    networkAdapter.on("peer-disconnected", (payload) => {
      this.emit("peer-disconnected", payload);
      this.peers.delete(payload.peerId);
    });

    // FIXME: Remove
    // setInterval(async () => {
    //   await saveKeyhiveWithHash(keyhive, storage);
    //   this.syncKeyhive(keyhive);
    //   console.debug("[Adapter] interval fired: saved and synced!");
    // }, 10000);
  }

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    console.log(`[Adapter] this.peerId: ${peerId}`);
    this.peerId = peerId;
    this.peerMetadata = peerMetadata;
    this.networkAdapter.connect(peerId, peerMetadata);
  }

  isReady(): boolean {
    return this.networkAdapter.isReady();
  }

  whenReady(): Promise<void> {
    return this.networkAdapter.whenReady();
  }

  disconnect(): void {
    this.networkAdapter.disconnect();
  }

  send(message: Message): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    if ("data" in message && message.data !== undefined) {
      if (message.type === "keyhive") {
        if (message.targetId == this.peerId) {
          const originalSenderId = message.senderId;
          message.senderId = this.peerId;
          for (const targetId of this.peers) {
            if (targetId === originalSenderId || targetId === this.peerId) {
              continue;
            }
            message.targetId = targetId;
            console.debug(`[Adapter] Sending keyhive message to ${targetId}`);
            this.signAndSend(message);
          }
        } else {
          console.debug(`[Adapter] Sending keyhive message to ${message.targetId}`);
          this.signAndSend(message);
        }
      } else {
        this.signAndSend(message);
      }
    } else {
      this.networkAdapter.send(message);
    }
  }

  private signAndSend(message: Message): void {
    void this.asyncSyncAndSend(message);
  }

  async asyncSyncAndSend(message: Message): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    if (!("data" in message && message.data !== undefined)) {
      throw new Error("Data is expected for message to sign");
    }
    const seqNumber = this.pending.register();
    const signedData = await signData(this.keyhive, message.data as Uint8Array);
    // Wait for network to be ready before sending
    await this.networkAdapter.whenReady();
    this.pending.fire(seqNumber, () => {
      message.data = signedData;
      this.networkAdapter.send(message);
    });
  }

  receiveMessage(message: Message): void {
    try {
      // if (this.hardcodedRemoteId &&
      //   message.senderId !== this.hardcodedRemoteId
      // ) {
      //   console.log(`Unknown remote peer ${message.senderId}. Ignoring message!`);
      //   return;
      // }
      if ("data" in message && message.data !== undefined) {
        const maybeSigned = verifyData(message.senderId, message.data);
        if (maybeSigned) {
          message.data = maybeSigned.payload;
          if (message.type === "keyhive") {
            (this as any).emit("keyhive", message);
          } else {
            this.emit("message", message);
          }
        } else {
          console.error("[Adapter] Signed message could not be verified!");
        }
      } else {
        if (message.type === "request-keyhive") {
          (this as any).emit("request-keyhive", message);
        } else {
          this.emit("message", message);
        }
      }
    } catch (e) {
      console.error("[Adapter] Could not decode signed message:", e);
      return;
    }
  }

  syncKeyhive(keyhive: Keyhive, maybeSenderId: PeerId | undefined = undefined): void {
    void this.asyncSyncKeyhive(keyhive, maybeSenderId);
  }

  private async asyncSyncKeyhive(keyhive: Keyhive, maybeSenderId: PeerId | undefined): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    let archiveBytes: Uint8Array;
    try {
      archiveBytes = (await keyhive.toArchive()).toBytes();
      if (!archiveBytes || archiveBytes.length === 0) {
        console.error("[Adapter] Archive serialization produced empty bytes, skipping sync");
        return;
      }
    } catch (error) {
      console.error("[Adapter] Failed to serialize keyhive archive:", error);
      return;
    }
    let senderId: PeerId;
    if (maybeSenderId) {
      senderId = maybeSenderId;
    } else {
      senderId = this.peerId;
    }
    for (const targetId of this.peers) {
      if (targetId == senderId) {
        continue;
      }
      console.debug(`Syncing to ${targetId} from senderId ${senderId}`)
      this.sendKeyhive(this.peerId, targetId, archiveBytes);
    }
  }

  private sendKeyhive(senderId: PeerId, targetId: PeerId, archiveBytes: Uint8Array): void {
    const message = {
      type: "keyhive",
      senderId: senderId,
      targetId: targetId,
      data: archiveBytes,
    };
    this.send(message);
  }

  private requestKeyhive(): void {
    if (this.peerId === undefined) {
      return;
    }
    for (const targetId of this.peers) {
      const message = {
        type: "request-keyhive",
        senderId: this.peerId,
        targetId: targetId,
      };
      this.send(message);
    }
  }
}
