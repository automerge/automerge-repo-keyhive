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
import {
  getMembershipOpsForPeer,
  keyhiveIdentifierFromPeerId,
} from "../utilities.js";

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
  }

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    console.log(`[AMRepoKeyhive] this.peerId: ${peerId}`);
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
            console.debug(
              `[AMRepoKeyhive] Sending keyhive message to ${targetId}`
            );
            this.signAndSend(message);
          }
        } else {
          console.debug(
            `[AMRepoKeyhive] Sending keyhive message to ${message.targetId}`
          );
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
          } else if (message.type === "keyhive-sync-request") {
            this.sendKeyhiveSyncResponse(message);
          } else if (message.type === "keyhive-sync-response") {
            this.sendKeyhiveSyncOps(message);
          } else if (message.type === "keyhive-sync-ops") {
            this.receiveKeyhiveSyncOps(message);
          } else {
            this.emit("message", message);
          }
        } else {
          console.error(
            "[AMRepoKeyhive] Signed message could not be verified!"
          );
        }
      } else {
        if (message.type === "request-keyhive") {
          (this as any).emit("request-keyhive", message);
        } else {
          this.emit("message", message);
        }
      }
    } catch (e) {
      console.error("[AMRepoKeyhive] Could not decode signed message:", e);
      return;
    }
  }

  syncKeyhive(
    keyhive: Keyhive,
    maybeSenderId: PeerId | undefined = undefined
  ): void {
    void this.asyncSyncKeyhive(keyhive, maybeSenderId);
  }

  private async asyncSyncKeyhive(
    keyhive: Keyhive,
    maybeSenderId: PeerId | undefined
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    let archiveBytes: Uint8Array;
    try {
      archiveBytes = (await keyhive.toArchive()).toBytes();
      if (!archiveBytes || archiveBytes.length === 0) {
        console.error(
          "[AMRepoKeyhive] Archive serialization produced empty bytes, skipping sync"
        );
        return;
      }
    } catch (error) {
      console.error(
        "[AMRepoKeyhive] Failed to serialize keyhive archive:",
        error
      );
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

      const ops = await getMembershipOpsForPeer(this.keyhive, targetId);
      if (ops) {
        console.log(`!@ asyncSyncKeyhive: Got agent for targetId ${targetId}`);
        const opHashes = Array.from(ops.keys());
        const dataString = JSON.stringify(opHashes);
        const data = new TextEncoder().encode(dataString);
        const message = {
          type: "keyhive-sync-request",
          senderId: senderId,
          targetId: targetId,
          data: data,
        };
        console.debug(
          `Sending keyhive sync request to ${targetId} from ${senderId}`
        );
        this.send(message);
      }
    }
  }

  private sendKeyhiveSyncResponse(message: Message): void {
    void this.asyncSendKeyhiveSyncResponse(message);
  }

  private async asyncSendKeyhiveSyncResponse(message: Message): Promise<void> {
    if (!("data" in message) || !message.data) {
      console.error("[AMRepoKeyhive] Expected data in keyhive-sync-request");
      return;
    }
    if (message.type !== "keyhive-sync-request") {
      console.error(
        `[AMRepoKeyhive] Expected keyhive-sync-request, but got ${message.type}`
      );
      return;
    }
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }

    const peerOpHashes = new Set(
      JSON.parse(new TextDecoder().decode(message.data as Uint8Array))
    );
    console.debug(
      `[AMRepoKeyhive] Received keyhive sync request with ${peerOpHashes.size} operation hashes`
    );

    const ops = await getMembershipOpsForPeer(this.keyhive, message.senderId);
    if (ops) {
      console.log(
        `!@ asyncSendKeyhiveSyncResponse: Got ops for senderId ${message.senderId}`
      );
      const opHashes = new Set(Array.from(ops.keys()));
      console.debug(
        `[AMRepoKeyhive] Found ${opHashes.size} operation hashes for peer`
      );

      const hashesToSend = opHashes.difference(peerOpHashes);
      const foundOps = Array.from(hashesToSend).map((hash) => ops.get(hash));

      const responseData = {
        requested: Array.from(peerOpHashes.difference(opHashes)),
        found: foundOps,
      };

      const dataString = JSON.stringify(responseData);
      const data = new TextEncoder().encode(dataString);
      const response = {
        type: "keyhive-sync-response",
        senderId: this.peerId,
        targetId: message.senderId,
        data,
      };
      console.debug(
        `Sending keyhive sync response to ${message.senderId} from ${this.peerId}`
      );
      this.send(response);
    }
  }

  private sendKeyhiveSyncOps(message: Message): void {
    void this.asyncSendKeyhiveSyncOps(message);
  }

  private async asyncSendKeyhiveSyncOps(message: Message): Promise<void> {
    if (!("data" in message) || !message.data) {
      console.error("[AMRepoKeyhive] Expected data in keyhive-sync-response");
      return;
    }
    if (message.type !== "keyhive-sync-response") {
      console.error(
        `[AMRepoKeyhive] Expected keyhive-sync-response, but got ${message.type}`
      );
      return;
    }
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }

    const responseData = JSON.parse(
      new TextDecoder().decode(message.data as Uint8Array)
    );
    const requestedHashes: Uint8Array[] = responseData.requested || [];
    const foundEvents: Uint8Array[] = responseData.found || [];

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync response: ${foundEvents.length} ops found, ${requestedHashes.length} ops requested`
    );

    if (foundEvents.length > 0) {
      console.debug(
        `[AMRepoKeyhive] Ingesting ${foundEvents.length} keyhive events from ${message.senderId}`
      );
      await this.keyhive.ingestEventsBytes(foundEvents);
    }

    if (requestedHashes.length > 0) {
      const ops = await getMembershipOpsForPeer(this.keyhive, message.senderId);
      if (ops) {
        console.log(
          `!@ asyncSendKeyhiveSyncOps: Got ops for senderId ${message.senderId}`
        );
        const requestedOps = requestedHashes
          .map((hash) => ops.get(hash))
          .filter((op) => op !== undefined);

        if (requestedOps.length < requestedHashes.length) {
          console.warn(
            `[AMRepoKeyhive] ${requestedHashes.length} keyhive events requested, ${requestedOps.length} found`
          );
          if (requestedOps.length === 0) {
            return;
          }
        }

        console.debug(
          `[AMRepoKeyhive] Sending ${requestedOps.length} requested ops to ${message.senderId}`
        );

        const data = new TextEncoder().encode(JSON.stringify(requestedOps));
        const response = {
          type: "keyhive-sync-ops",
          senderId: this.peerId,
          targetId: message.senderId,
          data,
        };
        this.send(response);
      }
    }
  }

  private receiveKeyhiveSyncOps(message: Message): void {
    void this.asyncReceiveKeyhiveSyncOps(message);
  }

  private async asyncReceiveKeyhiveSyncOps(message: Message): Promise<void> {
    if (!("data" in message) || !message.data) {
      console.error("[AMRepoKeyhive] Expected data in keyhive-sync-ops");
      return;
    }
    if (message.type !== "keyhive-sync-ops") {
      console.error(
        `[AMRepoKeyhive] Expected keyhive-sync-ops, but got ${message.type}`
      );
      return;
    }
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }

    const receivedEvents = JSON.parse(
      new TextDecoder().decode(message.data as Uint8Array)
    );

    console.debug(
      `[AMRepoKeyhive] Received ${receivedEvents.length} keyhive events`
    );

    if (receivedEvents.length > 0) {
      console.debug(
        `[AMRepoKeyhive] Ingesting ${receivedEvents.length} keyhive events from ${message.senderId}`
      );
      await this.keyhive.ingestEventsBytes(receivedEvents);
    }
  }

  // private sendKeyhive(senderId: PeerId, targetId: PeerId, archiveBytes: Uint8Array): void {
  //   const message = {
  //     type: "keyhive",
  //     senderId: senderId,
  //     targetId: targetId,
  //     data: archiveBytes,
  //   };
  //   this.send(message);
  // }

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
