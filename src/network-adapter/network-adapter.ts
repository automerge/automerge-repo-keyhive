import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
  StorageAdapterInterface,
} from "@automerge/automerge-repo/slim";
import { Keyhive } from "@keyhive/keyhive/slim";
import { encode, decode } from "cbor-x";

import {
  decodeKeyhiveMessageData,
  KeyhiveMessageData,
  signData,
  verifyData,
} from "./messages.js";
import { Pending } from "./pending.js";
import { getEventsForPeer } from "../utilities.js";
import {
  ingestKeyhiveFromStorage,
  saveEventBytesWithHash,
} from "../keyhive/keyhive.js";

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
    setInterval(this.requestKeyhiveSync.bind(this), 15000);

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

  send(message: Message, includeContactCard: boolean = false): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    void this.asyncSignAndSend(message, includeContactCard);
  }

  async asyncSignAndSend(
    message: Message,
    includeContactCard: boolean
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    const data: Uint8Array =
      "data" in message && message.data !== undefined
        ? message.data
        : new Uint8Array();
    const seqNumber = this.pending.register();
    const signedData = await signData(this.keyhive, data, includeContactCard);
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
      if (!("data" in message) || message.data === undefined) {
        this.emit("message", message);
        return;
      }
      const maybeKeyhiveMessageData = decodeKeyhiveMessageData(message.data);
      if (maybeKeyhiveMessageData) {
        if (verifyData(message.senderId, maybeKeyhiveMessageData)) {
          void this.handleKeyhiveMessage(message, maybeKeyhiveMessageData);
        }
      } else {
        this.emit("message", message);
      }
    } catch (e) {
      console.error("[AMRepoKeyhive] Could not decode signed message:", e);
      return;
    }
  }

  private async handleKeyhiveMessage(
    message: Message,
    keyhiveMessageData: KeyhiveMessageData
  ) {
    if (keyhiveMessageData.contactCard) {
      const maybeAgent = await this.keyhive.getAgent(
        keyhiveMessageData.contactCard.id
      );
      if (!maybeAgent) {
        await this.keyhive.receiveContactCard(keyhiveMessageData.contactCard);
      }
    }
    message.data = keyhiveMessageData.signed.payload;
    // FIXME: We should either remove "keyhive-archive" and "keyhive-archive-request"
    // or handle them here.
    if (message.type === "keyhive-archive") {
      (this as any).emit("keyhive-archive", message);
    } else if (message.type === "request-keyhive") {
      (this as any).emit("request-keyhive", message);
    } else if (message.type === "keyhive-sync-request") {
      await this.sendKeyhiveSyncResponse(message);
    } else if (message.type === "keyhive-sync-response") {
      await this.sendKeyhiveSyncOps(message);
    } else if (message.type === "keyhive-sync-request-contact-card") {
      await this.sendKeyhiveSyncMissingContactCard(message);
    } else if (message.type === "keyhive-sync-missing-contact-card") {
      await this.syncKeyhive(this.keyhive, message.senderId, true);
    } else if (message.type === "keyhive-sync-ops") {
      await this.receiveKeyhiveSyncOps(message);
    } else {
      this.emit("message", message);
    }
  }

  syncKeyhive(
    keyhive: Keyhive,
    maybeSenderId: PeerId | undefined = undefined,
    includeContactCard: boolean = false,
    attemptRecovery: boolean = false,
  ): void {
    void this.asyncSyncKeyhive(keyhive, maybeSenderId, includeContactCard, attemptRecovery);
  }

  private async asyncSyncKeyhive(
    keyhive: Keyhive,
    maybeSenderId: PeerId | undefined,
    includeContactCard: boolean,
    attemptRecovery: boolean = false,
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    if (attemptRecovery) {
      await ingestKeyhiveFromStorage(this.keyhive, this.storage);
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

      const ops = await getEventsForPeer(this.keyhive, targetId);
      if (ops) {
        const opHashes = Array.from(ops.keys());
        let pendingOpHashesArray: Uint8Array[] = new Array();
        const pendingOps = await this.keyhive.pendingEventHashes()
        if (pendingOps) {
          pendingOpHashesArray = Array.from(pendingOps.keys()) as Uint8Array[]
        }
        const data = encode({
          found: opHashes,
          pending: pendingOpHashesArray,
        });
        const message = {
          type: "keyhive-sync-request",
          senderId: senderId,
          targetId: targetId,
          data: data,
        };
        console.debug(
          `Sending keyhive sync request to ${targetId} from ${senderId}`
        );
        this.send(message, includeContactCard);
      } else {
        const message = {
          type: "keyhive-sync-request-contact-card",
          senderId: senderId,
          targetId: targetId,
        };
        this.send(message, true);
      }
    }
  }

  private async sendKeyhiveSyncResponse(message: Message): Promise<void> {
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

    const peerOpHashes: { found: Uint8Array[], pending: Uint8Array[] } = decode(message.data as Uint8Array);
    console.debug(
      `[AMRepoKeyhive] Received keyhive sync request with ${peerOpHashes.found.length} operation hashes`
    );
    // Log peer's event hashes
    const peerHashesHex = peerOpHashes.found.map((h) =>
      Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
    );
    console.debug(`[AMRepoKeyhive] Peer's event hashes (truncated): ${JSON.stringify(peerHashesHex)}`);

    const ops = await getEventsForPeer(this.keyhive, message.senderId);
    if (ops) {
      const opHashesArray = Array.from(ops.keys());
      console.debug(
        `[AMRepoKeyhive] asyncSendKeyhiveSyncResponse: Found ${opHashesArray.length} total local operation hashes for ${message.senderId}`
      );
      // Log local event hashes from eventsForAgent
      const localHashesHex = opHashesArray.map((h) =>
        Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
      );
      console.debug(`[AMRepoKeyhive] Local eventsForAgent hashes (truncated): ${JSON.stringify(localHashesHex)}`);

      // Add in pending events to our local set to make sure we're not requesting
      // events we already have.
      let pendingOpHashesArray: Uint8Array[] = new Array();
      const pendingOps = await this.keyhive.pendingEventHashes()
      if (pendingOps) {
        pendingOpHashesArray = Array.from(pendingOps.keys()) as Uint8Array[]
      }

      // Convert Uint8Arrays to strings for value-based comparison in Set operations
      const opHashStrings = new Set(opHashesArray.map((h) => h.toString()));
      const peerOpHashStrings = new Set(
        peerOpHashes.found.map((h) => h.toString())
      );
      const peerPendingOpHashStrings = new Set(
        peerOpHashes.pending.map((h) => h.toString())
      );
      const pendingOpHashStrings = new Set(
        pendingOpHashesArray.map((h) => h.toString())
      );

      // Create maps for converting back from string to Uint8Array
      const hashStringToBytes = new Map(
        opHashesArray.map((h) => [h.toString(), h])
      );
      const peerHashStringToBytes = new Map(
        peerOpHashes.found.map((h) => [h.toString(), h])
      );

      const hashStringsToSend = opHashStrings.difference(peerOpHashStrings.union(peerPendingOpHashStrings));
      const foundOps = Array.from(hashStringsToSend)
        .map((str) => {
          const hash = hashStringToBytes.get(str);
          return hash ? ops.get(hash)?.toBytes() : undefined;
        })
        .filter((op) => op !== undefined);

      const requestedHashStrings = peerOpHashStrings.difference(opHashStrings.union(pendingOpHashStrings));
      const requested = Array.from(requestedHashStrings)
        .map((str) => peerHashStringToBytes.get(str))
        .filter((hash) => hash !== undefined);

      // Log requested hashes (ones we're asking peer to send us)
      const requestedHashesHex = requested.map((h) =>
        Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
      );
      console.debug(`[AMRepoKeyhive] Requesting these hashes from peer (truncated): ${JSON.stringify(requestedHashesHex)}`);

      console.debug(
        `Found ${foundOps.length} ops to send to and ${requested.length} ops to request from ${message.senderId}`
      );

      const responseData = {
        requested,
        found: foundOps,
      };

      const data = encode(responseData);
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
    } else {
      console.debug(
        `[AMRepoKeyhive] No agent found for ${message.senderId}, sending keyhive-sync-missing-contact-card`
      );
      const response = {
        type: "keyhive-sync-missing-contact-card",
        senderId: this.peerId,
        targetId: message.senderId,
      };
      this.send(response, true);
    }
  }

  private async sendKeyhiveSyncOps(message: Message): Promise<void> {
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

    const responseData = decode(message.data as Uint8Array);
    const requestedHashes: Uint8Array[] = responseData.requested || [];
    const foundEvents: Uint8Array[] = responseData.found || [];

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync response from ${message.senderId}: ${foundEvents.length} ops found, ${requestedHashes.length} ops requested`
    );

    if (foundEvents.length > 0) {
      console.debug(
        `[AMRepoKeyhive] Ingesting ${foundEvents.length} keyhive events from ${message.senderId}`
      );
      try {
        const pendingEvents = await this.keyhive.ingestEventsBytes(foundEvents);
        console.debug(
          `[AMRepoKeyhive] After ingestion: ${pendingEvents.length} pending events`
        );

        // If there are pending events, attempt recovery from storage
        if (pendingEvents.length > 0) {
          console.warn(
            `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending. Attempting recovery from storage.`
          );
          try {
            await ingestKeyhiveFromStorage(this.keyhive, this.storage);
            const retryPending = await this.keyhive.ingestEventsBytes(foundEvents);
            if (retryPending.length === 0) {
              console.log(
                `[AMRepoKeyhive] Successfully ingested all events after recovery from storage`
              );
            } else {
              console.warn(
                `[AMRepoKeyhive] Still have ${retryPending.length} pending events after recovery`
              );
            }
          } catch (recoveryError) {
            console.error(
              `[AMRepoKeyhive] Failed during storage recovery:`,
              recoveryError
            );
          }
        }

        // Save all received events to storage
        await this.saveReceivedEvents(foundEvents);
      } catch (error) {
        await this.handleIngestError(error, foundEvents, message.senderId);
      }
    }

    if (requestedHashes.length > 0) {
      // Log what hashes the peer is requesting from us
      const requestedHashesHex = requestedHashes.map((h) =>
        Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
      );
      console.debug(`[AMRepoKeyhive] Peer requested these hashes (truncated): ${JSON.stringify(requestedHashesHex)}`);

      const ops = await getEventsForPeer(this.keyhive, message.senderId);
      if (ops) {
        // Log what we have for this peer
        const opsHashesHex = Array.from(ops.keys()).map((h) =>
          Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
        );
        console.debug(`[AMRepoKeyhive] Our eventsForAgent for peer (truncated): ${JSON.stringify(opsHashesHex)}`);

        // Create a map from hash string to operation for value-based lookup
        const hashStringToOp = new Map(
          Array.from(ops.entries()).map(([hash, op]) => [hash.toString(), op])
        );

        const requestedOps = requestedHashes
          .map((hash) => hashStringToOp.get(hash.toString())?.toBytes())
          .filter((op) => op !== undefined);

        if (requestedOps.length < requestedHashes.length) {
          // Log which specific hashes we couldn't find
          const missingHashes = requestedHashes.filter(
            (hash) => !hashStringToOp.has(hash.toString())
          );
          const missingHashesHex = missingHashes.map((h) =>
            Array.from(h).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16)
          );
          console.warn(
            `[AMRepoKeyhive] ${requestedHashes.length} keyhive events requested, ${requestedOps.length} found. Missing hashes (truncated): ${JSON.stringify(missingHashesHex)}`
          );
          if (requestedOps.length === 0) {
            return;
          }
        }

        console.debug(
          `[AMRepoKeyhive] Sending ${requestedOps.length} requested ops to ${message.senderId}`
        );

        const data = encode(requestedOps);
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

  private async sendKeyhiveSyncMissingContactCard(
    message: Message
  ): Promise<void> {
    if (message.type !== "keyhive-sync-request-contact-card") {
      console.error(
        `[AMRepoKeyhive] Expected keyhive-sync-request-contact-card, but got ${message.type}`
      );
      return;
    }
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }

    console.debug(
      `[AMRepoKeyhive] Sending keyhive-sync-missing-contact-card to ${message.senderId}`
    );

    const response = {
      type: "keyhive-sync-missing-contact-card",
      senderId: this.peerId,
      targetId: message.senderId,
    };
    this.send(response, true);
  }

  private async receiveKeyhiveSyncOps(message: Message): Promise<void> {
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

    const receivedEvents = decode(message.data as Uint8Array);

    console.debug(
      `[AMRepoKeyhive] Received ${receivedEvents.length} keyhive events`
    );

    if (receivedEvents.length > 0) {
      console.debug(
        `[AMRepoKeyhive] Ingesting ${receivedEvents.length} keyhive events from ${message.senderId}`
      );
      try {
        const pendingEvents = await this.keyhive.ingestEventsBytes(receivedEvents);
        console.debug(
          `[AMRepoKeyhive] After ingestion: ${pendingEvents.length} pending events`
        );

        // If there are pending events, attempt recovery from storage
        if (pendingEvents.length > 0) {
          console.warn(
            `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending. Attempting recovery from storage.`
          );
          try {
            await ingestKeyhiveFromStorage(this.keyhive, this.storage);
            const retryPending = await this.keyhive.ingestEventsBytes(receivedEvents);
            if (retryPending.length === 0) {
              console.log(
                `[AMRepoKeyhive] Successfully ingested all events after recovery from storage`
              );
            } else {
              console.warn(
                `[AMRepoKeyhive] Still have ${retryPending.length} pending events after recovery`
              );
            }
          } catch (recoveryError) {
            console.error(
              `[AMRepoKeyhive] Failed during storage recovery:`,
              recoveryError
            );
          }
        }

        // Save all received events to storage
        await this.saveReceivedEvents(receivedEvents);
      } catch (error) {
        await this.handleIngestError(error, receivedEvents, message.senderId);
      }
    }
  }

  private async saveReceivedEvents(events: Uint8Array[]): Promise<void> {
    for (const event of events) {
      try {
        await saveEventBytesWithHash(event, this.storage);
      } catch (error) {
        console.error("[AMRepoKeyhive] Failed to save received event:", error);
      }
    }
    console.debug(
      `[AMRepoKeyhive] Saved ${events.length} received events to storage`
    );
  }

  private async handleIngestError(
    error: unknown,
    events: Uint8Array[],
    senderId: PeerId
  ): Promise<void> {
    // @ts-ignore
    const jsError =
      error && typeof error == "object" && "toError" in error
        ? // @ts-ignore
          error.toError()
        : error;

    const errorMessage =
      jsError instanceof Error ? jsError.message : String(jsError);

    console.error(
      `[AMRepoKeyhive] Error while ingesting events from ${senderId}: ${errorMessage}`
    );
  }

  // FIXME: syncKeyhive should probably find keyhive and peerId on its own.
  private requestKeyhiveSync(): void {
    if (this.peerId === undefined) {
      return;
    }
    let includeContactCard = false
    let attemptRecovery = true
    this.syncKeyhive(this.keyhive, this.peerId, includeContactCard, attemptRecovery);
  }
}
