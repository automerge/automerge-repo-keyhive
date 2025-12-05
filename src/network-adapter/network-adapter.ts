import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim";
import { ContactCard, Keyhive } from "@keyhive/keyhive/slim";
import { encode, decode } from "cbor-x";

import {
  decodeKeyhiveMessageData,
  KeyhiveMessageData,
  signData,
  verifyData,
} from "./messages.js";
import { Pending } from "./pending.js";
import { getEventsForPeerPair } from "../utilities.js";
import {
  getPendingOpHashes,
  KeyhiveStorage,
  receiveContactCard,
} from "../keyhive/keyhive.js";

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending();
  private peers: Set<PeerId> = new Set();

  constructor(
    private networkAdapter: NetworkAdapter,
    private contactCard: ContactCard,
    private keyhive: Keyhive,
    private keyhiveStorage: KeyhiveStorage,
    // TODO: Replace with dynamic configuration
    private hardcodedRemoteId: PeerId | null = null
  ) {
    super();

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

  send(message: Message, contactCard?: ContactCard): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    void this.asyncSignAndSend(message, contactCard);
  }

  async asyncSignAndSend(
    message: Message,
    contactCard?: ContactCard
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    const data: Uint8Array =
      "data" in message && message.data !== undefined
        ? message.data
        : new Uint8Array();
    const seqNumber = this.pending.register();
    const signedData = await signData(this.keyhive, data, contactCard);
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
      receiveContactCard(
        this.keyhive,
        keyhiveMessageData.contactCard,
        this.keyhiveStorage
      );
    }
    message.data = keyhiveMessageData.signed.payload;

    if (message.type === "keyhive-sync-request") {
      await this.sendKeyhiveSyncResponse(message);
    } else if (message.type === "keyhive-sync-response") {
      await this.sendKeyhiveSyncOps(message);
    } else if (message.type === "keyhive-sync-request-contact-card") {
      await this.sendKeyhiveSyncMissingContactCard(message);
    } else if (message.type === "keyhive-sync-missing-contact-card") {
      await this.syncKeyhive(message.senderId, true);
    } else if (message.type === "keyhive-sync-ops") {
      await this.receiveKeyhiveSyncOps(message);
    } else {
      this.emit("message", message);
    }
  }

  syncKeyhive(
    maybeSenderId: PeerId | undefined = undefined,
    includeContactCard: boolean = false,
    attemptRecovery: boolean = false
  ): void {
    void this.asyncSyncKeyhive(
      maybeSenderId,
      includeContactCard,
      attemptRecovery
    );
  }

  private async asyncSyncKeyhive(
    maybeSenderId: PeerId | undefined,
    includeContactCard: boolean,
    attemptRecovery: boolean = false
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    if (attemptRecovery) {
      console.debug(
        "[AMRepoKeyhive] Preparing for keyhive sync. Reading from storage"
      );
      try {
        await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
      } catch (error) {
        console.error(`Unable to ingest from storage: ${error}`);
      }
    }
    let archiveBytes: Uint8Array;
    try {
      archiveBytes = (await this.keyhive.toArchive()).toBytes();
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

    // Get contact card once for all peers if needed, to avoid multiple rotations
    let contactCard: ContactCard | undefined;
    if (includeContactCard) {
      contactCard = this.contactCard;
    }

    for (const targetId of this.peers) {
      if (targetId == senderId) {
        continue;
      }

      const ops = await getEventsForPeerPair(this.keyhive, senderId, targetId);
      if (ops) {
        const opHashes = Array.from(ops.keys());
        let pendingOpHashes = await getPendingOpHashes(this.keyhive);
        const data = encode({
          found: opHashes,
          pending: pendingOpHashes,
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
        this.send(message, contactCard);
      } else {
        console.debug(`Requesting ContactCard from ${senderId}`);
        if (!contactCard) {
          contactCard = this.contactCard;
        }
        const message = {
          type: "keyhive-sync-request-contact-card",
          senderId: senderId,
          targetId: targetId,
        };
        this.send(message, contactCard);
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

    const requestData = decode(message.data as Uint8Array);
    const peerFoundHashes: Uint8Array[] = requestData.found || [];
    const peerPendingHashes: Uint8Array[] = requestData.pending || [];

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync request from ${message.senderId} with ${peerFoundHashes.length} found hashes, ${peerPendingHashes.length} pending hashes`
    );

    const ops = await getEventsForPeerPair(
      this.keyhive,
      this.peerId,
      message.senderId
    );
    if (ops) {
      const pendingOpHashes = await getPendingOpHashes(this.keyhive);
      console.debug(
        `[AMRepoKeyhive] asyncSendKeyhiveSyncResponse: Found ${ops.size} total local operation hashes for ${message.senderId} and ${pendingOpHashes.length} total pending hashes`
      );

      // Build maps to look up hashes again after doing set operations on strings
      const opsByHashString = new Map<string, { bytes: Uint8Array; op: any }>();
      for (const [hash, op] of ops.entries()) {
        opsByHashString.set(hash.toString(), { bytes: hash, op });
      }
      const peerFoundByHashString = new Map<string, Uint8Array>();
      for (const hash of peerFoundHashes) {
        peerFoundByHashString.set(hash.toString(), hash);
      }

      // Build sets for set operations
      const pendingHashStrings = new Set(
        pendingOpHashes.map((h) => h.toString())
      );
      const peerPendingHashStrings = new Set(
        peerPendingHashes.map((h) => h.toString())
      );
      const localHashStrings = new Set(opsByHashString.keys());
      const peerFoundHashStrings = new Set(peerFoundByHashString.keys());

      // Determine which ops we need to send to the peer
      const hashStringsToSend = localHashStrings.difference(
        peerFoundHashStrings.union(peerPendingHashStrings)
      );
      const foundOps = Array.from(hashStringsToSend)
        .map((str) => opsByHashString.get(str)?.op.toBytes())
        .filter((op) => op !== undefined);

      // Determine which ops we need to request from the peer
      const hashStringsToRequest = peerFoundHashStrings.difference(
        localHashStrings.union(pendingHashStrings)
      );
      const requested = Array.from(hashStringsToRequest)
        .map((str) => peerFoundByHashString.get(str))
        .filter((hash) => hash !== undefined);

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
      this.send(response, this.contactCard);
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

        // If there are pending events, try reading from storage (e.g., in case
        // they have already been processed by a separate tab in a browser).
        if (pendingEvents.length > 0) {
          console.warn(
            `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending. Reading from storage`
          );
          try {
            await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
            const retryPending =
              await this.keyhive.ingestEventsBytes(foundEvents);
            if (retryPending.length === 0) {
              console.log(
                `[AMRepoKeyhive] Successfully ingested all events after reading from storage`
              );
            } else {
              console.warn(
                `[AMRepoKeyhive] Still have ${retryPending.length} pending events after reading from storage`
              );
            }
          } catch (storageError) {
            console.error(
              `[AMRepoKeyhive] Failed while reading from storage:`,
              storageError
            );
          }
        }

        await this.saveReceivedEvents(foundEvents);
      } catch (error) {
        await this.handleIngestError(error, foundEvents, message.senderId);
      }
    }

    if (requestedHashes.length > 0) {
      const ops = await getEventsForPeerPair(
        this.keyhive,
        this.peerId,
        message.senderId
      );
      if (ops) {
        const hashStringToOp = new Map(
          Array.from(ops.entries()).map(([hash, op]) => [hash.toString(), op])
        );

        const requestedOps = requestedHashes
          .map((hash) => hashStringToOp.get(hash.toString())?.toBytes())
          .filter((op) => op !== undefined);

        if (requestedOps.length === 0) {
          console.debug(
            `[AMRepoKeyhive] 0 ops requested by ${message.senderId}`
          );
          return;
        }

        if (requestedOps.length < requestedHashes.length) {
          console.warn(
            `[AMRepoKeyhive] ${requestedHashes.length} keyhive events requested, ${requestedOps.length} found.`
          );
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
    this.send(response, this.contactCard);
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
        const pendingEvents =
          await this.keyhive.ingestEventsBytes(receivedEvents);
        console.debug(
          `[AMRepoKeyhive] After ingestion: ${pendingEvents.length} pending events`
        );

        // If there are pending events, try reading from storage (e.g., in case
        // they have already been processed by a separate tab in a browser).
        if (pendingEvents.length > 0) {
          console.warn(
            `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending. Reading from storage`
          );
          try {
            await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
            const retryPending =
              await this.keyhive.ingestEventsBytes(receivedEvents);
            if (retryPending.length === 0) {
              console.log(
                `[AMRepoKeyhive] Successfully ingested all events after reading from storage`
              );
            } else {
              console.warn(
                `[AMRepoKeyhive] Still have ${retryPending.length} pending events after reading from storage`
              );
            }
          } catch (storageError) {
            console.error(
              `[AMRepoKeyhive] Failed while reading from storage:`,
              storageError
            );
          }
        }

        await this.saveReceivedEvents(receivedEvents);
      } catch (error) {
        await this.handleIngestError(error, receivedEvents, message.senderId);
      }
    }
  }

  private async saveReceivedEvents(events: Uint8Array[]): Promise<void> {
    for (const event of events) {
      try {
        await this.keyhiveStorage.saveEventBytesWithHash(event);
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

  private requestKeyhiveSync(): void {
    if (this.peerId === undefined) {
      return;
    }
    let includeContactCard = false;
    let attemptRecovery = true;
    this.syncKeyhive(this.peerId, includeContactCard, attemptRecovery);
  }
}
