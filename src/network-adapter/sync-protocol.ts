import {
  Message,
  PeerId,
} from "@automerge/automerge-repo/slim";
import { ContactCard, Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { encode, decode } from "cbor-x";
import { buildSyncResponseCbor, buildSyncOpsCbor, buildCborByteStringArray } from "./cbor-builder.js";
import { PromiseQueue } from "./pending.js";
import { Metrics } from "./metrics.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";
import { keyhiveIdentifierFromPeerId, unwrapWasmError } from "../utilities.js";
import {
  receiveContactCard,
  KeyhiveStorage,
} from "../keyhive/keyhive.js";
import type { KeyhiveMessageData } from "./messages.js";
import { Peer } from "./peer.js";
import type { EventCache } from "./event-cache.js";

export interface SyncProtocolConfig {
  archiveThreshold: number;
  retryPendingFromStorage: boolean;
  minSyncRequestInterval: number;
  minSyncResponseInterval: number;
}

export interface SyncProtocolDeps {
  keyhive: Keyhive;
  keyhiveStorage: KeyhiveStorage;
  keyhiveQueue: PromiseQueue;
  peers: Map<PeerId, Peer>;
  contactCard: ContactCard;
  cache: EventCache;
  getPeerId: () => PeerId | undefined;
  getMetrics: () => Metrics;
  send: (message: Message, contactCard?: ContactCard) => void;
  emit: (event: string) => void;
}

export class SyncProtocol {
  private lastEmittedTotalOps: bigint = 0n;
  private syncRequestQueued: boolean = false;

  private readonly keyhive: Keyhive;
  private readonly keyhiveStorage: KeyhiveStorage;
  private readonly keyhiveQueue: PromiseQueue;
  private readonly peers: Map<PeerId, Peer>;
  private readonly contactCard: ContactCard;
  private readonly cache: EventCache;
  private readonly getPeerId: () => PeerId | undefined;
  private readonly sendMessage: (message: Message, contactCard?: ContactCard) => void;
  private readonly emitEvent: (...args: any[]) => void;
  private readonly getMetrics: () => Metrics;
  private readonly archiveThreshold: number;
  private readonly retryPendingFromStorage: boolean;
  private readonly minSyncRequestInterval: number;
  private readonly minSyncResponseInterval: number;

  constructor(deps: SyncProtocolDeps, config: SyncProtocolConfig) {
    this.keyhive = deps.keyhive;
    this.keyhiveStorage = deps.keyhiveStorage;
    this.keyhiveQueue = deps.keyhiveQueue;
    this.peers = deps.peers;
    this.contactCard = deps.contactCard;
    this.cache = deps.cache;
    this.getPeerId = deps.getPeerId;
    this.sendMessage = deps.send;
    this.emitEvent = deps.emit;
    this.getMetrics = deps.getMetrics;

    this.archiveThreshold = config.archiveThreshold;
    this.retryPendingFromStorage = config.retryPendingFromStorage;
    this.minSyncRequestInterval = config.minSyncRequestInterval;
    this.minSyncResponseInterval = config.minSyncResponseInterval;
  }

  private requirePeerId(): PeerId {
    const peerId = this.getPeerId();
    if (peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    return peerId;
  }

  async handleKeyhiveMessage(
    message: Message,
    keyhiveMessageData: KeyhiveMessageData,
    metrics: Metrics,
  ): Promise<boolean> {
    if (keyhiveMessageData.contactCard) {
      const contactCard = keyhiveMessageData.contactCard;
      await this.keyhiveQueue.run(() =>
        receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage)
      );
    }
    message.data = keyhiveMessageData.signed.payload;

    if (message.type === "keyhive-sync-request") {
      await this.sendKeyhiveSyncResponse(message, metrics);
      return true;
    } else if (message.type === "keyhive-sync-response") {
      await this.sendKeyhiveSyncOps(message, metrics);
      return true;
    } else if (message.type === "keyhive-sync-request-contact-card") {
      await this.sendKeyhiveSyncMissingContactCard(message);
      return true;
    } else if (message.type === "keyhive-sync-missing-contact-card") {
      await this.syncKeyhive(message.senderId, true);
      return true;
    } else if (message.type === "keyhive-sync-ops") {
      await this.receiveKeyhiveSyncOps(message, metrics);
      return true;
    } else if (message.type === "keyhive-sync-check") {
      await this.handleKeyhiveSyncCheck(message, metrics);
      return true;
    } else if (message.type === "keyhive-sync-confirmation") {
      await this.handleKeyhiveSyncConfirmation(message, metrics);
      return true;
    }
    return false;
  }

  syncKeyhive(
    maybeSenderId: PeerId | undefined = undefined,
    includeContactCard: boolean = false,
    attemptRecovery: boolean = false
  ): void {
    void this.initiateKeyhiveSync(
      maybeSenderId,
      includeContactCard,
      attemptRecovery
    ).catch((error) =>
      console.error("[AMRepoKeyhive] Sync initiation failed:", error)
    );
  }

  requestKeyhiveSync(): void {
    const peerId = this.getPeerId();
    if (peerId === undefined) {
      return;
    }
    if (this.syncRequestQueued) {
      return;
    }
    this.syncRequestQueued = true;
    void this.initiateKeyhiveSync(peerId, false, false).catch((error) =>
      console.error("[AMRepoKeyhive] Periodic sync failed:", error)
    ).finally(() => {
      this.syncRequestQueued = false;
    });
  }

  invalidateCaches(): void {
    this.cache.onKeyhiveChanged();
  }

  onPeerDisconnected(peerId: PeerId): void {
    this.cache.onPeerDisconnected(peerId);
  }

  // Trigger the keyhive op set reconciliation sync protocol. Determine the hashes
  // that are relevant for the given peer as well as any pending hashes on this
  // keyhive (any pending hash might be relevant). Then send a message to the
  // peer to begin the sync protocol.
  private async initiateKeyhiveSync(
    maybeSenderId: PeerId | undefined,
    includeContactCard: boolean,
    attemptRecovery: boolean = false
  ): Promise<void> {
    const peerId = this.requirePeerId();

    await this.keyhiveQueue.run(async () => {
      if (attemptRecovery) {
        console.debug(
          "[AMRepoKeyhive] Preparing for keyhive sync. Reading from storage"
        );
        try {
          const statsBefore = await this.keyhive.stats();
          await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
          await this.cache.onMaybeChanged(this.keyhive);
          // Emit ingest-remote if new ops were added from storage
          const statsAfter = await this.keyhive.stats();
          if (statsAfter.totalOps !== statsBefore.totalOps) {
            this.emitEvent("ingest-remote");
          }
        } catch (error) {
          console.error(`[AMRepoKeyhive] Unable to ingest from storage: ${error}`);
        }
      }
      let senderId: PeerId;
      if (maybeSenderId) {
        senderId = maybeSenderId;
      } else {
        senderId = peerId;
      }

      let maybeContactCard: ContactCard | undefined;
      if (includeContactCard) {
        console.debug("[AMRepoKeyhive] Including Contact Card in sync message.")
        maybeContactCard = this.contactCard;
      }

      console.debug(`[AMRepoKeyhive] Syncing with ${this.peers.size} peers`);
      for (const targetId of this.peers.keys()) {
        if (targetId === senderId || targetId === peerId) {
          continue;
        }
        if (!this.readyToSendKeyhiveRequest(targetId)) {
          console.debug(`[AMRepoKeyhive] Attempted to send keyhive sync request to ${targetId} too soon. Ignoring.`);
          continue;
        }

        // Check if we know the target agent
        const targetKeyhiveId = keyhiveIdentifierFromPeerId(targetId);
        const targetAgent = await this.keyhive.getAgent(targetKeyhiveId);
        if (!targetAgent) {
          console.debug(`[AMRepoKeyhive] Requesting ContactCard from ${targetId}`);
          if (!maybeContactCard) {
            maybeContactCard = this.contactCard;
          }
          const message = {
            type: "keyhive-sync-request-contact-card",
            senderId: senderId,
            targetId: targetId,
          };
          this.sendMessage(message, maybeContactCard);
        } else {
          const peer = this.peers.get(targetId);
          if (peer !== undefined && peer.syncpoint !== null) {
            // Send lightweight sync check instead of full request
            const pendingOpHashes = await this.cache.getPendingOpHashes(this.keyhive);
            const hashes = await this.getHashesForPeerPair(senderId, targetId);
            const senderTotal = hashes.size + pendingOpHashes.length;
            const data = encode({
              senderTotal,
              senderSyncpoint: peer.syncpoint,
            });
            const message = {
              type: "keyhive-sync-check",
              senderId: senderId,
              targetId: targetId,
              data: data,
            };
            console.debug(
              `[AMRepoKeyhive] Sending keyhive sync check to ${targetId} from ${senderId}: senderTotal=${senderTotal}, senderSyncpoint=${peer.syncpoint}`
            );
            this.getMetrics().recordSyncCheckSent();
            this.sendMessage(message, maybeContactCard);
          } else {
            // No syncpoint yet. Send full sync request
            const hashes = await this.getHashesForPeerPair(senderId, targetId);
            const opHashes = Array.from(hashes.values());
            const pendingOpHashes = await this.cache.getPendingOpHashes(this.keyhive);
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
              `[AMRepoKeyhive] Sending keyhive sync request to ${targetId} from ${senderId} with ${opHashes.length} local operations and ${pendingOpHashes.length} pending operations.`
            );
            this.sendMessage(message, maybeContactCard);
          }
        }
        const peer = this.peers.get(targetId);
        if (peer) {
          peer.lastKeyhiveRequestSent = Date.now();
        }
      }
    });
  }

  // Send a response to a request from a peer to initiate the keyhive op set
  // reconciliation sync protocol. Given the hashes sent by the peer, determine
  // which ops to send them. Then determine any missing ops to request from the
  // peer.
  private async sendKeyhiveSyncResponse(message: Message, metrics: Metrics): Promise<void> {
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
    const peerId = this.requirePeerId();

    const requestData = decode(message.data as Uint8Array);
    const peerFoundHashes: Uint8Array[] = requestData.found || [];
    const peerPendingHashes: Uint8Array[] = requestData.pending || [];

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync request from ${message.senderId} with ${peerFoundHashes.length} found hashes, ${peerPendingHashes.length} pending hashes`
    );

    const queueEnterTime = Date.now();
    await this.keyhiveQueue.run(async () => {
      metrics.recordQueueWait(Date.now() - queueEnterTime);
      if (!this.readyToSendKeyhiveResponse(message.senderId)) {
        console.debug(`[AMRepoKeyhive] Received next keyhive sync request too soon from ${message.senderId}. Ignoring.`);
        return;
      }

      // Check if we know the sender agent
      const senderKeyhiveId = keyhiveIdentifierFromPeerId(message.senderId);
      const senderAgent = await this.keyhive.getAgent(senderKeyhiveId);
      if (!senderAgent) {
        console.debug(
          `[AMRepoKeyhive] No agent found for ${message.senderId}, sending keyhive-sync-request-contact-card`
        );
        const response = {
          type: "keyhive-sync-request-contact-card",
          senderId: peerId,
          targetId: message.senderId,
        };
        this.sendMessage(response, this.contactCard);
      } else {
        const localHashes = await this.getHashesForPeerPair(peerId, message.senderId, metrics);
        const pendingOpHashes = await this.cache.getPendingOpHashes(this.keyhive, metrics);
        console.debug(
          `[AMRepoKeyhive] asyncSendKeyhiveSyncResponse: Found ${localHashes.size} total local operation hashes for ${message.senderId} and ${pendingOpHashes.length} total pending hashes`
        );

        // Build map to look up peer hashes by string
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
        const localHashStrings = new Set(localHashes.keys());
        const peerFoundHashStrings = new Set(peerFoundByHashString.keys());

        // Determine which ops we need to send to the peer
        const hashStringsToSend = localHashStrings.difference(
          peerFoundHashStrings.union(peerPendingHashStrings)
        );

        // Determine which ops we need to request from the peer
        const hashStringsToRequest = peerFoundHashStrings.difference(
          localHashStrings.union(pendingHashStrings)
        );
        const requested = Array.from(hashStringsToRequest)
          .map((str) => peerFoundByHashString.get(str))
          .filter((hash) => hash !== undefined);

        let foundResult: EventBytesResult = { events: [], cborEvents: [] };
        if (hashStringsToSend.size > 0) {
          foundResult = await this.cache.getEventBytesForPeer(this.keyhive, peerId, hashStringsToSend, metrics);
        }

        metrics.recordOpsSent(foundResult.events.length);
        metrics.recordOpsRequested(requested.length);

        console.debug(
          `[AMRepoKeyhive] Found ${foundResult.events.length} ops to send to and ${requested.length} ops to request from ${message.senderId}`
        );

        // Metadata for sync shortcut protocol
        const syncResponderTotal = localHashes.size + pendingOpHashes.length;
        const syncRequesterTotal = peerFoundHashes.length + peerPendingHashes.length;
        const data = buildSyncResponseCbor(requested, foundResult.cborEvents, syncResponderTotal, syncRequesterTotal);
        const response = {
          type: "keyhive-sync-response",
          senderId: peerId,
          targetId: message.senderId,
          data,
        };
        console.debug(
          `[AMRepoKeyhive] Sending keyhive sync response to ${message.senderId} from ${peerId}`
        );
        this.sendMessage(response);
      }
      const peer = this.peers.get(message.senderId);
      if (peer) {
        peer.lastKeyhiveRequestRcvd = Date.now();
      }
    });
  }

  // Send requested ops in response to a keyhive sync response. Look up ops
  // for the requested hashes and send them to the requesting peer.
  private async sendKeyhiveSyncOps(message: Message, metrics: Metrics): Promise<void> {
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
    const peerId = this.requirePeerId();

    const responseData = decode(message.data as Uint8Array);
    const requestedHashes: Uint8Array[] = responseData.requested || [];
    const foundEvents: Uint8Array[] = responseData.found || [];
    const syncResponderTotal: number | undefined = responseData.syncResponderTotal;
    const syncRequesterTotal: number | undefined = responseData.syncRequesterTotal;

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync response from ${message.senderId}: ${foundEvents.length} ops found, ${requestedHashes.length} ops requested`
    );

    const queueEnterTime = Date.now();
    await this.keyhiveQueue.run(async () => {
      metrics.recordQueueWait(Date.now() - queueEnterTime);
      if (foundEvents.length > 0) {
        await this.ingestAndRetry(foundEvents, message.senderId, metrics);
      }

      if (requestedHashes.length > 0) {
        const requestedHashStrings = new Set(
          requestedHashes.map((h) => h.toString())
        );
        const requestedResult = await this.cache.getEventBytesForPeer(this.keyhive, peerId, requestedHashStrings, metrics);

        if (requestedResult.events.length === 0) {
          console.debug(
            `[AMRepoKeyhive] 0 ops requested by ${message.senderId}`
          );
          // Fall through to confirmation below
        } else {
          if (requestedResult.events.length < requestedHashes.length) {
            console.warn(
              `[AMRepoKeyhive] ${requestedHashes.length} keyhive events requested, ${requestedResult.events.length} found.`
            );
          }

          metrics.recordOpsSent(requestedResult.events.length);

          console.debug(
            `[AMRepoKeyhive] Sending ${requestedResult.events.length} requested ops to ${message.senderId}`
          );

          const data = (syncResponderTotal !== undefined && syncRequesterTotal !== undefined)
            ? buildSyncOpsCbor(requestedResult.cborEvents, syncResponderTotal, syncRequesterTotal)
            : buildCborByteStringArray(requestedResult.cborEvents);
          this.sendMessage({
            type: "keyhive-sync-ops",
            senderId: peerId,
            targetId: message.senderId,
            data,
          });
          return;
        }
      }

      // No ops exchanged (or 0 found for requested). Send confirmation and establish syncpoint
      if (syncResponderTotal !== undefined && syncRequesterTotal !== undefined) {
        const peer = this.peers.get(message.senderId);
        if (peer) {
          peer.syncpoint = syncResponderTotal;
        }

        const confirmData = encode({
          confirmerTotal: syncRequesterTotal,
        });
        const confirmMsg = {
          type: "keyhive-sync-confirmation",
          senderId: peerId,
          targetId: message.senderId,
          data: confirmData,
        };
        metrics.recordSyncConfirmationSent();
        this.sendMessage(confirmMsg);
      }
    });
  }

  // In response to a message from a peer indicating they are missing our contact
  // card, send it along. This response will trigger a keyhive op sync.
  private async sendKeyhiveSyncMissingContactCard(
    message: Message
  ): Promise<void> {
    if (message.type !== "keyhive-sync-request-contact-card") {
      console.error(
        `[AMRepoKeyhive] Expected keyhive-sync-request-contact-card, but got ${message.type}`
      );
      return;
    }
    const peerId = this.requirePeerId();

    console.debug(
      `[AMRepoKeyhive] Sending keyhive-sync-missing-contact-card to ${message.senderId}`
    );

    const response = {
      type: "keyhive-sync-missing-contact-card",
      senderId: peerId,
      targetId: message.senderId,
    };
    this.sendMessage(response, this.contactCard);
  }

  // Receive ops sent by a peer.
  private async receiveKeyhiveSyncOps(message: Message, metrics: Metrics): Promise<void> {
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
    const peerId = this.requirePeerId();

    const decoded = decode(message.data as Uint8Array);

    // Handle both old array format and new map format with metadata
    let receivedEvents: Uint8Array[];
    let syncResponderTotal: number | undefined;
    let syncRequesterTotal: number | undefined;
    if (Array.isArray(decoded)) {
      receivedEvents = decoded;
    } else {
      receivedEvents = decoded.ops || [];
      syncResponderTotal = decoded.syncResponderTotal;
      syncRequesterTotal = decoded.syncRequesterTotal;
    }

    console.debug(
      `[AMRepoKeyhive] Received ${receivedEvents.length} keyhive events`
    );

    const queueEnterTime = Date.now();
    await this.keyhiveQueue.run(async () => {
      metrics.recordQueueWait(Date.now() - queueEnterTime);
      if (receivedEvents.length > 0) {
        const ingestionSucceeded = await this.ingestAndRetry(receivedEvents, message.senderId, metrics);

        // After successful ingestion, send confirmation and establish syncpoint
        if (ingestionSucceeded && syncResponderTotal !== undefined && syncRequesterTotal !== undefined) {
          const peer = this.peers.get(message.senderId);
          if (peer) {
            // syncRequesterTotal is the remote peer's total
            peer.syncpoint = syncRequesterTotal;
          }
          const confirmData = encode({
            confirmerTotal: syncResponderTotal,
          });
          const confirmMsg = {
            type: "keyhive-sync-confirmation",
            senderId: peerId,
            targetId: message.senderId,
            data: confirmData,
          };
          metrics.recordSyncConfirmationSent();
          this.sendMessage(confirmMsg);
        }
      }
    });
  }

  // Handle a lightweight sync check message. If counts match our syncpoints,
  // no sync is needed. Otherwise, fall back to a full sync request.
  private async handleKeyhiveSyncCheck(
    message: Message,
    metrics: Metrics,
  ): Promise<void> {
    if (!("data" in message) || !message.data) {
      console.error("[AMRepoKeyhive] Expected data in keyhive-sync-check");
      return;
    }
    const peerId = this.requirePeerId();

    const checkData = decode(message.data as Uint8Array);
    const theirTotalForUs: number = checkData.senderTotal;
    const theirSyncpoint: number = checkData.senderSyncpoint;

    metrics.recordSyncCheckReceived();

    const queueEnterTime = Date.now();
    await this.keyhiveQueue.run(async () => {
      metrics.recordQueueWait(Date.now() - queueEnterTime);

      let peer = this.peers.get(message.senderId);
      if (!peer) {
        // Auto-register the peer if we receive a sync check from an unknown
        // sender.
        console.debug(
          `[AMRepoKeyhive] Auto-registering peer from sync-check: ${message.senderId}`
        );
        peer = new Peer();
        this.peers.set(message.senderId, peer);
      }

      // Compute our actual total for the sender
      const pendingOpHashes = await this.cache.getPendingOpHashes(this.keyhive);
      const hashes = await this.getHashesForPeerPair(peerId, message.senderId);
      const ourTotalForThem = hashes.size + pendingOpHashes.length;

      // Sync check conditions
      const ourSyncpointMatchesTheirTotal = peer.syncpoint !== null &&
        peer.syncpoint === theirTotalForUs;
      const theirSyncpointMatchesOurTotal = theirSyncpoint === ourTotalForThem;

      if (ourSyncpointMatchesTheirTotal && theirSyncpointMatchesOurTotal) {
        console.debug(
          `[AMRepoKeyhive] Sync check passed for ${message.senderId}: both totals match (ours=${ourTotalForThem}, theirs=${theirTotalForUs})`
        );
        metrics.recordSyncCheckShortCircuited();
        return;
      }

      // Totals mismatch. Start full sync request
      console.debug(
        `[AMRepoKeyhive] Sync check failed for ${message.senderId}: mismatch (ourActual=${ourTotalForThem}, theirSyncpoint=${theirSyncpoint}, theirTotalForUs=${theirTotalForUs}, ourSyncpoint=${peer.syncpoint ?? "null"}). Falling back to full sync.`
      );
      metrics.recordSyncCheckFallback();

      const opHashes = Array.from(hashes.values());
      const data = encode({
        found: opHashes,
        pending: pendingOpHashes,
      });
      const request = {
        type: "keyhive-sync-request",
        senderId: peerId,
        targetId: message.senderId,
        data: data,
      };
      this.sendMessage(request);
      peer.lastKeyhiveRequestSent = Date.now();
    });
  }

  // Handle a sync confirmation message. Update our syncpoint for the sender.
  private async handleKeyhiveSyncConfirmation(
    message: Message,
    metrics: Metrics,
  ): Promise<void> {
    if (!("data" in message) || !message.data) {
      console.error("[AMRepoKeyhive] Expected data in keyhive-sync-confirmation");
      return;
    }

    const confirmData = decode(message.data as Uint8Array);
    const confirmerTotal: number = confirmData.confirmerTotal;

    metrics.recordSyncConfirmationReceived();

    const peer = this.peers.get(message.senderId);
    if (peer) {
      peer.syncpoint = confirmerTotal;
      console.debug(
        `[AMRepoKeyhive] Updated syncpoint for ${message.senderId}: ${confirmerTotal}`
      );
    }
  }

  // Returns true if ingestion succeeded (even if some events are still pending).
  // Returns false if ingestion threw an unrecoverable error.
  private async ingestAndRetry(events: Uint8Array[], senderId: PeerId, metrics: Metrics): Promise<boolean> {
    console.debug(
      `[AMRepoKeyhive] Ingesting ${events.length} keyhive events from ${senderId}`
    );

    try {
      let pendingEvents: Uint8Array[] | null = null;
      try {
        pendingEvents = await this.keyhiveStorage.withSuppressedEventWrites(() =>
          this.keyhive.ingestEventsBytes(events)
        );
      } catch (error) {
        console.error(`[AMRepoKeyhive] Error ingesting events: ${error}`);
      }

      if (pendingEvents) {
        metrics.recordIngestion(events.length, pendingEvents.length);
        console.debug(
          `[AMRepoKeyhive] After ingestion: ${pendingEvents.length} pending events`
        );
      }

      // If there are pending events or something went wrong ingesting, try
      // reading from storage (e.g., in case they have already been processed
      // by a separate tab in a browser).
      if (!pendingEvents || pendingEvents.length > 0) {
        if (pendingEvents) {
          console.warn(
            `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending${this.retryPendingFromStorage ? ". Reading from storage" : ""}`
          );
        }
        if (this.retryPendingFromStorage) {
          metrics.recordStorageRetry();
          try {
            await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
            const retryPending = await this.keyhiveStorage.withSuppressedEventWrites(() =>
              this.keyhive.ingestEventsBytes(events)
            );
            if (retryPending.length === 0) {
              console.debug(
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
      }

      // For large batches, write the full archive instead of individual events.
      if (events.length > this.archiveThreshold) {
        console.debug(
          `[AMRepoKeyhive] Large batch (${events.length} > ${this.archiveThreshold}): saving full archive instead of individual events`
        );
        void this.keyhiveStorage.saveKeyhiveWithHash(this.keyhive).catch((error) =>
          console.error("[AMRepoKeyhive] Failed to save archive after large batch:", error)
        );
      } else {
        void this.saveReceivedEvents(events).catch((error) =>
          console.error("[AMRepoKeyhive] Failed to save received events:", error)
        );
      }
      this.cache.onKeyhiveChanged();
      const statsAfterIngest = await this.keyhive.stats();
      if (statsAfterIngest.totalOps !== this.lastEmittedTotalOps) {
        this.lastEmittedTotalOps = statsAfterIngest.totalOps;
        this.invalidateSyncpoints();
        this.emitEvent("ingest-remote");
      }
      return true;
    } catch (error) {
      await this.handleIngestError(error, events, senderId);
      return false;
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
    _events: Uint8Array[],
    senderId: PeerId
  ): Promise<void> {
    const jsError = unwrapWasmError(error);
    const errorMessage =
      jsError instanceof Error ? jsError.message : String(jsError);

    console.error(
      `[AMRepoKeyhive] Error while ingesting events from ${senderId}: ${errorMessage}`
    );
  }

  private readyToSendKeyhiveRequest(targetId: PeerId): boolean {
    const last = this.peers.get(targetId)?.lastKeyhiveRequestSent;
    if (!last) return true;
    return (Date.now() - last) > this.minSyncRequestInterval;
  }

  private readyToSendKeyhiveResponse(senderId: PeerId): boolean {
    const last = this.peers.get(senderId)?.lastKeyhiveRequestRcvd;
    if (!last) return true;
    return (Date.now() - last) > this.minSyncResponseInterval;
  }

  private invalidateSyncpoints(): void {
    for (const peer of this.peers.values()) {
      peer.syncpoint = null;
    }
  }

  // Returns intersection of hashes both peers can access, plus public hashes.
  private async getHashesForPeerPair(
    peerA: PeerId,
    peerB: PeerId,
    metrics?: Metrics,
  ): Promise<PeerHashes> {
    const hashLookupStart = Date.now();
    const hashesForA = await this.cache.getHashesForPeer(this.keyhive, peerA, metrics) ?? new Map<string, Uint8Array>();
    const hashesForB = await this.cache.getHashesForPeer(this.keyhive, peerB, metrics) ?? new Map<string, Uint8Array>();

    const publicHashes = await this.cache.getPublicHashes(this.keyhive, metrics);
    metrics?.recordHashLookupTime(Date.now() - hashLookupStart);

    const result = new Map<string, Uint8Array>(publicHashes);
    for (const [hashString, hashBytes] of hashesForA.entries()) {
      if (hashesForB.has(hashString)) {
        result.set(hashString, hashBytes);
      }
    }

    return result;
  }
}
