import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim";
import { ContactCard, Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { encode, decode } from "cbor-x";
import { cborByteString, buildSyncResponseCbor, buildSyncOpsCbor, buildCborByteStringArray } from "./cbor-builder.js";
import {
  decodeKeyhiveMessageData,
  KeyhiveMessageData,
  signData,
  verifyData,
} from "./messages.js";
import { PromiseQueue, Pending } from "./pending.js";
import { OpCache } from "./op-cache.js";
import { Metrics } from "./metrics.js";
import { MessageBatch, BatchProcessor } from "./batch.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";
import { getEventsForAgent, getEventHashesForAgent, keyhiveIdentifierFromPeerId, unwrapWasmError } from "../utilities.js";
import {
  getPendingOpHashes,
  KeyhiveStorage,
  receiveContactCard,
} from "../keyhive/keyhive.js";

class Peer {
  lastKeyhiveRequestRcvd = Date.now();
  lastKeyhiveRequestSent = Date.now();
  // The remote peer's hash count (for our shared peer pair) at the last sync.
  // null before first full sync is completed
  syncpoint: number | null = null;
}

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending();
  private peers: Map<PeerId, Peer> = new Map();
  private syncIntervalId?: ReturnType<typeof setInterval> | undefined;
  private compactionIntervalId?: ReturnType<typeof setInterval>;
  private batchProcessor?: BatchProcessor;

  private cachingMode: "none" | "standard" | "periodic";
  private hashesCache: Map<PeerId, PeerHashes> = new Map();
  private publicHashesCache: PeerHashes | null = null;
  private publicEventsCache: Map<Uint8Array, Uint8Array> | null = null;
  private pendingOpHashesCache: Uint8Array[] | null = null;
  private lastKnownTotalOps: bigint = 0n;
  // Persistent cache for immutable event data (events never change once created)
  private eventBytesCache: Map<string, Uint8Array> = new Map();
  private eventCborBytesCache: Map<string, Uint8Array> = new Map();
  private static readonly MAX_CACHED_EVENTS = 10000;

  // Periodic op cache (only used when cachingMode="periodic")
  private opCache: OpCache | null = null;
  private opCacheRefreshId?: ReturnType<typeof setInterval>;

  private syncRequestQueued: boolean = false;
  private minSyncRequestInterval: number = 1000;
  private minSyncResponseInterval: number = 1000;

  private batchInterval: number | undefined;
  private keyhiveMsgBatch: MessageBatch;
  private streamingMetrics = new Metrics();
  private metricsIntervalId?: ReturnType<typeof setInterval>;

  constructor(
    private networkAdapter: NetworkAdapter,
    private contactCard: ContactCard,
    private keyhive: Keyhive,
    private keyhiveStorage: KeyhiveStorage,
    private keyhiveQueue: PromiseQueue,
    periodicallyRequestSync: boolean,
    cachingMode: "none" | "standard" | "periodic" = "none",
    // TODO: Replace with dynamic configuration
    private hardcodedRemoteId: PeerId | null = null,
    private syncRequestInterval: number,
    batchInterval?: number,
    private retryPendingFromStorage: boolean = true,
    enableCompaction: boolean = true,
    private archiveThreshold: number = 200,
  ) {
    super();
    this.cachingMode = cachingMode;

    if (cachingMode === "periodic") {
      this.opCache = new OpCache();
      // Periodic refresh at the same interval as sync requests
      this.opCacheRefreshId = setInterval(() => {
        void this.keyhiveQueue.run(() => this.opCache!.refresh(this.keyhive)).catch((error) =>
          console.error("[AMRepoKeyhive] OpCache refresh failed:", error)
        );
      }, syncRequestInterval);
      // Initial refresh
      void this.keyhiveQueue.run(() => this.opCache!.refresh(this.keyhive)).catch((error) =>
        console.error("[AMRepoKeyhive] Initial OpCache refresh failed:", error)
      );
    }

    if (periodicallyRequestSync) {
        this.syncIntervalId = setInterval(this.requestKeyhiveSync.bind(this), syncRequestInterval);
    }

    if (enableCompaction) {
      this.compactionIntervalId = setInterval(
        this.runCompaction.bind(this),
        60000
      );
    }

    networkAdapter.on("message", (msg) => {
      this.receiveMessage(msg);
    });

    networkAdapter.on("peer-candidate", (payload) => {
      if (this.peerId && payload.peerId == this.peerId) {
        console.warn(`[AMRepoKeyhive] Received peer-candidate msg with our own peerID`);
        return;
      }
      console.debug(`[AMRepoKeyhive] peer-candidate: ${payload.peerId}`);
      this.emit("peer-candidate", payload);
      this.peers.set(payload.peerId, new Peer());
    });

    networkAdapter.on("peer-disconnected", (payload) => {
      this.emit("peer-disconnected", payload);
      this.peers.delete(payload.peerId);
      if (!this.opCache) {
        this.hashesCache.delete(payload.peerId);
      }
    });

    this.keyhiveMsgBatch = new MessageBatch();

    this.batchInterval = batchInterval;
    if (this.isBatching()) {
      this.batchProcessor = new BatchProcessor(
        this.batchInterval!,
        this.keyhive,
        this.handleKeyhiveMessage.bind(this),
        () => {
          const old = this.keyhiveMsgBatch;
          this.keyhiveMsgBatch = new MessageBatch();
          return old;
        },
      );
      this.batchProcessor.start();
    } else {
      this.metricsIntervalId = setInterval(async () => {
        const stats = await this.keyhive.stats();
        this.streamingMetrics.recordTotalOps(stats.totalOps);
        this.streamingMetrics.logReport("Streaming");
        this.streamingMetrics = new Metrics();
      }, 1000);
    }
  }

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    console.log(`[AMRepoKeyhive] connect: peerId=${peerId}`);
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

  isBatching(): boolean {
    return this.batchInterval !== undefined
  }

  disconnect(): void {
    if (this.syncIntervalId) {
      clearInterval(this.syncIntervalId);
      this.syncIntervalId = undefined;
    }
    if (this.compactionIntervalId) {
      clearInterval(this.compactionIntervalId);
      this.compactionIntervalId = undefined;
    }
    if (this.batchProcessor) {
      this.batchProcessor.stop();
      this.batchProcessor = undefined;
    }
    if (this.opCacheRefreshId) {
      clearInterval(this.opCacheRefreshId);
      this.opCacheRefreshId = undefined;
    }
    if (this.metricsIntervalId) {
      clearInterval(this.metricsIntervalId);
      this.metricsIntervalId = undefined;
    }
    this.networkAdapter.disconnect();
  }

  send(message: Message, contactCard?: ContactCard): void {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    void this.signAndSend(message, contactCard).catch((error) =>
      console.error(`[AMRepoKeyhive] Failed to sign and send (type=${message.type}):`, error)
    );
  }

  async signAndSend(
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
    try {
      const signedData = await this.keyhiveQueue.run(() =>
        signData(this.keyhive, data, contactCard)
      );
      await this.networkAdapter.whenReady();
      this.pending.fire(seqNumber, () => {
        message.data = signedData;
        this.networkAdapter.send(message);
      });
    } catch (error) {
      console.error(
        `[AMRepoKeyhive] asyncSignAndSend FAILED for seq=${seqNumber}, type=${message.type}:`,
        error
      );
      this.pending.cancel(seqNumber);
    }
  }

  receiveMessage(message: Message): void {
    try {
      if (
        this.hardcodedRemoteId &&
        message.senderId !== this.hardcodedRemoteId
      ) {
        console.debug(
          `[AMRepoKeyhive] Unknown remote peer ${message.senderId}. Ignoring message!`
        );
        return;
      }
      if (!("data" in message) || message.data === undefined) {
        this.emit("message", message);
        return;
      }
      const maybeKeyhiveMessageData = decodeKeyhiveMessageData(message.data);
      if (maybeKeyhiveMessageData) {
        if (verifyData(message.senderId, maybeKeyhiveMessageData)) {
          if (!message.type?.startsWith("keyhive-")) {
            if (this.isBatching()) {
              this.keyhiveMsgBatch.countNonKeyhive();
            } else {
              this.streamingMetrics.recordNonKeyhive();
            }
            message.data = maybeKeyhiveMessageData.signed.payload;
            this.emit("message", message);
          } else if (this.isBatching()) {
            this.keyhiveMsgBatch.add(message, maybeKeyhiveMessageData);
          } else {
            this.streamingMetrics.recordMessage(
              message.type, message.senderId,
              maybeKeyhiveMessageData.signed.payload?.byteLength ?? 0,
            );
            const startTime = Date.now();
            const msgType = message.type ?? "unknown";
            void this.handleKeyhiveMessage(message, maybeKeyhiveMessageData, this.streamingMetrics).then(() => {
              this.streamingMetrics.recordProcessingTime(Date.now() - startTime);
              this.streamingMetrics.recordProcessingTimeByType(msgType, Date.now() - startTime);
            }).catch((error) =>
              console.error(`[AMRepoKeyhive] Error handling message (type=${message.type}, from=${message.senderId}):`, error)
            );
          }
        } else {
          console.error(
            `[AMRepoKeyhive] verifyData FAILED for type=${message.type} from=${message.senderId} doc=${(message as any).documentId}`
          );
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
    keyhiveMessageData: KeyhiveMessageData,
    metrics: Metrics,
  ) {
    if (keyhiveMessageData.contactCard) {
      const contactCard = keyhiveMessageData.contactCard;
      await this.keyhiveQueue.run(() =>
        receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage)
      );
    }
    message.data = keyhiveMessageData.signed.payload;

    if (message.type === "keyhive-sync-request") {
      await this.sendKeyhiveSyncResponse(message, metrics);
    } else if (message.type === "keyhive-sync-response") {
      await this.sendKeyhiveSyncOps(message, metrics);
    } else if (message.type === "keyhive-sync-request-contact-card") {
      await this.sendKeyhiveSyncMissingContactCard(message);
    } else if (message.type === "keyhive-sync-missing-contact-card") {
      await this.syncKeyhive(message.senderId, true);
    } else if (message.type === "keyhive-sync-ops") {
      await this.receiveKeyhiveSyncOps(message, metrics);
    } else if (message.type === "keyhive-sync-check") {
      await this.handleKeyhiveSyncCheck(message, metrics);
    } else if (message.type === "keyhive-sync-confirmation") {
      await this.handleKeyhiveSyncConfirmation(message, metrics);
    } else {
      this.emit("message", message);
    }
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

  // Trigger the keyhive op set reconciliation sync protocol. Determine the hashes
  // that are relevant for the given peer as well as any pending hashes on this
  // keyhive (any pending hash might be relevant). Then send a request to the
  // peer to begin the sync protocol.
  // This is the first keyhive op sync protocol message.
  private async initiateKeyhiveSync(
    maybeSenderId: PeerId | undefined,
    includeContactCard: boolean,
    attemptRecovery: boolean = false
  ): Promise<void> {
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    await this.keyhiveQueue.run(async () => {
      if (attemptRecovery) {
        console.debug(
          "[AMRepoKeyhive] Preparing for keyhive sync. Reading from storage"
        );
        try {
          const statsBefore = await this.keyhive.stats();
          await this.keyhiveStorage.ingestKeyhiveFromStorage(this.keyhive);
          // Check if ingestion changed state and invalidate cache if needed
          await this.checkAndInvalidateCache();
          // Emit ingest-remote if new ops were added from storage
          const statsAfter = await this.keyhive.stats();
          if (statsAfter.totalOps !== statsBefore.totalOps) {
            (this.emit as any)("ingest-remote");
          }
        } catch (error) {
          console.error(`[AMRepoKeyhive] Unable to ingest from storage: ${error}`);
        }
      }
      let senderId: PeerId;
      if (maybeSenderId) {
        senderId = maybeSenderId;
      } else {
        senderId = this.peerId!;
      }

      // Get contact card once for all peers if needed, to avoid multiple rotations
      let maybeContactCard: ContactCard | undefined;
      if (includeContactCard) {
        console.debug("[AMRepoKeyhive] Including Contact Card in sync message.")
        maybeContactCard = this.contactCard;
      }

      console.debug(`[AMRepoKeyhive] Syncing with ${this.peers.size} peers`);
      for (const targetId of this.peers.keys()) {
        if (targetId == senderId || targetId == this.peerId!) {
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
          this.send(message, maybeContactCard);
        } else {
          const peer = this.peers.get(targetId);
          if (peer !== undefined && peer.syncpoint !== null) {
            // Send lightweight sync check instead of full request
            const pendingOpHashes = await this.getCachedPendingOpHashes();
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
            this.streamingMetrics.recordSyncCheckSent();
            this.send(message, maybeContactCard);
          } else {
            // No syncpoint yet. Send full sync request
            const hashes = await this.getHashesForPeerPair(senderId, targetId);
            const opHashes = Array.from(hashes.values());
            const pendingOpHashes = await this.getCachedPendingOpHashes();
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
            this.send(message, maybeContactCard);
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
  // This is the second keyhive op sync protocol message.
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
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    const peerId = this.peerId;

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
          `[AMRepoKeyhive] No agent found for ${message.senderId}, sending keyhive-sync-missing-contact-card`
        );
        const response = {
          type: "keyhive-sync-request-contact-card",
          senderId: peerId,
          targetId: message.senderId,
        };
        this.send(response, this.contactCard);
      } else {
        const localHashes = await this.getHashesForPeerPair(peerId, message.senderId, metrics);
        const pendingOpHashes = await this.getCachedPendingOpHashes(metrics);
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
          foundResult = await this.getEventBytesForHashes(peerId, hashStringsToSend, metrics);
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
        this.send(response);
      }
      const peer = this.peers.get(message.senderId);
      if (peer) {
        peer.lastKeyhiveRequestRcvd = Date.now();
      }
    });
  }

  // Send requested ops in response to a keyhive sync response. Look up ops
  // for the requested hashes and send them to the requesting peer.
  // This is the third (and final) keyhive op sync protocol message.
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
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    const peerId = this.peerId;

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
        const requestedResult = await this.getEventBytesForHashes(peerId, requestedHashStrings, metrics);

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
          this.send({
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
        this.send(confirmMsg);
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

  // Receive ops sent by a peer as part of the third (and final) keyhive ops
  // sync protocol message.
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
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }

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
            senderId: this.peerId!,
            targetId: message.senderId,
            data: confirmData,
          };
          metrics.recordSyncConfirmationSent();
          this.send(confirmMsg);
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
    if (this.peerId === undefined) {
      throw new Error("peerId must be defined!");
    }
    const peerId = this.peerId;

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
      const pendingOpHashes = await this.getCachedPendingOpHashes();
      const hashes = await this.getHashesForPeerPair(peerId, message.senderId);
      const ourTotalForThem = hashes.size + pendingOpHashes.length;

      // Check both conditions
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

      // Totals mismatch. Fall back to full sync request
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
      this.send(request);
      peer.lastKeyhiveRequestRcvd = Date.now();
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
      // Invalidate/refresh cache since we ingested events from a peer
      // (OpCache relies on periodic interval refresh so no need to explicitly invalidate)
      if (!this.opCache) {
        this.invalidateCaches();
      }
      const statsAfterIngest = await this.keyhive.stats();
      if (statsAfterIngest.totalOps !== this.lastKnownTotalOps) {
        this.lastKnownTotalOps = statsAfterIngest.totalOps;
        this.invalidateSyncpoints();
        (this.emit as any)("ingest-remote");
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
    events: Uint8Array[],
    senderId: PeerId
  ): Promise<void> {
    const jsError = unwrapWasmError(error);
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
    if (this.syncRequestQueued) {
      return;
    }
    this.syncRequestQueued = true;
    void this.initiateKeyhiveSync(this.peerId, false, false).catch((error) =>
      console.error("[AMRepoKeyhive] Periodic sync failed:", error)
    ).finally(() => {
      this.syncRequestQueued = false;
    });
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

  private runCompaction(): void {
    void this.keyhiveQueue.run(async () => {
      await this.keyhiveStorage.compact(this.keyhive);
    }).catch((error) =>
      console.error("[AMRepoKeyhive] Compaction failed:", error)
    );
  }

  private invalidateCaches(): void {
    this.hashesCache.clear();
    this.publicHashesCache = null;
    this.publicEventsCache = null;
    this.pendingOpHashesCache = null;
  }

  private invalidateSyncpoints(): void {
    for (const peer of this.peers.values()) {
      peer.syncpoint = null;
    }
  }

  // Check if keyhive state changed and invalidate/refresh cache if needed
  private async checkAndInvalidateCache(): Promise<void> {
    if (this.opCache) {
      await this.opCache.refresh(this.keyhive);
      return;
    }
    const stats = await this.keyhive.stats();
    const currentTotalOps = stats.totalOps;
    if (currentTotalOps !== this.lastKnownTotalOps) {
      console.debug(
        `[AMRepoKeyhive] Total ops changed from ${this.lastKnownTotalOps} to ${currentTotalOps}, invalidating cache`
      );
      this.lastKnownTotalOps = currentTotalOps;
      this.invalidateCaches();
    }
  }

  private async getCachedPendingOpHashes(metrics?: Metrics): Promise<Uint8Array[]> {
    if (this.opCache) {
      metrics?.recordCacheHit();
      return this.opCache.getPendingOpHashes();
    }

    if (this.cachingMode === "standard" && this.pendingOpHashesCache !== null) {
      metrics?.recordCacheHit();
      return this.pendingOpHashesCache;
    }
    if (this.cachingMode === "standard") {
      metrics?.recordCacheMiss();
    }
    const hashes = await getPendingOpHashes(this.keyhive);
    if (this.cachingMode === "standard") {
      this.pendingOpHashesCache = hashes;
    }
    return hashes;
  }

  private async getCachedPublicHashes(metrics?: Metrics): Promise<PeerHashes> {
    if (this.opCache) {
      metrics?.recordCacheHit();
      return this.opCache.getPublicHashes();
    }

    if (this.cachingMode === "standard" && this.publicHashesCache !== null) {
      metrics?.recordCacheHit();
      return this.publicHashesCache;
    }
    if (this.cachingMode === "standard") {
      metrics?.recordCacheMiss();
    }
    const agent = await this.keyhive.getAgent(Identifier.publicId());
    let hashes: PeerHashes;
    if (!agent) {
      hashes = new Map();
    } else {
      hashes = await getEventHashesForAgent(this.keyhive, agent);
    }
    if (this.cachingMode === "standard") {
      this.publicHashesCache = hashes;
    }
    return hashes;
  }

  private async getCachedPublicEvents(): Promise<Map<Uint8Array, Uint8Array>> {
    if (this.cachingMode === "standard" && this.publicEventsCache !== null) {
      return this.publicEventsCache;
    }
    const agent = await this.keyhive.getAgent(Identifier.publicId());
    const events = agent
      ? await getEventsForAgent(this.keyhive, agent)
      : new Map<Uint8Array, Uint8Array>();
    if (this.cachingMode === "standard") {
      this.publicEventsCache = events;
    }
    return events;
  }

  // Get event hashes for a peer. Returns null if the peer agent is unknown.
  private async getHashesForPeer(peerId: PeerId, metrics?: Metrics): Promise<PeerHashes | null> {
    if (this.opCache) {
      const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
      const agentIdStr = keyhiveId.toBytes().toString();
      const cached = this.opCache.getHashesForAgent(agentIdStr);
      if (cached) {
        metrics?.recordCacheHit();
        return cached;
      }
      // Agent not in cache
      metrics?.recordCacheMiss();
      return null;
    }

    if (this.cachingMode === "standard") {
      const cached = this.hashesCache.get(peerId);
      if (cached) {
        metrics?.recordCacheHit();
        return cached;
      }
      metrics?.recordCacheMiss();
    }

    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await this.keyhive.getAgent(keyhiveId);
    if (!agent) {
      return null;
    }
    const hashes = await getEventHashesForAgent(this.keyhive, agent);

    if (this.cachingMode === "standard") {
      this.hashesCache.set(peerId, hashes);
    }
    return hashes;
  }

  // Returns intersection of hashes both peers can access, plus public hashes.
  private async getHashesForPeerPair(
    peerA: PeerId,
    peerB: PeerId,
    metrics?: Metrics,
  ): Promise<PeerHashes> {
    const hashLookupStart = Date.now();
    const hashesForA = await this.getHashesForPeer(peerA, metrics) ?? new Map<string, Uint8Array>();
    const hashesForB = await this.getHashesForPeer(peerB, metrics) ?? new Map<string, Uint8Array>();

    const publicHashes = await this.getCachedPublicHashes(metrics);
    metrics?.recordHashLookupTime(Date.now() - hashLookupStart);

    const result = new Map<string, Uint8Array>(publicHashes);
    for (const [hashString, hashBytes] of hashesForA.entries()) {
      if (hashesForB.has(hashString)) {
        result.set(hashString, hashBytes);
      }
    }

    return result;
  }

  // Fetch full event bytes for a set of hashes, with pre-encoded CBOR byte strings.
  // for immutable event data to avoid redundant WASM lookups.
  private async getEventBytesForHashes(
    peerId: PeerId,
    hashStrings: Set<string>,
    metrics?: Metrics,
  ): Promise<EventBytesResult> {
    const eventLookupStart = Date.now();

    if (this.opCache) {
      const cached = this.opCache.getEventBytesForHashes(hashStrings);
      if (cached) {
        metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
        return cached;
      }
      console.debug(`[AMRepoKeyhive] OpCache miss for ${hashStrings.size} hashes, falling back to WASM`);
    }

    // Check which hashes already have stored bytes and CBOR
    const events: Uint8Array[] = [];
    const cborEvents: Uint8Array[] = [];
    const missingHashes = new Set<string>();
    for (const hashStr of hashStrings) {
      const bytes = this.eventBytesCache.get(hashStr);
      const cbor = this.eventCborBytesCache.get(hashStr);
      if (bytes && cbor) {
        events.push(bytes);
        cborEvents.push(cbor);
      } else {
        missingHashes.add(hashStr);
      }
    }

    // If all requested hashes have stored bytes, skip WASM entirely
    if (missingHashes.size === 0) {
      metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
      return { events, cborEvents };
    }

    // Fetch from WASM for misses
    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await this.keyhive.getAgent(keyhiveId);

    const wasmEvents = new Map<Uint8Array, Uint8Array>();

    if (agent) {
      const peerEvents = await getEventsForAgent(this.keyhive, agent);
      for (const [hash, event] of peerEvents) {
        wasmEvents.set(hash, event);
      }
    }

    const publicEvents = await this.getCachedPublicEvents();
    for (const [hash, event] of publicEvents) {
      wasmEvents.set(hash, event);
    }

    // Store all fetched events and collect the ones we need
    for (const [hash, eventBytes] of wasmEvents.entries()) {
      const hashStr = hash.toString();
      if (!this.eventBytesCache.has(hashStr)) {
        this.eventBytesCache.set(hashStr, eventBytes);
        this.eventCborBytesCache.set(hashStr, cborByteString(eventBytes));
        // FIFO eviction: remove oldest entries when over capacity
        while (this.eventBytesCache.size > KeyhiveNetworkAdapter.MAX_CACHED_EVENTS) {
          const oldest = this.eventBytesCache.keys().next().value;
          if (oldest !== undefined) {
            this.eventBytesCache.delete(oldest);
            this.eventCborBytesCache.delete(oldest);
          }
        }
      }
      if (missingHashes.has(hashStr)) {
        events.push(eventBytes);
        cborEvents.push(this.eventCborBytesCache.get(hashStr)!);
      }
    }

    metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
    return { events, cborEvents };
  }
}
