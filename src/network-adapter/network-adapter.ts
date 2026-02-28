import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim";
import {
  ContactCard,
  Identifier,
  Keyhive,
} from "@keyhive/keyhive/slim";
import { encode, decode } from "cbor-x";

import {
  decodeKeyhiveMessageData,
  KeyhiveMessageData,
  signData,
  verifyData,
} from "./messages.js";
import { PromiseQueue, Pending } from "./pending.js";
import { getEventsForAgent, getEventHashesForAgent, keyhiveIdentifierFromPeerId } from "../utilities.js";
import {
  getPendingOpHashes,
  KeyhiveStorage,
  receiveContactCard,
} from "../keyhive/keyhive.js";

// Map from hash string to hash bytes
type PeerHashes = Map<string, Uint8Array>;

type KeyhiveMessage = {
  msg: Message;
  data: KeyhiveMessageData;
}

class SyncContext {
  private publicHashes?: Map<string, Uint8Array>;
  private publicEvents?: Map<Uint8Array, any>;
  publicHashCount = 0;
  publicEventCount = 0;

  constructor(private keyhive: Keyhive) {}

  async getPublicHashes(): Promise<Map<string, Uint8Array>> {
    if (!this.publicHashes) {
      const agent = await this.keyhive.getAgent(Identifier.publicId());
      this.publicHashes = agent
        ? await getEventHashesForAgent(this.keyhive, agent)
        : new Map();
      this.publicHashCount = this.publicHashes.size;
    }
    return this.publicHashes;
  }

  async getPublicEvents(): Promise<Map<Uint8Array, any>> {
    if (!this.publicEvents) {
      const agent = await this.keyhive.getAgent(Identifier.publicId());
      this.publicEvents = agent
        ? await getEventsForAgent(this.keyhive, agent)
        : new Map();
      this.publicEventCount = this.publicEvents.size;
    }
    return this.publicEvents;
  }
}

class Metrics {
  private msgTypeCounts: Record<string, number> = {};
  private totalPayloadBytes = 0;
  private uniqueSenders = new Set<string>();
  private nonKeyhiveCount = 0;
  private droppedSyncRequests = 0;
  private messageCount = 0;
  private totalProcessingTimeMs = 0;
  private publicHashCount = 0;
  private publicEventCount = 0;

  recordMessage(msgType: string | undefined, senderId: string | undefined, payloadBytes: number) {
    const type = msgType ?? "unknown";
    this.msgTypeCounts[type] = (this.msgTypeCounts[type] ?? 0) + 1;
    this.totalPayloadBytes += payloadBytes;
    if (senderId) this.uniqueSenders.add(senderId);
    this.messageCount++;
  }

  recordNonKeyhive() {
    this.nonKeyhiveCount++;
  }

  recordDroppedSyncRequest() {
    this.droppedSyncRequests++;
  }

  recordProcessingTime(ms: number) {
    this.totalProcessingTimeMs += ms;
  }

  recordPublicLookups(hashCount: number, eventCount: number) {
    this.publicHashCount += hashCount;
    this.publicEventCount += eventCount;
  }

  hasActivity(): boolean {
    return this.messageCount > 0 || this.nonKeyhiveCount > 0;
  }

  logReport(label: string) {
    if (!this.hasActivity()) return;
    const countsStr = Object.entries(this.msgTypeCounts)
      .map(([type, count]) => `${type}=${count}`)
      .join(", ");
    console.log(
      `[${label}] ${this.messageCount} keyhive messages from ${this.uniqueSenders.size} peers at ${new Date().toLocaleTimeString("en-GB")}. ` +
      `${this.droppedSyncRequests} duplicate sync requests dropped. ` +
      `${this.nonKeyhiveCount} non-keyhive messages. ` +
      `Breakdown: ${countsStr}. Total payload: ${this.totalPayloadBytes} bytes. ` +
      `Processing: ${this.totalProcessingTimeMs}ms. ` +
      `Public lookups: ${this.publicHashCount} hashes, ${this.publicEventCount} events`
    );
  }

  reset() {
    this.msgTypeCounts = {};
    this.totalPayloadBytes = 0;
    this.uniqueSenders = new Set();
    this.nonKeyhiveCount = 0;
    this.droppedSyncRequests = 0;
    this.messageCount = 0;
    this.totalProcessingTimeMs = 0;
    this.publicHashCount = 0;
    this.publicEventCount = 0;
  }
}

class MessageBatch {
  readonly messages: KeyhiveMessage[] = [];
  readonly metrics = new Metrics();
  private readonly syncRequestSenders = new Set<string>();

  add(msg: Message, data: KeyhiveMessageData) {
    if (msg.type === "keyhive-sync-request" && msg.senderId) {
      if (this.syncRequestSenders.has(msg.senderId)) {
        this.metrics.recordDroppedSyncRequest();
        return;
      }
      this.syncRequestSenders.add(msg.senderId);
    }
    this.metrics.recordMessage(msg.type, msg.senderId, data.signed.payload?.byteLength ?? 0);
    this.messages.push({ msg, data });
  }

  countNonKeyhive() {
    this.metrics.recordNonKeyhive();
  }

  get isEmpty(): boolean {
    return !this.metrics.hasActivity();
  }
}

class BatchProcessor {
  private timeoutId?: ReturnType<typeof setTimeout>;

  constructor(
    private readonly batchInterval: number,
    private readonly keyhive: Keyhive,
    private readonly handleMessage: (msg: Message, data: KeyhiveMessageData, ctx: SyncContext) => Promise<void>,
    private readonly swapBatch: () => MessageBatch,
  ) {}

  start() {
    this.scheduleNext();
  }

  stop() {
    if (this.timeoutId !== undefined) {
      clearTimeout(this.timeoutId);
      this.timeoutId = undefined;
    }
  }

  private scheduleNext() {
    this.timeoutId = setTimeout(() => { void this.processAndReschedule() }, this.batchInterval);
  }

  private async processAndReschedule() {
    const batch = this.swapBatch();
    if (!batch.isEmpty) {
      await this.processBatch(batch);
    }
    this.scheduleNext();
  }

  private async processBatch(batch: MessageBatch) {
    const ctx = new SyncContext(this.keyhive);
    const startTime = Date.now();
    for (const { msg, data } of batch.messages) {
      try {
        await this.handleMessage(msg, data, ctx);
      } catch (error) {
        console.error(`[AMRepoKeyhive] Error processing batch message (type=${msg.type}, from=${msg.senderId}):`, error);
      }
    }
    batch.metrics.recordProcessingTime(Date.now() - startTime);
    batch.metrics.recordPublicLookups(ctx.publicHashCount, ctx.publicEventCount);
    batch.metrics.logReport("Batch");
  }
}

class Peer {
  lastKeyhiveRequestRcvd: Date = new Date();
  lastKeyhiveRequestSent: Date = new Date();
  constructor() {}
};

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending();
  // Connected peers and metadata
  private peers: Map<PeerId, Peer> = new Map();
  private syncIntervalId?: ReturnType<typeof setInterval> | undefined;
  private compactionIntervalId?: ReturnType<typeof setInterval>;
  private batchProcessor?: BatchProcessor;

  private cacheHashes: boolean;
  private hashesCache: Map<PeerId, PeerHashes> = new Map();
  private pendingOpHashesCache: Uint8Array[] | null = null;
  private lastKnownTotalOps: bigint = 0n;

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
    cacheHashes: boolean = false,
    // TODO: Replace with dynamic configuration
    private hardcodedRemoteId: PeerId | null = null,
    private syncRequestInterval: number,
    batchInterval?: number,
  ) {
    super();
    this.cacheHashes = cacheHashes;

    if (periodicallyRequestSync) {
        this.syncIntervalId = setInterval(this.requestKeyhiveSync.bind(this), syncRequestInterval);
    }

    this.compactionIntervalId = setInterval(
      this.runCompaction.bind(this),
      60000
    );

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
      this.hashesCache.delete(payload.peerId);
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
      this.metricsIntervalId = setInterval(() => {
        this.streamingMetrics.logReport("Streaming");
        this.streamingMetrics.reset();
      }, 1000);
    }
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
        console.log(
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
            const ctx = new SyncContext(this.keyhive);
            const startTime = Date.now();
            void this.handleKeyhiveMessage(message, maybeKeyhiveMessageData, ctx).then(() => {
              this.streamingMetrics.recordProcessingTime(Date.now() - startTime);
              this.streamingMetrics.recordPublicLookups(ctx.publicHashCount, ctx.publicEventCount);
            });
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
    ctx: SyncContext,
  ) {
    if (keyhiveMessageData.contactCard) {
      await receiveContactCard(
        this.keyhive,
        keyhiveMessageData.contactCard,
        this.keyhiveStorage
      );
    }
    message.data = keyhiveMessageData.signed.payload;

    if (message.type === "keyhive-sync-request") {
      await this.sendKeyhiveSyncResponse(message, ctx);
    } else if (message.type === "keyhive-sync-response") {
      await this.sendKeyhiveSyncOps(message, ctx);
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

  // Trigger the keyhive op set reconciliation sync protocol. Determine the hashes
  // that are relevant for the given peer as well as any pending hashes on this
  // keyhive (any pending hash might be relevant). Then send a request to the
  // peer to begin the sync protocol.
  // This is the first keyhive op sync protocol message.
  private async asyncSyncKeyhive(
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

      const ctx = new SyncContext(this.keyhive);
      console.debug(`[AMRepoKeyhive] Syncing with ${this.peers.size} peers`);
      for (const targetId of this.peers.keys()) {
        if (targetId == senderId || targetId == this.peerId!) {
          continue;
        }
        if (!this.readyToSendKeyhiveRequest(targetId)) {
          console.debug(`[AMRepoKeyhive] Attempted to send keyhive sync request to ${targetId} too soon. Ignoring.`);
          continue;
        }

        const hashes = await this.getHashesForPeerPair(senderId, targetId, ctx);
        if (hashes) {
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
        } else {
          const keyhiveId = keyhiveIdentifierFromPeerId(targetId);
          const agent = await this.keyhive.getAgent(keyhiveId);
          if (!agent) {
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
          }
        }
        const peer = this.peers.get(targetId);
        if (peer) {
          peer.lastKeyhiveRequestSent = new Date();
        }
      }
    });
  }

  // Send a response to a request from a peer to initiate the keyhive op set
  // reconciliation sync protocol. Given the hashes sent by the peer, determine
  // which ops to send them. Then determine any missing ops to request from the
  // peer.
  // This is the second keyhive op sync protocol message.
  private async sendKeyhiveSyncResponse(message: Message, ctx: SyncContext): Promise<void> {
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

    await this.keyhiveQueue.run(async () => {
      if (!this.readyToSendKeyhiveResponse(message.senderId)) {
        console.debug(`[AMRepoKeyhive] Received next keyhive sync request too soon from ${message.senderId}. Ignoring.`);
        return;
      }

      const localHashes = await this.getHashesForPeerPair(peerId, message.senderId, ctx);
      if (localHashes) {
        const pendingOpHashes = await this.getCachedPendingOpHashes();
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

        // Only fetch full events if we have ops to send
        let foundOps: Uint8Array[] = [];
        if (hashStringsToSend.size > 0) {
          foundOps = await this.getEventBytesForHashes(peerId, hashStringsToSend, ctx);
        }

        console.debug(
          `[AMRepoKeyhive] Found ${foundOps.length} ops to send to and ${requested.length} ops to request from ${message.senderId}`
        );

        const responseData = {
          requested,
          found: foundOps,
        };

        const data = encode(responseData);
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
      } else {
        const keyhiveId = keyhiveIdentifierFromPeerId(message.senderId);
        const agent = await this.keyhive.getAgent(keyhiveId);
        if (!agent) {
          console.debug(
            `[AMRepoKeyhive] No agent found for ${message.senderId}, sending keyhive-sync-missing-contact-card`
          );
          const response = {
            type: "keyhive-sync-request-contact-card",
            senderId: peerId,
            targetId: message.senderId,
          };
          this.send(response, this.contactCard);
        }
      }
      const peer = this.peers.get(message.senderId);
      if (peer) {
        peer.lastKeyhiveRequestRcvd = new Date();
      }
    });
  }

  // Send requested ops in response to a keyhive sync response. Look up ops
  // for the requested hashes and send them to the requesting peer.
  // This is the third (and final) keyhive op sync protocol message.
  private async sendKeyhiveSyncOps(message: Message, ctx: SyncContext): Promise<void> {
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

    console.debug(
      `[AMRepoKeyhive] Received keyhive sync response from ${message.senderId}: ${foundEvents.length} ops found, ${requestedHashes.length} ops requested`
    );

    await this.keyhiveQueue.run(async () => {
      if (foundEvents.length > 0) {
        console.debug(
          `[AMRepoKeyhive] Ingesting ${foundEvents.length} keyhive events from ${message.senderId}`
        );

        try {
          let pendingEvents: any[] | null = null;
          try {
            pendingEvents = await this.keyhive.ingestEventsBytes(foundEvents);
          } catch (error) {
            console.error(`[AMRepoKeyhive] Error ingesting events: ${error}`);
          }

          if (pendingEvents) {
            console.debug(
              `[AMRepoKeyhive] After ingestion: ${pendingEvents.length} pending events`
            );
          }

          // If there are pending events or something went wrong ingesting, try reading from
          // storage (e.g., in case they have already been processed by a separate tab in a
          // browser).
          if (!pendingEvents || pendingEvents.length > 0) {
            if (pendingEvents) {
              console.warn(
                `[AMRepoKeyhive] ${pendingEvents.length} events stuck in pending. Reading from storage`
              );
            }
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

          void this.saveReceivedEvents(foundEvents);
          // Invalidate cache since we ingested events from a peer
          this.invalidateCaches();
          (this.emit as any)("ingest-remote");
        } catch (error) {
          await this.handleIngestError(error, foundEvents, message.senderId);
        }
      }

      if (requestedHashes.length > 0) {
        const requestedHashStrings = new Set(
          requestedHashes.map((h) => h.toString())
        );
        const requestedOps = await this.getEventBytesForHashes(peerId, requestedHashStrings, ctx);

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
          senderId: peerId,
          targetId: message.senderId,
          data,
        };
        this.send(response);
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

    await this.keyhiveQueue.run(async () => {
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

          void this.saveReceivedEvents(receivedEvents);
          // Invalidate cache since we ingested events from a peer
          this.invalidateCaches();
          (this.emit as any)("ingest-remote");
        } catch (error) {
          await this.handleIngestError(error, receivedEvents, message.senderId);
        }
      }
    });
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

  private readyToSendKeyhiveRequest(targetId: PeerId): boolean {
    const now = new Date().getTime();
    const lastKeyhiveRequestSent: Date | undefined = this.peers.get(targetId)?.lastKeyhiveRequestSent;
    if (!lastKeyhiveRequestSent) {
      return true
    }
    return (now - lastKeyhiveRequestSent.getTime()) > this.minSyncRequestInterval
  }

  private readyToSendKeyhiveResponse(senderId: PeerId): boolean {
    const now = new Date().getTime();
    const lastKeyhiveRequestRcvd: Date | undefined = this.peers.get(senderId)?.lastKeyhiveRequestRcvd;
    if (!lastKeyhiveRequestRcvd) {
      return true
    }
    return (now - lastKeyhiveRequestRcvd.getTime()) > this.minSyncResponseInterval
  }

  private runCompaction(): void {
    void this.keyhiveQueue.run(async () => {
      await this.keyhiveStorage.compact(this.keyhive);
    });
  }

  private invalidateCaches(): void {
    this.hashesCache.clear();
    this.pendingOpHashesCache = null;
  }

  // Check if keyhive state changed and invalidate cache if needed
  private async checkAndInvalidateCache(): Promise<void> {
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

  private async getCachedPendingOpHashes(): Promise<Uint8Array[]> {
    if (this.cacheHashes && this.pendingOpHashesCache !== null) {
      return this.pendingOpHashesCache;
    }
    const hashes = await getPendingOpHashes(this.keyhive);
    if (this.cacheHashes) {
      this.pendingOpHashesCache = hashes;
    }
    return hashes;
  }

  // Get event hashes for a peer. Returns null if the peer agent is unknown.
  private async getHashesForPeer(peerId: PeerId): Promise<PeerHashes | null> {
    if (this.cacheHashes) {
      const cached = this.hashesCache.get(peerId);
      if (cached) {
        return cached;
      }
    }

    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await this.keyhive.getAgent(keyhiveId);
    if (!agent) {
      return null;
    }

    const hashes = await getEventHashesForAgent(this.keyhive, agent);

    if (this.cacheHashes) {
      this.hashesCache.set(peerId, hashes);
    }
    return hashes;
  }

  // Returns intersection of hashes both peers can access
  // (A ∪ Public) ∩ (B ∪ Public) = (A ∩ B) ∪ Public
  private async getHashesForPeerPair(
    peerA: PeerId,
    peerB: PeerId,
    ctx: SyncContext,
  ): Promise<PeerHashes | null> {
    const hashesForA = await this.getHashesForPeer(peerA);
    const hashesForB = await this.getHashesForPeer(peerB);

    if (!hashesForA || !hashesForB) {
      return null;
    }

    const publicHashes = await ctx.getPublicHashes();

    const result = new Map<string, Uint8Array>(publicHashes);
    for (const [hashString, hashBytes] of hashesForA.entries()) {
      if (hashesForB.has(hashString)) {
        result.set(hashString, hashBytes);
      }
    }

    return result;
  }

  // Fetch full event bytes for a set of hashes
  private async getEventBytesForHashes(
    peerId: PeerId,
    hashStrings: Set<string>,
    ctx: SyncContext,
  ): Promise<Uint8Array[]> {
    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await this.keyhive.getAgent(keyhiveId);

    const events = new Map<Uint8Array, any>();

    if (agent) {
      const peerEvents = await getEventsForAgent(this.keyhive, agent);
      for (const [hash, event] of peerEvents) {
        events.set(hash, event);
      }
    }

    const publicEvents = await ctx.getPublicEvents();
    for (const [hash, event] of publicEvents) {
      events.set(hash, event);
    }

    if (events.size === 0) {
      return [];
    }

    const result: Uint8Array[] = [];
    for (const [hash, eventBytes] of events.entries()) {
      if (hashStrings.has(hash.toString())) {
        result.push(eventBytes);
      }
    }
    return result;
  }
}
