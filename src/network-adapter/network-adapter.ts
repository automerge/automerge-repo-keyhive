import {
  Message,
  NetworkAdapter,
  PeerId,
  PeerMetadata,
} from "@automerge/automerge-repo/slim";
import { ContactCard, Keyhive } from "@keyhive/keyhive/slim";
import {
  decodeKeyhiveMessageData,
  signData,
  verifyData,
} from "./messages.js";
import { PromiseQueue, Pending } from "./pending.js";
import { Metrics } from "./metrics.js";
import type { EventCache } from "./event-cache.js";
import { StandardEventCache } from "./standard-event-cache.js";
import { EventBytesOnlyEventCache } from "./event-bytes-only-event-cache.js";
import { PeriodicEventCache } from "./periodic-event-cache.js";
import { MessageBatch, BatchProcessor } from "./batch.js";
import {
  KeyhiveStorage,
} from "../keyhive/keyhive.js";
import { SyncProtocol } from "./sync-protocol.js";
import { Peer } from "./peer.js";

export interface KeyhiveNetworkAdapterOptions {
  networkAdapter: NetworkAdapter;
  contactCard: ContactCard;
  keyhive: Keyhive;
  keyhiveStorage: KeyhiveStorage;
  keyhiveQueue: PromiseQueue;
  periodicallyRequestSync: boolean;
  cachingMode?: "none" | "standard" | "periodic";
  // TODO: Replace with dynamic configuration
  hardcodedRemoteId?: PeerId | null;
  syncRequestInterval: number;
  batchInterval?: number;
  retryPendingFromStorage?: boolean;
  enableCompaction?: boolean;
  archiveThreshold?: number;
}

export class KeyhiveNetworkAdapter extends NetworkAdapter {
  private pending = new Pending();
  private peers: Map<PeerId, Peer> = new Map();
  private syncIntervalId?: ReturnType<typeof setInterval> | undefined;
  private compactionIntervalId?: ReturnType<typeof setInterval>;
  private batchProcessor?: BatchProcessor;

  private periodicCacheRefreshId?: ReturnType<typeof setInterval>;

  private networkAdapter: NetworkAdapter;
  private contactCard: ContactCard;
  private keyhive: Keyhive;
  private keyhiveStorage: KeyhiveStorage;
  private keyhiveQueue: PromiseQueue;
  // TODO: Replace with dynamic configuration
  private hardcodedRemoteId: PeerId | null;

  private batchInterval: number | undefined;
  private keyhiveMsgBatch: MessageBatch;
  private streamingMetrics = new Metrics();
  private metricsIntervalId?: ReturnType<typeof setInterval>;

  private syncProtocol: SyncProtocol;

  constructor(options: KeyhiveNetworkAdapterOptions) {
    super();

    const {
      networkAdapter,
      contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync,
      cachingMode = "none",
      hardcodedRemoteId = null,
      syncRequestInterval,
      batchInterval,
      retryPendingFromStorage = true,
      enableCompaction = true,
      archiveThreshold = 200,
    } = options;

    this.networkAdapter = networkAdapter;
    this.contactCard = contactCard;
    this.keyhive = keyhive;
    this.keyhiveStorage = keyhiveStorage;
    this.keyhiveQueue = keyhiveQueue;
    this.hardcodedRemoteId = hardcodedRemoteId;

    let cache: EventCache;
    if (cachingMode === "periodic") {
      const periodicCache = new PeriodicEventCache();
      cache = periodicCache;
      // Periodic refresh at the same interval as sync requests
      this.periodicCacheRefreshId = setInterval(() => {
        void this.keyhiveQueue.run(() => periodicCache.refresh(this.keyhive)).catch((error) =>
          console.error("[AMRepoKeyhive] PeriodicEventCache refresh failed:", error)
        );
      }, syncRequestInterval);
      // Initial refresh
      void this.keyhiveQueue.run(() => periodicCache.refresh(this.keyhive)).catch((error) =>
        console.error("[AMRepoKeyhive] Initial PeriodicEventCache refresh failed:", error)
      );
    } else if (cachingMode === "standard") {
      cache = new StandardEventCache();
    } else {
      cache = new EventBytesOnlyEventCache();
    }

    this.syncProtocol = new SyncProtocol(
      {
        keyhive,
        keyhiveStorage,
        keyhiveQueue,
        peers: this.peers,
        contactCard,
        cache,
        getPeerId: () => this.peerId,
        getMetrics: () => this.streamingMetrics,
        send: (message, contactCard?) => this.send(message, contactCard),
        emit: (event) => (this.emit as any)(event),
      },
      {
        archiveThreshold,
        retryPendingFromStorage,
        minSyncRequestInterval: 1000,
        minSyncResponseInterval: 1000,
      },
    );

    if (periodicallyRequestSync) {
        this.syncIntervalId = setInterval(
          () => this.syncProtocol.requestKeyhiveSync(),
          syncRequestInterval,
        );
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
      this.syncProtocol.onPeerDisconnected(payload.peerId);
    });

    this.keyhiveMsgBatch = new MessageBatch();

    this.batchInterval = batchInterval;
    if (this.isBatching()) {
      this.batchProcessor = new BatchProcessor(
        this.batchInterval!,
        this.keyhive,
        async (message, data, metrics) => {
          const handled = await this.syncProtocol.handleKeyhiveMessage(message, data, metrics);
          if (!handled) {
            this.emit("message", message);
          }
        },
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
    if (this.periodicCacheRefreshId) {
      clearInterval(this.periodicCacheRefreshId);
      this.periodicCacheRefreshId = undefined;
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
            void this.syncProtocol.handleKeyhiveMessage(message, maybeKeyhiveMessageData, this.streamingMetrics).then((handled) => {
              this.streamingMetrics.recordProcessingTime(Date.now() - startTime);
              this.streamingMetrics.recordProcessingTimeByType(msgType, Date.now() - startTime);
              if (!handled) {
                this.emit("message", message);
              }
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

  syncKeyhive(
    maybeSenderId: PeerId | undefined = undefined,
    includeContactCard: boolean = false,
    attemptRecovery: boolean = false
  ): void {
    this.syncProtocol.syncKeyhive(maybeSenderId, includeContactCard, attemptRecovery);
  }

  invalidateCaches(): void {
    this.syncProtocol.invalidateCaches();
  }

  private runCompaction(): void {
    void this.keyhiveQueue.run(async () => {
      await this.keyhiveStorage.compact(this.keyhive);
    }).catch((error) =>
      console.error("[AMRepoKeyhive] Compaction failed:", error)
    );
  }
}
