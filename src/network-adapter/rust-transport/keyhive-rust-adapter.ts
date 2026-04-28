// Keyhive sync over the Rust subduction wire (SUK frames on a raw
// WebSocket). Drives the existing `SyncProtocol` after translating
// between the TS Message shape and the Rust `KeyhiveMessage` enum shape.
//
// Compared to `KeyhiveNetworkAdapter`, this does NOT use an automerge-repo
// `NetworkAdapter`. The transport is a `FrameDemuxer` over a raw WebSocket;
// subduction-WASM consumes non-SUK frames (sedimentree sync), and this
// class consumes SUK frames (keyhive sync).

import type { Message, PeerId } from "@automerge/automerge-repo/slim";
import { ContactCard, Keyhive, Signed } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

import { Metrics } from "../metrics.js";
import { Peer } from "../peer.js";
import { Pending, PromiseQueue } from "../pending.js";
import { SyncProtocol } from "../sync-protocol.js";
import { EventCache } from "../event-cache.js";
import { EventBytesOnlyEventCache } from "../event-bytes-only-event-cache.js";
import { StandardEventCache } from "../standard-event-cache.js";
import { PeriodicEventCache } from "../periodic-event-cache.js";
import { peerIdFromVerifyingKey } from "../messages.js";
import { keyhiveIdentifierFromPeerId } from "../../utilities.js";
import { KeyhiveStorage, receiveContactCard } from "../../keyhive/keyhive.js";

import { FrameDemuxer } from "./frame-demuxer.js";
import {
  KeyhiveMessageType,
  decodeRustKeyhiveMessage,
  decodeSignedMessage,
  decodeSukFrame,
  encodeRustKeyhiveMessage,
  encodeSignedMessage,
  encodeSukFrame,
} from "./codec.js";

const KEYHIVE_MESSAGE_TYPES: ReadonlySet<string> = new Set<KeyhiveMessageType>([
  "keyhive-sync-request",
  "keyhive-sync-response",
  "keyhive-sync-ops",
  "keyhive-sync-request-contact-card",
  "keyhive-sync-missing-contact-card",
  "keyhive-sync-check",
  "keyhive-sync-confirmation",
]);

export interface KeyhiveRustAdapterOptions {
  demuxer: FrameDemuxer;
  keyhive: Keyhive;
  keyhiveStorage: KeyhiveStorage;
  keyhiveQueue: PromiseQueue;
  contactCard: ContactCard;
  /** Our own peer id (base64 of the keyhive verifying key, optional suffix). */
  localPeerId: PeerId;
  /** The remote (Rust server) peer id, learned from the subduction handshake. */
  remotePeerId: PeerId;
  cachingMode?: "none" | "standard" | "periodic";
  syncRequestInterval?: number;
  periodicallyRequestSync?: boolean;
}

/**
 * Events emitted by {@link KeyhiveRustAdapter}. Mirrors the subset of
 * `KeyhiveNetworkAdapter` events that consumers (e.g. the demo's TUI
 * peer-manager) rely on, so callers can branch on adapter type and use
 * the same event names.
 */
export interface KeyhiveRustAdapterEvents {
  "peer-candidate": (payload: { peerId: PeerId }) => void;
  "peer-disconnected": (payload: { peerId: PeerId }) => void;
  "ingest-remote": () => void;
}

/**
 * Sync the local keyhive against a single Rust subduction-keyhive peer
 * (typically the Rust sync server) using SUK-framed wire messages.
 */
export class KeyhiveRustAdapter extends EventEmitter<KeyhiveRustAdapterEvents> {
  readonly localPeerId: PeerId;
  readonly remotePeerId: PeerId;

  private readonly demuxer: FrameDemuxer;
  private readonly keyhive: Keyhive;
  private readonly keyhiveStorage: KeyhiveStorage;
  private readonly keyhiveQueue: PromiseQueue;
  private readonly contactCard: ContactCard;
  private readonly peers: Map<PeerId, Peer>;
  private readonly metrics = new Metrics();
  private readonly pending = new Pending();
  private readonly syncProtocol: SyncProtocol;
  private readonly syncRequestInterval: number;
  private periodicSyncEnabled: boolean;
  private syncIntervalId?: ReturnType<typeof setInterval>;
  private periodicCacheRefreshId?: ReturnType<typeof setInterval>;
  /**
   * Tracks whether the underlying transport currently has an active connection
   * to `remotePeerId`. Set true just before emitting the initial
   * `peer-candidate` (in a `queueMicrotask` after construction) and reset when
   * the demuxer closes. Exposed via {@link connected} so subscribers that
   * attach *after* the initial emit can still observe current state without
   * waiting for the next event.
   */
  private _connected = false;

  constructor(options: KeyhiveRustAdapterOptions) {
    super();
    this.demuxer = options.demuxer;
    this.keyhive = options.keyhive;
    this.keyhiveStorage = options.keyhiveStorage;
    this.keyhiveQueue = options.keyhiveQueue;
    this.contactCard = options.contactCard;
    this.localPeerId = options.localPeerId;
    this.remotePeerId = options.remotePeerId;

    this.peers = new Map<PeerId, Peer>();
    this.peers.set(this.remotePeerId, new Peer());

    const cachingMode = options.cachingMode ?? "periodic";
    const syncRequestInterval = options.syncRequestInterval ?? 5000;
    this.syncRequestInterval = syncRequestInterval;
    this.periodicSyncEnabled = options.periodicallyRequestSync ?? false;

    let cache: EventCache;
    if (cachingMode === "periodic") {
      const periodicCache = new PeriodicEventCache();
      cache = periodicCache;
      this.periodicCacheRefreshId = setInterval(() => {
        void this.keyhiveQueue
          .run(() => periodicCache.refresh(this.keyhive))
          .catch((err) =>
            console.error(
              "[KeyhiveRustAdapter] PeriodicEventCache refresh failed:",
              err,
            ),
          );
      }, syncRequestInterval);
      void this.keyhiveQueue
        .run(() => periodicCache.refresh(this.keyhive))
        .catch((err) =>
          console.error(
            "[KeyhiveRustAdapter] Initial PeriodicEventCache refresh failed:",
            err,
          ),
        );
    } else if (cachingMode === "standard") {
      cache = new StandardEventCache();
    } else {
      cache = new EventBytesOnlyEventCache();
    }

    this.syncProtocol = new SyncProtocol(
      {
        keyhive: this.keyhive,
        keyhiveStorage: this.keyhiveStorage,
        keyhiveQueue: this.keyhiveQueue,
        peers: this.peers,
        contactCard: this.contactCard,
        cache,
        getPeerId: () => this.localPeerId,
        getMetrics: () => this.metrics,
        send: (message, contactCard) => {
          this.send(message, contactCard);
        },
        emit: (event) => {
          // Forward keyhive sync events (notably "ingest-remote") to consumers.
          (this.emit as (e: string) => boolean)(event);
        },
      },
      {
        archiveThreshold: 200,
        retryPendingFromStorage: true,
        minSyncRequestInterval: 1000,
        minSyncResponseInterval: 1000,
      },
    );

    this.demuxer.setSukHandler((bytes) => {
      void this.handleInbound(bytes).catch((err) =>
        console.error("[KeyhiveRustAdapter] inbound SUK handler failed:", err),
      );
    });

    if (this.periodicSyncEnabled) {
      this.syncIntervalId = setInterval(() => {
        this.syncProtocol.requestKeyhiveSync();
      }, syncRequestInterval);
    }

    // Demuxer is connected by the time we reach here (FrameDemuxer.connect
    // resolves on `open`). Surface a peer-candidate immediately so consumers
    // tracking adapter readiness see the remote as live, then wire the
    // close path to peer-disconnected.
    queueMicrotask(() => {
      this._connected = true;
      this.emit("peer-candidate", { peerId: this.remotePeerId });
    });
    void this.demuxer.closed().then(() => {
      this._connected = false;
      this.emit("peer-disconnected", { peerId: this.remotePeerId });
    });
  }

  /**
   * Whether the transport currently has an active connection to
   * `remotePeerId`. Useful for subscribers attached after the initial
   * `peer-candidate` microtask fires (e.g. UI started after
   * `initializeAutomergeRepoKeyhiveRust` returned) to seed their state.
   */
  get connected(): boolean {
    return this._connected;
  }

  /**
   * Resolves once the underlying transport is connected and this adapter
   * is ready to send/receive. The `FrameDemuxer.connect()` factory resolves
   * on WebSocket `open`, so by the time this adapter is constructed the
   * transport is already open — `whenReady()` resolves on the next tick.
   */
  whenReady(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Start (or restart) the periodic keyhive sync request loop. No-op if it
   * is already running. Used by callers that asked for
   * `periodicallyRequestSync: false` and want to defer kicking off the
   * loop until after the subduction handshake completes (so SUK frames
   * don't race the SUH handshake on a multiplexed WebSocket).
   */
  startPeriodicSync(): void {
    if (this.syncIntervalId !== undefined) return;
    this.periodicSyncEnabled = true;
    this.syncIntervalId = setInterval(() => {
      this.syncProtocol.requestKeyhiveSync();
    }, this.syncRequestInterval);
  }

  /**
   * Trigger an outbound sync attempt against the remote.
   */
  syncKeyhive(includeContactCard: boolean = false): void {
    this.syncProtocol.syncKeyhive(undefined, includeContactCard, false);
  }

  invalidateCaches(): void {
    this.syncProtocol.invalidateCaches();
  }

  disconnect(): void {
    if (this.syncIntervalId) {
      clearInterval(this.syncIntervalId);
      this.syncIntervalId = undefined;
    }
    if (this.periodicCacheRefreshId) {
      clearInterval(this.periodicCacheRefreshId);
      this.periodicCacheRefreshId = undefined;
    }
    this.demuxer.setSukHandler(null);
  }

  // ─────────────────────────────────────────────────────────────────────
  // Outbound: SyncProtocol → SUK-framed bytes
  // ─────────────────────────────────────────────────────────────────────

  private send(message: Message, contactCard?: ContactCard): void {
    if (this.demuxer.isClosed()) {
      console.warn(
        "[KeyhiveRustAdapter] dropping outbound message: demuxer closed",
        message.type,
      );
      return;
    }
    if (!message.type || !KEYHIVE_MESSAGE_TYPES.has(message.type)) {
      console.warn(
        `[KeyhiveRustAdapter] dropping non-keyhive message type=${message.type}`,
      );
      return;
    }
    const seqNumber = this.pending.register();
    void (async () => {
      try {
        const bytes = await this.keyhiveQueue.run(() =>
          this.signAndFrame(message, contactCard),
        );
        this.pending.fire(seqNumber, () => {
          this.demuxer.sendSuk(bytes);
        });
      } catch (err) {
        console.error(
          `[KeyhiveRustAdapter] failed to sign+frame (type=${message.type}):`,
          err,
        );
        this.pending.cancel(seqNumber);
      }
    })();
  }

  private async signAndFrame(
    message: Message,
    contactCard?: ContactCard,
  ): Promise<Uint8Array> {
    const inlineDataCbor: Uint8Array =
      "data" in message && message.data instanceof Uint8Array
        ? (message.data as Uint8Array)
        : new Uint8Array();

    const messageType = message.type as KeyhiveMessageType;
    const senderId = message.senderId as PeerId;
    const targetId = message.targetId as PeerId;

    const payload = encodeRustKeyhiveMessage({
      type: messageType,
      senderId,
      targetId,
      inlineDataCbor,
    });

    const signed = await this.keyhive.trySign(payload);
    const signedBytes = signed.toBytes();
    const contactCardJson = contactCard ? contactCard.toJson() : "";
    const signedMessageBytes = encodeSignedMessage({
      contactCard: contactCardJson,
      signed: signedBytes,
    });
    return encodeSukFrame(signedMessageBytes);
  }

  // ─────────────────────────────────────────────────────────────────────
  // Inbound: SUK-framed bytes → SyncProtocol.dispatchByType
  // ─────────────────────────────────────────────────────────────────────

  private async handleInbound(frame: Uint8Array): Promise<void> {
    let signedMessageBytes: Uint8Array;
    try {
      signedMessageBytes = decodeSukFrame(frame);
    } catch (err) {
      console.error("[KeyhiveRustAdapter] bad SUK frame:", err);
      return;
    }

    let signedMessage: ReturnType<typeof decodeSignedMessage>;
    try {
      signedMessage = decodeSignedMessage(signedMessageBytes);
    } catch (err) {
      console.error("[KeyhiveRustAdapter] bad SignedMessage CBOR:", err);
      return;
    }

    let signed: Signed;
    try {
      signed = Signed.fromBytes(signedMessage.signed);
    } catch (err) {
      console.error("[KeyhiveRustAdapter] Signed.fromBytes failed:", err);
      return;
    }

    if (!signed.verify()) {
      console.error("[KeyhiveRustAdapter] signature verification failed");
      return;
    }

    let decoded: ReturnType<typeof decodeRustKeyhiveMessage>;
    try {
      decoded = decodeRustKeyhiveMessage(signed.payload);
    } catch (err) {
      console.error(
        "[KeyhiveRustAdapter] Rust KeyhiveMessage decode failed:",
        err,
      );
      return;
    }

    const verifyingKeyPeer = peerIdFromVerifyingKey(signed.verifyingKey);
    if (decoded.senderId !== verifyingKeyPeer) {
      console.error(
        "[KeyhiveRustAdapter] sender mismatch: payload says",
        decoded.senderId,
        "verifyingKey says",
        verifyingKeyPeer,
      );
      return;
    }

    if (signedMessage.contactCard !== "") {
      try {
        const contactCard = ContactCard.fromJson(signedMessage.contactCard);
        console.warn(
          `[KeyhiveRustAdapter] DIAG: ingesting contactCard from sender=${decoded.senderId} (cardJsonLen=${signedMessage.contactCard.length})`,
        );
        await this.keyhiveQueue.run(() =>
          receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage),
        );
        try {
          const senderIdentifier = keyhiveIdentifierFromPeerId(decoded.senderId);
          const agent = await this.keyhive.getAgent(senderIdentifier);
          console.warn(
            `[KeyhiveRustAdapter] DIAG: post-ingest getAgent(senderId=${decoded.senderId}) -> ${agent ? "FOUND" : "NULL"}`,
          );
        } catch (lookupErr) {
          console.warn(
            `[KeyhiveRustAdapter] DIAG: post-ingest getAgent threw:`,
            lookupErr,
          );
        }
      } catch (err) {
        console.error("[KeyhiveRustAdapter] contactCard ingest failed:", err);
        // Continue — the rest of the message may still be processable.
      }
    }

    if (!this.peers.has(decoded.senderId)) {
      this.peers.set(decoded.senderId, new Peer());
    }

    const message: Message = {
      type: decoded.type,
      senderId: decoded.senderId,
      targetId: decoded.targetId,
      data: decoded.inlineDataCbor,
    } as unknown as Message;

    try {
      await this.syncProtocol.dispatchByType(message, this.metrics);
    } catch (err) {
      console.error(
        `[KeyhiveRustAdapter] dispatchByType failed (type=${decoded.type}):`,
        err,
      );
    }
  }
}
