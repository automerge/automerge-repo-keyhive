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

import { Metrics } from "../metrics.js";
import { Peer } from "../peer.js";
import { Pending, PromiseQueue } from "../pending.js";
import { SyncProtocol } from "../sync-protocol.js";
import { EventCache } from "../event-cache.js";
import { EventBytesOnlyEventCache } from "../event-bytes-only-event-cache.js";
import { StandardEventCache } from "../standard-event-cache.js";
import { PeriodicEventCache } from "../periodic-event-cache.js";
import { peerIdFromVerifyingKey } from "../messages.js";
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
 * Sync the local keyhive against a single Rust subduction-keyhive peer
 * (typically the Rust sync server) using SUK-framed wire messages.
 */
export class KeyhiveRustAdapter {
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
  private syncIntervalId?: ReturnType<typeof setInterval>;
  private periodicCacheRefreshId?: ReturnType<typeof setInterval>;

  constructor(options: KeyhiveRustAdapterOptions) {
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
        emit: () => {
          // Direct transport doesn't fan out events; suppress.
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

    if (options.periodicallyRequestSync) {
      this.syncIntervalId = setInterval(() => {
        this.syncProtocol.requestKeyhiveSync();
      }, syncRequestInterval);
    }
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
        await this.keyhiveQueue.run(() =>
          receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage),
        );
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
