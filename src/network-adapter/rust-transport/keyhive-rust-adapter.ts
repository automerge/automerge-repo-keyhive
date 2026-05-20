// Keyhive sync over subduction (SUK frames). Translates between the TS
// Message shape and the Rust `KeyhiveMessage` wire format, driving
// `SyncProtocol` for the actual sync logic.

import type { Message, PeerId } from "@automerge/automerge-repo/slim";
import type { Subduction } from "@automerge/automerge-subduction/slim";
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

import {
  KeyhiveMessageType,
  decodeRustKeyhiveMessage,
  decodeSignedMessage,
  encodeRustKeyhiveMessage,
  encodeSignedMessage,
  peerIdToRust,
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
  subduction: Subduction | Promise<Subduction>;
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

  private readonly subductionReady: Promise<{ subduction: Subduction; wasmPeerId: any }>;
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
   * `peer-candidate` (in a `queueMicrotask` after construction). Exposed
   * via {@link connected} so subscribers that attach *after* the initial
   * emit can still observe current state without
   * waiting for the next event.
   */
  private _connected = false;

  constructor(options: KeyhiveRustAdapterOptions) {
    super();
    this.keyhive = options.keyhive;
    this.keyhiveStorage = options.keyhiveStorage;
    this.keyhiveQueue = options.keyhiveQueue;
    this.contactCard = options.contactCard;
    this.localPeerId = options.localPeerId;
    this.remotePeerId = options.remotePeerId;

    // Get a PeerId instance from subduction's own module to avoid
    // cross-module wasm-bindgen instanceof failures.
    const remotePeerBytes = peerIdToRust(options.remotePeerId).verifying_key;
    this.subductionReady = Promise.resolve(options.subduction).then(
      async (subduction) => {
        const wasmPeerId = await waitForPeer(subduction, remotePeerBytes);
        return { subduction, wasmPeerId };
      },
    );

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

    void this.subductionReady.then(({ subduction }) => {
      subduction.registerFrameHandler({
        onMessage: (payload: Uint8Array, _peerId: any) => {
          void this.handleInbound(payload).catch((err) =>
            console.error(
              "[KeyhiveRustAdapter] inbound SUK handler failed:",
              err,
            ),
          );
        },
        onPeerDisconnect: (_peerId: any) => {
          this._connected = false;
          this.syncProtocol.onPeerDisconnected(this.remotePeerId);
          this.emit("peer-disconnected", { peerId: this.remotePeerId });
        },
      });
    });

    if (this.periodicSyncEnabled) {
      this.syncIntervalId = setInterval(() => {
        this.syncProtocol.requestKeyhiveSync();
      }, syncRequestInterval);
    }

    // Surface a peer-candidate immediately so consumers tracking
    // adapter readiness see the remote as live.
    queueMicrotask(() => {
      this._connected = true;
      this.emit("peer-candidate", { peerId: this.remotePeerId });
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

  /** Resolves once the adapter is ready to send/receive. */
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
    void this.subductionReady.then(({ subduction }) =>
      subduction.registerFrameHandler(undefined),
    );
  }

  // ─────────────────────────────────────────────────────────────────────
  // Outbound: SyncProtocol → keyhive payload bytes
  // ─────────────────────────────────────────────────────────────────────

  private send(message: Message, contactCard?: ContactCard): void {
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
          this.signAndEncode(message, contactCard),
        );
        this.pending.fire(seqNumber, () => {
          void this.subductionReady
            .then(({ subduction, wasmPeerId }) =>
              subduction.sendKeyhiveMessage(bytes, wasmPeerId),
            )
            .catch((err: any) =>
              console.warn(
                "[KeyhiveRustAdapter] sendKeyhiveMessage failed:",
                err,
              ),
            );
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

  private async signAndEncode(
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
    return encodeSignedMessage({
      contactCard: contactCardJson,
      signed: signedBytes,
    });
  }

  // ─────────────────────────────────────────────────────────────────────
  // Inbound: keyhive payload → SyncProtocol.dispatchByType
  // ─────────────────────────────────────────────────────────────────────

  private async handleInbound(payload: Uint8Array): Promise<void> {
    let signedMessage: ReturnType<typeof decodeSignedMessage>;
    try {
      signedMessage = decodeSignedMessage(payload);
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

async function waitForPeer(
  subduction: Subduction,
  remotePeerBytes: Uint8Array,
): Promise<any> {
  for (;;) {
    const peers = await subduction.getConnectedPeerIds();
    for (const p of peers) {
      const bytes = (p as any).toBytes() as Uint8Array;
      if (
        bytes.length === remotePeerBytes.length &&
        bytes.every((b, i) => b === remotePeerBytes[i])
      ) {
        return p;
      }
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}
