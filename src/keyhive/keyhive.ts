import {
  AutomergeUrl,
  NetworkAdapter,
  parseAutomergeUrl,
  PeerId,
  StorageAdapterInterface,
  StorageKey,
} from "@automerge/automerge-repo/slim";
import { peerIdFromSigner, uint8ArrayToHex, unwrapWasmError } from "../utilities.js";
import {
  Archive,
  CiphertextStore,
  ContactCard,
  DocumentId as KeyhiveDocumentId,
  Event as KeyhiveEvent,
  Individual,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import { syncServerFromContactCard, SyncServer } from "../sync-server.js";
import { Active, createActive, loadOrCreateSigner, storeActiveKeyPair } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { KeyhiveRustAdapter } from "../network-adapter/rust-transport/keyhive-rust-adapter.js";
import type { Subduction } from "@automerge/automerge-subduction/slim";
import { PromiseQueue } from "../network-adapter/pending.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import { AutomergeRepoKeyhive, AutomergeRepoKeyhiveRust, generateDoc, keyhiveIdFactory } from "./automerge-repo-keyhive.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";
export const KEYHIVE_PREKEY_SECRETS_KEY = "/prekey-secrets";

export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

/**
 * Shared keyhive bootstrap: signer, storage, keyhive instance, active,
 * peerId, emitter, queue, and the hardcoded demo syncServer.
 *
 * Private to this module. Both `initializeAutomergeRepoKeyhive` and
 * `initializeAutomergeRepoKeyhiveRust` build on this so the keyhive
 * queue (which serializes WASM access) stays out of the public API.
 */
interface KeyhiveBootstrap {
  active: Active;
  keyhive: Keyhive;
  keyhiveStorage: KeyhiveStorage;
  peerId: PeerId;
  emitter: KeyhiveEventEmitter;
  keyhiveQueue: PromiseQueue;
  keyPair: CryptoKeyPair;
  signer: Signer;
  syncServer: SyncServer;
  serverPeerIdHardcoded: PeerId;
}

async function bootstrapKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  keyPair?: CryptoKeyPair;
}): Promise<KeyhiveBootstrap> {
  console.warn(`[bootstrapKeyhive] enter suffix=${options.peerIdSuffix}`);
  const { keyPair, signer } = await loadOrCreateKeyPairAndSigner(options.storage, options.keyPair)
  console.warn(`[bootstrapKeyhive] after loadOrCreateKeyPairAndSigner`);
  const emitter = new KeyhiveEventEmitter();
  const uniqueIdHash = new Uint8Array(
    await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(options.peerIdSuffix)
    )
  );
  const keyhiveStorage = new KeyhiveStorage(uniqueIdHash, options.storage);
  console.warn(`[bootstrapKeyhive] before loadOrCreateKeyhive`);
  const keyhive = await keyhiveStorage.loadOrCreateKeyhive(
    signer,
    uniqueIdHash,
    emitter.handleKeyhiveEvent
  );
  console.warn(`[bootstrapKeyhive] after loadOrCreateKeyhive`);
  const active = await createActive(keyPair, signer, keyhive);
  console.warn(`[bootstrapKeyhive] after createActive`);
  const peerId = peerIdFromSigner(active.signer, options.peerIdSuffix);

  // TODO: Server contact card and PeerId are currently just hardcoded for the demo
  const serverContactCardJson =
    '{"Rotate":{"payload":{"old":[73,163,230,244,111,233,153,119,133,211,134,237,111,36,52,131,22,50,54,144,150,45,227,235,128,36,33,217,190,198,55,75],"new":[109,115,204,144,178,114,182,238,113,124,4,139,249,76,220,44,128,104,194,68,187,184,82,241,94,145,104,198,159,122,186,43]},"issuer":[215,244,30,111,15,78,235,218,7,241,63,222,141,131,33,22,234,116,180,208,97,235,210,55,202,209,170,178,98,37,223,159],"signature":[178,64,85,76,51,199,196,151,129,14,191,53,127,191,34,223,97,238,95,109,118,179,152,17,205,188,204,177,116,166,147,231,192,201,48,137,19,214,180,45,108,104,34,8,14,63,115,139,215,142,4,179,233,89,150,218,174,168,107,23,8,109,228,6]}}';
  const serverPeerIdHardcoded = "1/Qebw9O69oH8T/ejYMhFup0tNBh69I3ytGqsmIl358=" as PeerId;

  console.warn(`[bootstrapKeyhive] before syncServerFromContactCard`);
  const syncServer = await syncServerFromContactCard(
    serverContactCardJson,
    serverPeerIdHardcoded,
    keyhive,
    keyhiveStorage
  );
  console.warn(`[bootstrapKeyhive] after syncServerFromContactCard`);

  return {
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    keyhiveQueue: new PromiseQueue(),
    keyPair,
    signer,
    syncServer,
    serverPeerIdHardcoded,
  };
}

/**
 * The minimal interface a sync driver must expose so the
 * keyhive event-flush listener can drive cache invalidation and
 * outbound sync triggers. Both `KeyhiveNetworkAdapter` and
 * `KeyhiveRustAdapter` satisfy this.
 */
interface KeyhiveSyncDriver {
  invalidateCaches(): void;
  syncKeyhive(): void;
}

/**
 * Wire the per-event flush + sync trigger onto the shared emitter.
 * Saves event bytes/prekey secrets to keyhive storage and, when there
 * are new events, asks the driver to invalidate its caches and schedule
 * an outbound sync after 1s.
 */
function setupEventFlushListener(
  bootstrap: KeyhiveBootstrap,
  driver: KeyhiveSyncDriver,
  options: { automaticArchiveIngestion: boolean }
): void {
  const { emitter, keyhiveQueue, keyhiveStorage, keyhive } = bootstrap;
  let syncTimeout: ReturnType<typeof setTimeout> | undefined;
  let pendingEventBytes: Uint8Array[] = [];
  let pendingPrekeySecrets = false;
  let pendingSync = false;
  let flushQueued = false;

  emitter.on("update", (event: KeyhiveEvent) => {
    if (!keyhiveStorage.isEventWriteSuppressed()) {
      pendingEventBytes.push(event.toBytes());
    }

    if (options.automaticArchiveIngestion) {
      if (
        event.variant === "PREKEY_ROTATED" ||
        event.variant === "PREKEYS_EXPANDED"
      ) {
        pendingPrekeySecrets = true;
      }
      pendingSync = true;
    }

    if (!flushQueued) {
      flushQueued = true;
      void keyhiveQueue.run(async () => {
        flushQueued = false;
        const eventsToSave = pendingEventBytes;
        const needPrekeySecrets = pendingPrekeySecrets;
        const needSync = pendingSync;
        pendingEventBytes = [];
        pendingPrekeySecrets = false;
        pendingSync = false;

        if (eventsToSave.length > 0) {
          console.debug(
            `[AMRepoKeyhive] Keyhive updated. Saving ${eventsToSave.length} events.`
          );
          for (const eventBytes of eventsToSave) {
            await keyhiveStorage.saveEventBytesWithHash(eventBytes);
          }
        }

        if (needPrekeySecrets) {
          await keyhiveStorage.savePrekeySecrets(keyhive);
        }

        if (eventsToSave.length > 0) {
          // Invalidate caches so the next sync computes fresh totals
          // (otherwise sync-check uses stale senderTotal from cached hashes)
          driver.invalidateCaches();
        }

        if (needSync && !syncTimeout) {
          syncTimeout = setTimeout(() => {
            syncTimeout = undefined;
            driver.syncKeyhive();
          }, 1000);
        }
      }).catch((error) =>
        console.error("[AMRepoKeyhive] Event flush failed:", error)
      );
    }
  });
}

export async function initializeAutomergeRepoKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  networkAdapter: NetworkAdapter;
  automaticArchiveIngestion?: boolean;
  onlyShareWithHardcodedServerPeerId?: boolean;
  periodicallyRequestSync?: boolean;
  cachingMode?: "none" | "standard" | "periodic";
  keyPair?: CryptoKeyPair;
  syncRequestInterval?: number;
  batchInterval?: number;
  retryPendingFromStorage?: boolean;
  enableCompaction?: boolean;
  archiveThreshold?: number;
}): Promise<AutomergeRepoKeyhive> {
  const {
    automaticArchiveIngestion = true,
    onlyShareWithHardcodedServerPeerId = false,
    periodicallyRequestSync = true,
    cachingMode = "none" as "none" | "standard" | "periodic",
    syncRequestInterval = 2000,
    batchInterval,
    retryPendingFromStorage = true,
    enableCompaction = true,
    archiveThreshold = 200,
  } = options;

  const bootstrap = await bootstrapKeyhive(options);
  const { active, keyhive, keyhiveStorage, peerId, emitter, keyhiveQueue, syncServer, serverPeerIdHardcoded } = bootstrap;

  const createKeyhiveNetworkAdapter = (networkAdapter: NetworkAdapter, onlyShareWithHardcodedServerPeerId: boolean, periodicallyRequestSync: boolean, syncRequestInterval: number, batchIntervalOverride?: number, archiveThresholdOverride?: number) => {
    let hardcodedServerPeerId = null;
    if (onlyShareWithHardcodedServerPeerId) {
      hardcodedServerPeerId = serverPeerIdHardcoded
    }

    return new KeyhiveNetworkAdapter({
      networkAdapter,
      contactCard: active.contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync,
      cachingMode,
      hardcodedRemoteId: hardcodedServerPeerId,
      syncRequestInterval,
      batchInterval: batchIntervalOverride,
      retryPendingFromStorage,
      enableCompaction,
      archiveThreshold: archiveThresholdOverride ?? archiveThreshold,
    })
  };

  const keyhiveNetworkAdapter = createKeyhiveNetworkAdapter(options.networkAdapter, onlyShareWithHardcodedServerPeerId, periodicallyRequestSync, syncRequestInterval, batchInterval, archiveThreshold);

  setupEventFlushListener(bootstrap, keyhiveNetworkAdapter, { automaticArchiveIngestion });

  return new AutomergeRepoKeyhive(
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    syncServer,
    keyhiveNetworkAdapter,
    emitter,
    keyhiveIdFactory(keyhiveNetworkAdapter, keyhive),
    createKeyhiveNetworkAdapter,
  );
}

/**
 * Build a keyhive instance and a `KeyhiveRustAdapter` backed by subduction,
 * sharing one private `PromiseQueue` across both. Use this for peers that
 * talk to a Rust subduction-keyhive sync server (SUK frames over a single
 * multiplexed WebSocket).
 *
 * Mirrors the keyhive setup in `initializeAutomergeRepoKeyhive` but routes
 * sync triggers through the Rust adapter instead of `KeyhiveNetworkAdapter`.
 */
export async function initializeAutomergeRepoKeyhiveRust(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  subduction: Subduction | Promise<Subduction>;
  /**
   * The Rust sync server's keyhive peer id. Defaults to the same
   * hardcoded demo server peer id used by the legacy hive's
   * `onlyShareWithHardcodedServerPeerId` path, so callers running
   * against the demo server don't need to plumb it through. Override
   * when running against a server with a different key, e.g. a local
   * dev server with its own `--ready-file`.
   */
  remotePeerId?: PeerId;
  keyPair?: CryptoKeyPair;
  automaticArchiveIngestion?: boolean;
  cachingMode?: "none" | "standard" | "periodic";
  syncRequestInterval?: number;
  periodicallyRequestSync?: boolean;
}): Promise<AutomergeRepoKeyhiveRust> {
  const {
    automaticArchiveIngestion = true,
    cachingMode = "standard" as "none" | "standard" | "periodic",
    syncRequestInterval = 2000,
    periodicallyRequestSync = true,
  } = options;

  console.warn(`[initRustHive] before bootstrapKeyhive suffix=${options.peerIdSuffix}`);
  const bootstrap = await bootstrapKeyhive(options);
  console.warn(`[initRustHive] after bootstrapKeyhive suffix=${options.peerIdSuffix}`);
  const { active, keyhive, keyhiveStorage, peerId, emitter, keyhiveQueue, serverPeerIdHardcoded } = bootstrap;

  const networkAdapter = new KeyhiveRustAdapter({
    subduction: options.subduction,
    keyhive,
    keyhiveStorage,
    keyhiveQueue,
    contactCard: active.contactCard,
    localPeerId: peerId,
    remotePeerId: options.remotePeerId ?? serverPeerIdHardcoded,
    cachingMode,
    syncRequestInterval,
    periodicallyRequestSync,
  });

  setupEventFlushListener(bootstrap, networkAdapter, { automaticArchiveIngestion });

  networkAdapter.syncKeyhive(true);

  // Lets the Rust hive fan additional tab connections (over MessageChannels)
  // out via legacy KeyhiveNetworkAdapters that share the same keyhive state.
  // Used by the patchwork-next service worker; mirrors the legacy hive's
  // closure shape so callers don't branch on hive variant.
  const createKeyhiveNetworkAdapter = (
    legacyAdapter: NetworkAdapter,
    onlyShareWithHardcodedServerPeerId: boolean,
    periodicallyRequestSyncFlag: boolean,
    syncRequestIntervalArg: number,
    batchIntervalOverride?: number,
    archiveThresholdOverride?: number,
  ): KeyhiveNetworkAdapter => {
    const hardcodedRemoteId = onlyShareWithHardcodedServerPeerId
      ? serverPeerIdHardcoded
      : null;
    return new KeyhiveNetworkAdapter({
      networkAdapter: legacyAdapter,
      contactCard: active.contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync: periodicallyRequestSyncFlag,
      cachingMode,
      hardcodedRemoteId,
      syncRequestInterval: syncRequestIntervalArg,
      batchInterval: batchIntervalOverride,
      retryPendingFromStorage: true,
      enableCompaction: true,
      archiveThreshold: archiveThresholdOverride ?? 200,
    });
  };

  const idFactory = async (_heads: import("@automerge/automerge-repo/slim").Heads) => {
    const doc = await generateDoc(keyhive);
    return doc.doc_id.toBytes();
  };

  return new AutomergeRepoKeyhiveRust(
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    networkAdapter,
    idFactory,
    createKeyhiveNetworkAdapter,
  );
}

export async function receiveContactCard(keyhive: Keyhive, contactCard: ContactCard, keyhiveStorage: KeyhiveStorage
  ): Promise<Individual | undefined> {
  let agent = await keyhive.getAgent(contactCard.id);
  if (agent) {
    return await keyhive.getIndividual(contactCard.individualId);
  } else {
    if (contactCard.op) {
      console.debug(`[AMRepoKeyhive] Saving Contact Card event: ${contactCard.op}`);
      await keyhiveStorage.saveEventWithHash(contactCard.op);
    } else {
      console.error(`[AMRepoKeyhive] No op found for ${contactCard.toJson()}`);
    }
    return await keyhive.receiveContactCard(contactCard);
  }
}

export async function getPendingOpHashes(keyhive: Keyhive): Promise<Uint8Array[]> {
  const pendingOps = await keyhive.pendingEventHashes();
  return pendingOps ? Array.from(pendingOps.keys()) as Uint8Array[] : [];
}

async function loadOrCreateKeyPairAndSigner(storage: StorageAdapterInterface, keyPair?: CryptoKeyPair): Promise<{keyPair: CryptoKeyPair, signer: Signer}> {
  if (keyPair) {
    await storeActiveKeyPair(keyPair, storage);
    const signer = await Signer.webCryptoSigner(keyPair);
    return { keyPair, signer };
  } else {
    return await loadOrCreateSigner(storage);
  }
}
export class KeyhiveStorage {
  private suppressEventWrites = false;

  isEventWriteSuppressed(): boolean {
    return this.suppressEventWrites;
  }

  async withSuppressedEventWrites<T>(fn: () => Promise<T>): Promise<T> {
    this.suppressEventWrites = true;
    try {
      return await fn();
    } finally {
      this.suppressEventWrites = false;
    }
  }

  constructor(
    private keyhiveStorageId: Uint8Array,
    private storage: StorageAdapterInterface
  ) {}

  private async removeNonPendingEvents(
    eventChunks: { key: StorageKey; data: Uint8Array | undefined }[],
    pendingKeys: StorageKey[],
  ): Promise<void> {
    for (const chunk of eventChunks) {
      const isPending = pendingKeys.some(
        (pk) => pk.length === chunk.key.length && pk.every((v, i) => v === chunk.key[i])
      );
      if (!isPending) {
        await this.storage.remove(chunk.key);
      }
    }
  }

  async saveKeyhiveWithHash(kh: Keyhive) {
    const khBytes = (await kh.toArchive()).toBytes();
    const hash = uint8ArrayToHex(this.keyhiveStorageId);
    console.debug(`[AMRepoKeyhive] Saving keyhive archive. Hash: ${hash}`);
    await this.storage.save(
      [KEYHIVE_DB_KEY, KEYHIVE_ARCHIVES_KEY, hash],
      khBytes
    );
  }

  async saveEventWithHash(event: KeyhiveEvent) {
    const eventBytes = event.toBytes();
    await this.saveEventBytesWithHash(eventBytes);
  }

  async saveEventBytesWithHash(eventBytes: Uint8Array) {
    // Copy into a fresh JS-owned ArrayBuffer in this realm so Node 22's
    // WebCrypto BufferSource brand-check accepts it. The network-path inputs
    // come through cbor-x and (across pnpm-hoisted module copies) sometimes
    // arrive as a plain Array of numbers or as a Uint8Array branded by a
    // different realm; the local-emit path comes from wasm-bindgen. The copy
    // normalizes all of those at the cost of one allocation per event.
    const canonical = new Uint8Array(eventBytes);
    const hash = await crypto.subtle.digest("SHA-256", canonical);
    await this.storage.save(
      [
        KEYHIVE_DB_KEY,
        KEYHIVE_EVENTS_KEY,
        uint8ArrayToHex(new Uint8Array(hash)),
      ],
      canonical
    );
  }

  async savePrekeySecrets(kh: Keyhive): Promise<void> {
    try {
      const bytes = await kh.exportPrekeySecrets();
      await this.storage.save([KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY], bytes);
    } catch (error) {
      console.error("[AMRepoKeyhive] Failed to export prekey secrets:", error);
    }
  }

  async loadPrekeySecrets(kh: Keyhive): Promise<void> {
    const data = await this.storage.load([KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY]);
    if (data) {
      await kh.importPrekeySecrets(data);
    }
  }

  async compact(kh: Keyhive): Promise<void> {
    const keyhiveArchiveChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_ARCHIVES_KEY,
    ]);
    const keyhiveEventsChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_EVENTS_KEY,
    ]);

    // Nothing to compact if no events and at most one archive
    if (keyhiveEventsChunks.length === 0 && keyhiveArchiveChunks.length <= 1) {
      return;
    }

    console.debug(
      `[AMRepoKeyhive] Compacting: ${keyhiveArchiveChunks.length} archives, ${keyhiveEventsChunks.length} events`
    );

    // Ingest all archives
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.data) {
        try {
          await kh.ingestArchive(new Archive(chunk.data));
        } catch (error) {
          console.warn(
            `[AMRepoKeyhive] Failed to ingest archive during compaction:`,
            error
          );
        }
      }
    }

    // Build map from event data to key for tracking pending events.
    // Uses string keys because ingestEventsBytes returns new Uint8Array
    // instances (copied across the WASM boundary), not the same references.
    const dataToKey = new Map<string, StorageKey>();
    for (const chunk of keyhiveEventsChunks) {
      if (chunk.data) {
        dataToKey.set(chunk.data.toString(), chunk.key);
      }
    }

    // Ingest all events
    const eventsBytes: Array<Uint8Array> = keyhiveEventsChunks
      .map((chunk) => chunk.data)
      .filter((data): data is Uint8Array => data !== undefined);

    let pendingKeys: StorageKey[] = [];
    if (eventsBytes.length > 0) {
      try {
        pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
          .map((bytes: Uint8Array) => dataToKey.get(bytes.toString()))
          .filter((key): key is StorageKey => key !== undefined);
      } catch (error) {
        console.warn(
          `[AMRepoKeyhive] Failed to ingest events during compaction:`,
          error
        );
      }
    }

    // Write the new compacted archive
    await this.saveKeyhiveWithHash(kh);

    // Remove old archives (skip the one we just wrote)
    const currentCompactHash = uint8ArrayToHex(this.keyhiveStorageId);
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.key[2] !== currentCompactHash) {
        await this.storage.remove(chunk.key);
      }
    }

    // Remove events that are not pending
    await this.removeNonPendingEvents(keyhiveEventsChunks, pendingKeys);

    console.debug(
      `[AMRepoKeyhive] Compaction complete. ${pendingKeys.length} pending events retained.`
    );
  }

  async ingestKeyhiveFromStorage(kh: Keyhive): Promise<void> {
    const keyhiveArchiveChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_ARCHIVES_KEY,
    ]);
    const keyhiveEventsChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_EVENTS_KEY,
    ]);

    // Ingest all archives
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.data) {
        console.debug(
          `[AMRepoKeyhive] Ingesting archive from storage. Hash: ${chunk.key[2]}`
        );
        try {
          await kh.ingestArchive(new Archive(chunk.data));
        } catch (error) {
          console.warn(
            `[AMRepoKeyhive] Failed to re-ingest archive during recovery:`,
            error
          );
        }
      }
    }

    // Ingest all events
    const eventsBytes: Array<Uint8Array> = keyhiveEventsChunks
      .map((chunk) => chunk.data)
      .filter((data): data is Uint8Array => data !== undefined);

    if (eventsBytes.length > 0) {
      console.debug(
        `[AMRepoKeyhive] Ingesting ${eventsBytes.length} events from storage`
      );
      try {
        await kh.ingestEventsBytes(eventsBytes);
      } catch (error) {
        console.warn(
          `[AMRepoKeyhive] Failed to ingest events during recovery:`,
          error
        );
      }
    }

    console.debug("[AMRepoKeyhive] Reading from storage completed");
  }

  async loadOrCreateKeyhive(
    signer: Signer,
    uniqueIdHash: Uint8Array,
    event_handler: (event: KeyhiveEvent) => void
  ): Promise<Keyhive> {
    const keyhiveArchiveChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_ARCHIVES_KEY,
    ]);
    const keyhiveEventsChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_EVENTS_KEY,
    ]);

    // Collect any individual events first.
    // Uses string keys because ingestEventsBytes returns new Uint8Array
    // instances (copied across the WASM boundary), not the same references.
    const dataToKey = new Map<string, string[]>();
    for (const chunk of keyhiveEventsChunks) {
      if (chunk.data) {
        dataToKey.set(chunk.data.toString(), chunk.key);
      }
    }
    const eventsBytes: Array<Uint8Array> = keyhiveEventsChunks
      .map((chunk) => chunk.data)
      .filter((data): data is Uint8Array => data !== undefined);

    if (keyhiveArchiveChunks.length > 0) {
      const firstChunk = keyhiveArchiveChunks[0];
      // TODO: Something went wrong if data is missing.
      if (firstChunk.data) {
        const firstArchive = new Archive(firstChunk.data);
        try {
          console.log("[AMRepoKeyhive] Attempting to load Keyhive archive");
          let store = CiphertextStore.newInMemory();
          const chunk_count = keyhiveArchiveChunks.length;
          console.log(
            `[AMRepoKeyhive] Ingesting archive from storage (1 of ${chunk_count}). Hash: ${firstChunk.key[2]}`
          );

          const kh = await firstArchive.tryToKeyhive(
            store,
            signer,
            event_handler
          );

          // Ingest additional archives
          for (let idx = 1; idx < keyhiveArchiveChunks.length; idx++) {
            const chunk = keyhiveArchiveChunks[idx];
            if (chunk.data) {
              console.log(
                `[AMRepoKeyhive] Ingesting archive from storage (${idx + 1} of ${chunk_count}). Hash: ${chunk.key[2]}`
              );
              await kh.ingestArchive(new Archive(chunk.data));
            }
          }

          // Ingest individual events
          console.log(
            `[AMRepoKeyhive] Ingesting ${eventsBytes.length} keyhive events from storage.`
          );
          let pendingKeys: StorageKey[] = [];
          if (eventsBytes.length > 0) {
            pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
              .map((bytes: Uint8Array) => dataToKey.get(bytes.toString()))
              .filter((key): key is StorageKey => key !== undefined);
          }

          await this.loadPrekeySecrets(kh);
          console.log(
            "[AMRepoKeyhive] Successfully loaded Keyhive from archive"
          );
          await this.saveKeyhiveWithHash(kh);
          const currentHash = uint8ArrayToHex(this.keyhiveStorageId);
          for (const chunk of keyhiveArchiveChunks) {
            if (chunk.key[2] !== currentHash) {
              await this.storage.remove(chunk.key);
            }
          }
          await this.removeNonPendingEvents(keyhiveEventsChunks, pendingKeys);
          return kh;
        } catch (error: unknown) {
          console.error(
            "[AMRepoKeyhive] Failed to load Keyhive archive:",
            unwrapWasmError(error)
          );
        }
      }
    }

    // No archives in storage. Create new keyhive
    const store = CiphertextStore.newInMemory();
    console.log(`[AMRepoKeyhive] Initializing new Keyhive`);
    const kh = await Keyhive.init(signer, store, event_handler);
    await this.loadPrekeySecrets(kh);

    if (eventsBytes.length > 0) {
      console.log(
        `[AMRepoKeyhive] Ingesting ${eventsBytes.length} keyhive events from storage.`
      );
      try {
        const pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
          .map((bytes: Uint8Array) => dataToKey.get(bytes.toString()))
          .filter((key): key is StorageKey => key !== undefined);

        await this.saveKeyhiveWithHash(kh);
        await this.removeNonPendingEvents(keyhiveEventsChunks, pendingKeys);
        return kh;
      } catch (e: unknown) {
        console.error(
          `[AMRepoKeyhive] Failed to ingest keyhive events from storage:`,
          unwrapWasmError(e)
        );
      }
    }

    await this.saveKeyhiveWithHash(kh);
    return kh;
  }
}

export type KeyhiveArchiveBytes = Uint8Array;
