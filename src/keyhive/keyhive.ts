import { log } from "../logging.js";
import { initKeyhiveWasm } from "../wasm.js";
import {
  AutomergeUrl,
  NetworkAdapter,
  parseAutomergeUrl,
  PeerId,
  type Repo,
  type RepoConfig,
  StorageAdapterInterface,
  StorageKey,
} from "@automerge/automerge-repo/slim";
import {
  arraysEqual,
  peerIdFromSigner,
  uint8ArrayToHex,
  unwrapWasmError,
} from "../utilities.js";
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
import {
  Active,
  createActive,
  loadOrCreateSigner,
  storeActiveKeyPair,
} from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { KeyhiveSubductionAdapter } from "../network-adapter/subduction-transport/keyhive-subduction-adapter.js";
import type { Subduction } from "@automerge/automerge-subduction/slim";
import { PromiseQueue } from "../network-adapter/pending.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import {
  LegacyAutomergeRepoKeyhive,
  AutomergeRepoKeyhive,
  type CreateKeyhiveNetworkAdapter,
  keyhiveIdFactory,
} from "./automerge-repo-keyhive.js";
import { KeyhiveBlobInterceptor } from "./blob-interceptor.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";
export const KEYHIVE_PREKEY_SECRETS_KEY = "/prekey-secrets";
const COMPACTION_LOCK = "automerge-repo-keyhive-compaction";

/**
 * @deprecated Kept public only for prebuilt bundles compiled against 0.3.
 * Will eventually be private. Every ARK method takes an `AutomergeUrl` directly,
 * so this is no longer required. Use {@link isUnprotectedDoc} to distinguish
 * keyhive documents from non-keyhive ones.
 */
export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

/** A sync server's contact card paired with its keyhive peer id. */
export interface SyncServerIdentity {
  contactCard: Uint8Array;
  peerId: PeerId;
}

/**
 * Selects which sync server to register as the relay, either an explicit
 * {@link SyncServerIdentity} or `"none"` to run without a sync server.
 * `"none"` is only supported on the network-adapter path (peer-to-peer
 * adapters, tests).
 */
export type SyncServerSelection = "none" | SyncServerIdentity;

/**
 * Shared keyhive context.
 */
interface KeyhiveContext {
  active: Active;
  keyhive: Keyhive;
  keyhiveStorage: KeyhiveStorage;
  peerId: PeerId;
  emitter: KeyhiveEventEmitter;
  keyhiveQueue: PromiseQueue;
  keyPair: CryptoKeyPair;
  signer: Signer;
  /** Null when the selection is "none". */
  syncServer: SyncServer | null;
  /** Null when the selection is "none". */
  serverPeerIdHardcoded: PeerId | null;
}

/** Resolve a {@link SyncServerSelection} to its card and peer id (null for "none"). */
function resolveSyncServer(
  selection: SyncServerSelection
): SyncServerIdentity | null {
  return selection === "none" ? null : selection;
}

/**
 * Identity and storage options, shared by both initialization functions.
 */
export interface KeyhiveIdentityOptions {
  /**
   * Storage for keyhive state: the identity key pair, archives, events, and
   * secrets.
   */
  storage: StorageAdapterInterface;
  /**
   * A readability label appended to the identity-derived peer id as
   * `-<label>-<random>`. ARK adds the `-<random>` component itself, so peers
   * sharing an identity (e.g., several tabs) never collide even with the same
   * label.
   */
  peerIdSuffix: string;
  /**
   * An existing extractable Ed25519 key pair to adopt as this identity,
   * instead of loading one from `storage` or generating a new one.
   */
  keyPair?: CryptoKeyPair;
  /**
   * Which sync server to register as the relay.
   *
   * The identity must match the server the repo actually connects to.
   * Pass `"none"` to run without one.
   */
  syncServer: SyncServerSelection;
}

/**
 * Sync-behaviour options shared by both initialization functions.
 */
export interface KeyhiveSyncOptions {
  /**
   * On keyhive changes, persist state and schedule a keyhive sync.
   * Default true.
   */
  automaticArchiveIngestion?: boolean;
  /**
   * Event cache strategy for the sync protocol. `"periodic"` caches sync
   * state and refreshes it on the `syncRequestInterval` timer.
   */
  cachingMode?: "none" | "periodic";
  /** Request keyhive sync from peers on an interval. Default true. */
  periodicallyRequestSync?: boolean;
  /**
   * Interval in ms for periodic sync requests, and for periodic cache
   * refresh. Default 2000.
   */
  syncRequestInterval?: number;
}

/** Options for building a hive on the automerge-repo network-adapter path. */
export interface LegacyHiveOptions
  extends KeyhiveIdentityOptions, KeyhiveSyncOptions {
  /** The raw adapter to wrap. Its traffic is signed and verified. */
  networkAdapter: NetworkAdapter;
  /**
   * Share only with the configured sync server. Messages from any other peer
   * are ignored. Default false. Throws if `syncServer` is `"none"`.
   */
  onlyShareWithSyncServer?: boolean;
  /** If set, batch outgoing keyhive messages on this interval in ms. */
  batchInterval?: number;
  /** Retry events left pending in storage on reconnect. Default true. */
  retryPendingFromStorage?: boolean;
  /** Compact events into an archive in the background. Default true. */
  enableCompaction?: boolean;
  /** Event count that triggers compaction into an archive. Default 200. */
  archiveThreshold?: number;
}

/** Options for building a hive for the subduction configuration. */
export interface SubductionHiveOptions
  extends KeyhiveIdentityOptions, KeyhiveSyncOptions {
  /** The subduction instance to sync over. */
  subduction: Subduction | Promise<Subduction>;
  /**
   * The single remote this hive syncs against. Defaults to the peer id of the
   * selected `syncServer`, and is therefore required when `syncServer` is
   * `"none"`.
   */
  remotePeerId?: PeerId;
}

async function bootstrapKeyhive(
  options: KeyhiveIdentityOptions
): Promise<KeyhiveContext> {
  const { keyPair, signer } = await loadOrCreateKeyPairAndSigner(
    options.storage,
    options.keyPair
  );
  const emitter = new KeyhiveEventEmitter();
  // Append a random component so concurrent peers sharing an identity (for
  // example, several tabs) never collide, even with the same label.
  const peerIdSuffix = `${options.peerIdSuffix}-${Math.random().toString(36).slice(2, 10)}`;
  const uniqueIdHash = new Uint8Array(
    await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(peerIdSuffix)
    )
  );
  const keyhiveStorage = new KeyhiveStorage(uniqueIdHash, options.storage);
  const keyhive = await keyhiveStorage.loadOrCreateKeyhive(
    signer,
    uniqueIdHash,
    emitter.handleKeyhiveEvent
  );
  const active = await createActive(keyPair, signer, keyhive);
  const peerId = peerIdFromSigner(active.signer, peerIdSuffix);

  const serverIdentity = resolveSyncServer(options.syncServer);
  const serverPeerIdHardcoded = serverIdentity?.peerId ?? null;
  const syncServer = serverIdentity
    ? await syncServerFromContactCard(
        serverIdentity.contactCard,
        serverIdentity.peerId,
        keyhive,
        keyhiveStorage
      )
    : null;

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
 * outbound sync triggers.
 */
interface KeyhiveSyncDriver {
  invalidateCaches(): void;
  syncKeyhive(): void;
}

/**
 * Wire the per-event flush and sync trigger onto the shared emitter.
 * Saves event bytes/prekey secrets to keyhive storage and, when there
 * are new events, asks the driver to invalidate its caches and schedule
 * an outbound sync after 1s.
 *
 * Returns a cleanup callback that detaches the listener and cancels any
 * pending sync timer. `AutomergeRepoKeyhiveBase.close()` runs it.
 */
function setupEventFlushListener(
  keyhiveContext: KeyhiveContext,
  driver: KeyhiveSyncDriver,
  options: { automaticArchiveIngestion: boolean }
): () => void {
  const { emitter, keyhiveQueue, keyhiveStorage, keyhive } = keyhiveContext;
  let syncTimeout: ReturnType<typeof setTimeout> | undefined;
  let pendingEventBytes: Uint8Array[] = [];
  let pendingPrekeySecrets = false;
  let pendingSync = false;
  let flushQueued = false;
  let closed = false;

  const onUpdate = (event: KeyhiveEvent) => {
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
      void keyhiveQueue
        .run(async () => {
          flushQueued = false;
          const eventsToSave = pendingEventBytes;
          const needPrekeySecrets = pendingPrekeySecrets;
          const needSync = pendingSync;
          pendingEventBytes = [];
          pendingPrekeySecrets = false;
          pendingSync = false;

          if (eventsToSave.length > 0) {
            log.debug(
              `[AMRepoKeyhive] Keyhive updated. Saving ${eventsToSave.length} events.`
            );
            for (const eventBytes of eventsToSave) {
              await keyhiveStorage.saveEventBytesWithHash(eventBytes);
            }
          }

          if (needPrekeySecrets) {
            await keyhiveStorage.savePrekeySecrets(keyhive);
          }

          if (eventsToSave.length > 0 || needSync) {
            // Invalidate caches so the next sync computes fresh totals.
            // Needed even when event writes are suppressed (e.g. during
            // ingestion from a Tab) because the keyhive state changed.
            driver.invalidateCaches();
          }

          if (needSync && !syncTimeout && !closed) {
            syncTimeout = setTimeout(() => {
              syncTimeout = undefined;
              driver.syncKeyhive();
            }, 1000);
          }
        })
        .catch((error) =>
          log.error("[AMRepoKeyhive] Event flush failed:", error)
        );
    }
  };

  emitter.on("update", onUpdate);

  return () => {
    closed = true;
    emitter.off("update", onUpdate);
    if (syncTimeout) {
      clearTimeout(syncTimeout);
      syncTimeout = undefined;
    }
  };
}

/** Build the hive for the legacy network adapter path (no repo wiring). */
async function buildLegacyHive(
  options: LegacyHiveOptions
): Promise<LegacyAutomergeRepoKeyhive> {
  const {
    automaticArchiveIngestion = true,
    onlyShareWithSyncServer = false,
    periodicallyRequestSync = true,
    cachingMode = "none" as "none" | "periodic",
    syncRequestInterval = 2000,
    batchInterval,
    retryPendingFromStorage = true,
    enableCompaction = true,
    archiveThreshold = 200,
  } = options;

  const bootstrap = await bootstrapKeyhive(options);
  const {
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    keyhiveQueue,
    syncServer,
    serverPeerIdHardcoded,
  } = bootstrap;

  const createKeyhiveNetworkAdapter: CreateKeyhiveNetworkAdapter = (
    networkAdapter,
    wrapOptions = {}
  ) => {
    const {
      onlyShareWithSyncServer: onlyServer = false,
      periodicallyRequestSync: periodicSync = false,
      syncRequestInterval: interval = 2000,
      batchInterval: batchIntervalOverride,
      archiveThreshold: archiveThresholdOverride,
    } = wrapOptions;
    if (onlyServer && !serverPeerIdHardcoded) {
      throw new Error(
        'onlyShareWithSyncServer requires a configured sync server (syncServer is "none")'
      );
    }

    return new KeyhiveNetworkAdapter({
      networkAdapter,
      contactCard: active.contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync: periodicSync,
      cachingMode,
      hardcodedRemoteId: onlyServer ? serverPeerIdHardcoded : null,
      syncRequestInterval: interval,
      batchInterval: batchIntervalOverride,
      retryPendingFromStorage,
      enableCompaction,
      archiveThreshold: archiveThresholdOverride ?? archiveThreshold,
    });
  };

  const keyhiveNetworkAdapter = createKeyhiveNetworkAdapter(
    options.networkAdapter,
    {
      onlyShareWithSyncServer,
      periodicallyRequestSync,
      syncRequestInterval,
      batchInterval,
      archiveThreshold,
    }
  );

  const cleanupEventFlushListener = setupEventFlushListener(
    bootstrap,
    keyhiveNetworkAdapter,
    { automaticArchiveIngestion }
  );

  await keyhiveStorage.savePrekeySecrets(keyhive);

  const hive = new LegacyAutomergeRepoKeyhive(
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    syncServer,
    keyhiveNetworkAdapter,
    emitter,
    keyhiveIdFactory(keyhive),
    createKeyhiveNetworkAdapter
  );
  hive.registerCleanup(cleanupEventFlushListener);
  return hive;
}

/**
 * Build the hive for the subduction configuration (no repo wiring): a keyhive
 * instance and a `KeyhiveSubductionAdapter` backed by subduction.
 */
async function buildHive(
  options: SubductionHiveOptions
): Promise<AutomergeRepoKeyhive> {
  const {
    automaticArchiveIngestion = true,
    cachingMode = "periodic" as "none" | "periodic",
    syncRequestInterval = 2000,
    periodicallyRequestSync = true,
  } = options;
  const keyhiveContext = await bootstrapKeyhive(options);
  const {
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    keyhiveQueue,
    serverPeerIdHardcoded,
  } = keyhiveContext;

  const remotePeerId = options.remotePeerId ?? serverPeerIdHardcoded;
  if (!remotePeerId) {
    throw new Error(
      'initializeAutomergeRepoKeyhive: the subduction configuration syncs against a single remote, so syncServer "none" requires an explicit remotePeerId'
    );
  }

  const networkAdapter = new KeyhiveSubductionAdapter({
    subduction: options.subduction,
    keyhive,
    keyhiveStorage,
    keyhiveQueue,
    contactCard: active.contactCard,
    localPeerId: peerId,
    remotePeerId,
    cachingMode,
    syncRequestInterval,
    periodicallyRequestSync,
  });

  const cleanupEventFlushListener = setupEventFlushListener(
    keyhiveContext,
    networkAdapter,
    { automaticArchiveIngestion }
  );

  await keyhiveStorage.savePrekeySecrets(keyhive);

  networkAdapter.syncKeyhive(true);

  const createKeyhiveNetworkAdapter: CreateKeyhiveNetworkAdapter = (
    legacyAdapter,
    wrapOptions = {}
  ) => {
    const {
      onlyShareWithSyncServer: onlyServer = false,
      periodicallyRequestSync: periodicSync = false,
      syncRequestInterval: interval = 2000,
      batchInterval: batchIntervalOverride,
      archiveThreshold: archiveThresholdOverride,
    } = wrapOptions;
    const adapter = new KeyhiveNetworkAdapter({
      networkAdapter: legacyAdapter,
      contactCard: active.contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync: periodicSync,
      cachingMode,
      hardcodedRemoteId: onlyServer ? serverPeerIdHardcoded : null,
      syncRequestInterval: interval,
      batchInterval: batchIntervalOverride,
      retryPendingFromStorage: true,
      enableCompaction: true,
      archiveThreshold: archiveThresholdOverride ?? 200,
    });
    const onEncrypt = () => adapter.invalidateCaches();
    emitter.on("encrypt", onEncrypt);
    const disconnect = adapter.disconnect.bind(adapter);
    adapter.disconnect = () => {
      emitter.off("encrypt", onEncrypt);
      disconnect();
    };
    return adapter;
  };

  const idFactory = keyhiveIdFactory(keyhive);

  const blobInterceptor = new KeyhiveBlobInterceptor(
    keyhive,
    keyhiveQueue,
    () => {
      emitter.emit("encrypt");
    },
    options.storage
  );

  const hive = new AutomergeRepoKeyhive(
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    networkAdapter,
    idFactory,
    createKeyhiveNetworkAdapter,
    blobInterceptor
  );
  hive.registerCleanup(cleanupEventFlushListener);
  return hive;
}

/** Options shared by both init functions for wiring the hive to its repo. */
export interface InitRepoLinkOptions {
  /**
   * Debounce interval for propagating keyhive membership changes to
   * `repo.shareConfigChanged()`. Default 2000 ms.
   */
  shareConfigDebounceMs?: number;
  /** Called immediately before each (debounced) `repo.shareConfigChanged()`. */
  onBeforeShareConfigChanged?: () => void;
}

/** Options for {@link initializeLegacyAutomergeRepoKeyhive}. */
export interface InitializeLegacyAutomergeRepoKeyhiveOptions
  extends LegacyHiveOptions, InitRepoLinkOptions {
  /** Constructs the `Repo` from a config. Usually `(config) => new Repo(config)`. */
  createRepo: (config: RepoConfig) => Repo;
  /**
   * Extra `RepoConfig` fields. The hive-derived fields are injected by the
   * init function and so are absent here.
   */
  repo?: Omit<RepoConfig, "peerId" | "idFactory" | "network">;
}

/** Options for {@link initializeAutomergeRepoKeyhive}. */
export interface InitializeAutomergeRepoKeyhiveOptions
  extends Omit<SubductionHiveOptions, "subduction">, InitRepoLinkOptions {
  /** Constructs the `Repo` from a config. Usually `(config) => new Repo(config)`. */
  createRepo: (config: RepoConfig) => Repo;
  /**
   * Extra `RepoConfig` fields. The hive-derived fields are injected by the
   * init function and so are absent here.
   */
  repo?: Omit<
    RepoConfig,
    "peerId" | "idFactory" | "signer" | "subductionBlobInterceptor"
  >;
}

/**
 * Construct a keyhive and its Automerge `Repo` together, for peers syncing
 * keyhive state over an automerge-repo network adapter.
 *
 * Initializes the keyhive WASM module, builds the hive, passes a repo config
 * with the hive-derived `network`, `peerId`, and `idFactory` to `createRepo`,
 * and links the resulting repo to the hive.
 */
export async function initializeLegacyAutomergeRepoKeyhive(
  options: InitializeLegacyAutomergeRepoKeyhiveOptions
): Promise<{ hive: LegacyAutomergeRepoKeyhive; repo: Repo }> {
  initKeyhiveWasm();
  const {
    createRepo,
    repo: repoOptions,
    shareConfigDebounceMs,
    onBeforeShareConfigChanged,
    ...hiveOptions
  } = options;
  const hive = await buildLegacyHive(hiveOptions);
  const repo = createRepo({
    ...repoOptions,
    network: [hive.networkAdapter],
    peerId: hive.peerId,
    idFactory: hive.idFactory,
  });
  hive.linkRepo(repo, {
    debounceMs: shareConfigDebounceMs,
    onBeforeShareConfigChanged,
  });
  return { hive, repo };
}

/**
 * Construct a keyhive and its Automerge `Repo` (connected to a Rust subduction
 * sync server) together.
 *
 * Initializes the keyhive WASM module and resolves the circular dependency
 * between hive and repo: the hive's network adapter needs the repo's
 * subduction instance, but the repo needs the hive first. This function
 * injects the hive-derived `signer`, `peerId`, `idFactory`, and
 * `subductionBlobInterceptor` into the repo config passed to `createRepo`,
 * hands `repo.subduction` back to the hive, and links the repo to the hive.
 */
export async function initializeAutomergeRepoKeyhive(
  options: InitializeAutomergeRepoKeyhiveOptions
): Promise<{ hive: AutomergeRepoKeyhive; repo: Repo }> {
  initKeyhiveWasm();
  const {
    createRepo,
    repo: repoOptions,
    shareConfigDebounceMs,
    onBeforeShareConfigChanged,
    ...hiveOptions
  } = options;

  // Hand the hive a deferred subduction promise, build the repo, then resolve
  // it from `repo.subduction`.
  let resolveSubduction!: (s: Subduction) => void;
  const subductionPromise = new Promise<Subduction>((resolve) => {
    resolveSubduction = resolve;
  });

  const hive = await buildHive({
    ...hiveOptions,
    subduction: subductionPromise,
  });

  const repo = createRepo({
    ...repoOptions,
    signer: await hive.constructSubductionSigner(),
    peerId: hive.peerId,
    idFactory: hive.idFactory,
    subductionBlobInterceptor: hive.blobInterceptor,
  });

  void repo.subduction
    .then(resolveSubduction as (s: unknown) => void)
    .catch((err) =>
      log.error("[AMRepoKeyhive] Failed to resolve repo.subduction:", err)
    );
  hive.linkRepo(repo, {
    debounceMs: shareConfigDebounceMs,
    onBeforeShareConfigChanged,
  });

  return { hive, repo };
}

export async function receiveContactCard(
  keyhive: Keyhive,
  contactCard: ContactCard,
  keyhiveStorage: KeyhiveStorage
): Promise<Individual | undefined> {
  if (!(await keyhive.hasIndividual(contactCard.individualId))) {
    log.debug(`[AMRepoKeyhive] Saving Contact Card event: ${contactCard.op}`);
    await keyhiveStorage.saveEventWithHash(contactCard.op);
    await keyhive.receiveContactCard(contactCard);
  }
  return keyhive.getIndividual(contactCard.individualId);
}

export async function getPendingOpHashes(
  keyhive: Keyhive
): Promise<Uint8Array[]> {
  const pendingOps = await keyhive.pendingEventHashes();
  return pendingOps ? (Array.from(pendingOps.keys()) as Uint8Array[]) : [];
}

async function loadOrCreateKeyPairAndSigner(
  storage: StorageAdapterInterface,
  keyPair?: CryptoKeyPair
): Promise<{ keyPair: CryptoKeyPair; signer: Signer }> {
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
    pendingKeys: StorageKey[]
  ): Promise<void> {
    for (const chunk of eventChunks) {
      const isPending = pendingKeys.some(
        (pk) =>
          pk.length === chunk.key.length &&
          pk.every((v, i) => v === chunk.key[i])
      );
      if (!isPending) {
        await this.storage.remove(chunk.key);
      }
    }
  }

  async saveKeyhiveWithHash(kh: Keyhive) {
    const khBytes = (await kh.toArchive()).toBytes();
    const hash = uint8ArrayToHex(this.keyhiveStorageId);
    log.debug(`[AMRepoKeyhive] Saving keyhive archive. Hash: ${hash}`);
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
    // Every step aborts the save on failure, in which case the step is logged.
    let step = "load stored prekey secrets";
    try {
      // Load existing secrets first so we don't overwrite secrets saved by another
      // instance.
      const existing = await this.storage.load([
        KEYHIVE_DB_KEY,
        KEYHIVE_PREKEY_SECRETS_KEY,
      ]);
      if (existing) {
        step = "import stored prekey secrets (merge with a sibling instance)";
        await kh.importPrekeySecrets(existing);
      }
      step = "export prekey secrets";
      const bytes = await kh.exportPrekeySecrets();
      step = "save prekey secrets";
      await this.storage.save(
        [KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY],
        bytes
      );
    } catch (error) {
      log.error(`[AMRepoKeyhive] Failed to ${step}:`, error);
    }
  }

  async loadPrekeySecrets(kh: Keyhive): Promise<void> {
    const data = await this.storage.load([
      KEYHIVE_DB_KEY,
      KEYHIVE_PREKEY_SECRETS_KEY,
    ]);
    if (data) {
      await kh.importPrekeySecrets(data);
    }
  }

  async compact(kh: Keyhive): Promise<void> {
    // Instances sharing this storage (tabs, workers) must not compact at the
    // same time, or each can remove the archive the other has just written.
    // Web Locks are shared by all tabs and workers of an origin. Where they are
    // unavailable (Node, insecure contexts), only the re-read before each
    // removal in compactUnlocked protects other instances' archives.
    const locks = globalThis.navigator?.locks;
    if (locks) {
      await locks.request(COMPACTION_LOCK, () => this.compactUnlocked(kh));
    } else {
      await this.compactUnlocked(kh);
    }
  }

  private async compactUnlocked(kh: Keyhive): Promise<void> {
    const keyhiveArchiveChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_ARCHIVES_KEY,
    ]);
    const keyhiveEventsChunks = await this.storage.loadRange([
      KEYHIVE_DB_KEY,
      KEYHIVE_EVENTS_KEY,
    ]);

    // Nothing to compact if no events and one archive. With no archive at
    // all, write ours: storage may have lost state this instance still holds.
    if (keyhiveEventsChunks.length === 0 && keyhiveArchiveChunks.length === 1) {
      return;
    }

    log.debug(
      `[AMRepoKeyhive] Compacting: ${keyhiveArchiveChunks.length} archives, ${keyhiveEventsChunks.length} events`
    );

    // Ingest all archives
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.data) {
        try {
          await kh.ingestArchive(new Archive(chunk.data));
        } catch (error) {
          log.warn(
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
        log.warn(
          `[AMRepoKeyhive] Failed to ingest events during compaction:`,
          error
        );
      }
    }

    // Write the new compacted archive
    await this.saveKeyhiveWithHash(kh);

    // Remove the archives we read and ingested, except the one we just wrote.
    // Another instance may have rewritten one since we read it, so re-read
    // each and keep it if it changed. The storage API has no
    // compare-and-remove, so a write landing between the re-read and the
    // removal is still lost.
    const currentCompactHash = uint8ArrayToHex(this.keyhiveStorageId);
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.key[2] === currentCompactHash || !chunk.data) continue;
      const current = await this.storage.load(chunk.key);
      if (current && arraysEqual(current, chunk.data)) {
        await this.storage.remove(chunk.key);
      }
    }

    // Remove events that are not pending
    await this.removeNonPendingEvents(keyhiveEventsChunks, pendingKeys);

    log.debug(
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
        log.debug(
          `[AMRepoKeyhive] Ingesting archive from storage. Hash: ${chunk.key[2]}`
        );
        try {
          await kh.ingestArchive(new Archive(chunk.data));
        } catch (error) {
          log.warn(
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
      log.debug(
        `[AMRepoKeyhive] Ingesting ${eventsBytes.length} events from storage`
      );
      try {
        await kh.ingestEventsBytes(eventsBytes);
      } catch (error) {
        log.warn(
          `[AMRepoKeyhive] Failed to ingest events during recovery:`,
          error
        );
      }
    }

    log.debug("[AMRepoKeyhive] Reading from storage completed");
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
          log.info("[AMRepoKeyhive] Attempting to load Keyhive archive");
          const store = CiphertextStore.newInMemory();
          const chunk_count = keyhiveArchiveChunks.length;
          log.info(
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
              log.info(
                `[AMRepoKeyhive] Ingesting archive from storage (${idx + 1} of ${chunk_count}). Hash: ${chunk.key[2]}`
              );
              await kh.ingestArchive(new Archive(chunk.data));
            }
          }

          // Ingest individual events
          log.info(
            `[AMRepoKeyhive] Ingesting ${eventsBytes.length} keyhive events from storage.`
          );
          let pendingKeys: StorageKey[] = [];
          if (eventsBytes.length > 0) {
            pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
              .map((bytes: Uint8Array) => dataToKey.get(bytes.toString()))
              .filter((key): key is StorageKey => key !== undefined);
          }

          await this.loadPrekeySecrets(kh);
          log.info("[AMRepoKeyhive] Successfully loaded Keyhive from archive");
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
          log.error(
            "[AMRepoKeyhive] Failed to load Keyhive archive:",
            unwrapWasmError(error)
          );
        }
      }
    }

    // No archives in storage. Create new keyhive.
    const store = CiphertextStore.newInMemory();
    log.info(`[AMRepoKeyhive] Initializing new Keyhive`);
    const kh = await Keyhive.init(signer, store, event_handler);
    await this.loadPrekeySecrets(kh);

    if (eventsBytes.length > 0) {
      log.info(
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
        log.error(
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
