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
  generateDoc,
  keyhiveIdFactory,
} from "./automerge-repo-keyhive.js";
import { KeyhiveBlobInterceptor } from "./blob-interceptor.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";
export const KEYHIVE_PREKEY_SECRETS_KEY = "/prekey-secrets";
export const KEYHIVE_LEAF_SECRETS_KEY = "/leaf-secrets/";

export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

/** A sync server's signed contact card JSON paired with its keyhive peer id. */
export interface SyncServerIdentity {
  contactCardJson: string;
  peerId: PeerId;
}

/**
 * Selects which sync server to register as the relay.
 *
 * Pass `"subduction"` or `"keyhive"` to use a known server, an explicit
 * {@link SyncServerIdentity} to target a custom server, or `"none"` to run
 * without a sync server. `"none"` is only supported on the network-adapter
 * path (peer-to-peer adapters, tests).
 */
export type SyncServerSelection =
  "subduction" | "keyhive" | "none" | SyncServerIdentity;

/**
 * Shared keyhive context: signer, storage, keyhive instance, active,
 * peerId, emitter, queue, and the selected syncServer.
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

// TODO: these sync-server identities are temporarily hardcoded.

/** keyhive.sync.automerge.org keyhive contact card (issuer d7f41e6f…). */
export const KEYHIVE_SYNC_SERVER_CONTACT_CARD_JSON =
  '{"Rotate":{"payload":{"old":[73,163,230,244,111,233,153,119,133,211,134,237,111,36,52,131,22,50,54,144,150,45,227,235,128,36,33,217,190,198,55,75],"new":[109,115,204,144,178,114,182,238,113,124,4,139,249,76,220,44,128,104,194,68,187,184,82,241,94,145,104,198,159,122,186,43]},"issuer":[215,244,30,111,15,78,235,218,7,241,63,222,141,131,33,22,234,116,180,208,97,235,210,55,202,209,170,178,98,37,223,159],"signature":[178,64,85,76,51,199,196,151,129,14,191,53,127,191,34,223,97,238,95,109,118,179,152,17,205,188,204,177,116,166,147,231,192,201,48,137,19,214,180,45,108,104,34,8,14,63,115,139,215,142,4,179,233,89,150,218,174,168,107,23,8,109,228,6]}}';
/** keyhive.sync.automerge.org subduction peer id. */
export const KEYHIVE_SYNC_SERVER_PEER_ID =
  "1/Qebw9O69oH8T/ejYMhFup0tNBh69I3ytGqsmIl358=" as PeerId;

/** subduction.sync.inkandswitch.com keyhive contact card (issuer f709c58c…),
 * captured over the wire from its MissingContactCard at subduction-transport/codec.ts. */
export const SUBDUCTION_SYNC_SERVER_CONTACT_CARD_JSON =
  '{"Rotate":{"payload":{"old":[33,137,111,238,179,125,3,30,133,139,50,112,178,153,171,132,174,170,136,174,242,17,131,192,37,125,253,108,91,65,84,110],"new":[34,214,236,200,87,89,211,226,207,54,34,177,181,49,195,126,244,174,192,163,6,174,225,45,147,0,82,73,79,78,196,60]},"issuer":[247,9,197,140,25,81,187,131,117,217,148,149,178,185,64,74,187,27,132,103,122,139,165,214,120,158,18,104,148,65,76,25],"signature":[92,213,174,209,83,196,175,199,100,26,241,68,150,220,89,224,87,196,169,73,234,232,74,212,191,3,116,4,138,189,221,44,177,140,139,161,19,139,164,49,224,197,190,25,144,134,90,157,255,86,184,5,184,94,70,127,22,40,43,34,200,182,65,8]}}';
/** subduction.sync.inkandswitch.com subduction peer id. */
export const SUBDUCTION_SYNC_SERVER_PEER_ID =
  "9wnFjBlRu4N12ZSVsrlASrsbhGd6i6XWeJ4SaJRBTBk=" as PeerId;

// Known sync-server identities, keyed by name. Each card and peer id are a
// matched pair, so selecting by name can never mismatch them.
const SYNC_SERVERS: Record<"subduction" | "keyhive", SyncServerIdentity> = {
  subduction: {
    contactCardJson: SUBDUCTION_SYNC_SERVER_CONTACT_CARD_JSON,
    peerId: SUBDUCTION_SYNC_SERVER_PEER_ID,
  },
  keyhive: {
    contactCardJson: KEYHIVE_SYNC_SERVER_CONTACT_CARD_JSON,
    peerId: KEYHIVE_SYNC_SERVER_PEER_ID,
  },
};

/** Resolve a {@link SyncServerSelection} to its card + peer id (null for "none"). */
function resolveSyncServer(
  selection?: SyncServerSelection
): SyncServerIdentity | null {
  if (selection === undefined) {
    log.warn(
      "[AMRepoKeyhive] No syncServer selection was passed; defaulting to the " +
        "subduction.sync.inkandswitch.com identity. Pass syncServer " +
        '("subduction", "keyhive", "none", or a custom identity) explicitly. ' +
        "The identity must match the server the repo actually connects to."
    );
    return SYNC_SERVERS.subduction;
  }
  if (selection === "none") return null;
  return typeof selection === "string" ? SYNC_SERVERS[selection] : selection;
}

async function bootstrapKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  keyPair?: CryptoKeyPair;
  /** Which sync server to register as the relay. Defaults to subduction.sync. */
  syncServer?: SyncServerSelection;
}): Promise<KeyhiveContext> {
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
        serverIdentity.contactCardJson,
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
 */
function setupEventFlushListener(
  bootstrap: KeyhiveContext,
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

          if (needSync && !syncTimeout) {
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
  });
}

/** Build the hive for the legacy network adapter path (no repo wiring). */
async function buildLegacyHive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  networkAdapter: NetworkAdapter;
  automaticArchiveIngestion?: boolean;
  onlyShareWithSyncServer?: boolean;
  periodicallyRequestSync?: boolean;
  cachingMode?: "none" | "periodic";
  keyPair?: CryptoKeyPair;
  syncServer?: SyncServerSelection;
  syncRequestInterval?: number;
  batchInterval?: number;
  retryPendingFromStorage?: boolean;
  enableCompaction?: boolean;
  archiveThreshold?: number;
}): Promise<LegacyAutomergeRepoKeyhive> {
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

  setupEventFlushListener(bootstrap, keyhiveNetworkAdapter, {
    automaticArchiveIngestion,
  });

  await keyhiveStorage.savePrekeySecrets(keyhive);

  return new LegacyAutomergeRepoKeyhive(
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    syncServer,
    keyhiveNetworkAdapter,
    emitter,
    keyhiveIdFactory(keyhiveNetworkAdapter, keyhive),
    createKeyhiveNetworkAdapter
  );
}

/**
 * Build the hive for the subduction path (no repo wiring): a keyhive
 * instance and a `KeyhiveSubductionAdapter` backed by subduction.
 */
async function buildHive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  subduction: Subduction | Promise<Subduction>;
  remotePeerId?: PeerId;
  keyPair?: CryptoKeyPair;
  syncServer?: SyncServerSelection;
  automaticArchiveIngestion?: boolean;
  cachingMode?: "none" | "periodic";
  syncRequestInterval?: number;
  periodicallyRequestSync?: boolean;
}): Promise<AutomergeRepoKeyhive> {
  const {
    automaticArchiveIngestion = true,
    cachingMode = "periodic" as "none" | "periodic",
    syncRequestInterval = 2000,
    periodicallyRequestSync = true,
  } = options;
  const bootstrap = await bootstrapKeyhive(options);
  const {
    active,
    keyhive,
    keyhiveStorage,
    peerId,
    emitter,
    keyhiveQueue,
    serverPeerIdHardcoded,
  } = bootstrap;

  const remotePeerId = options.remotePeerId ?? serverPeerIdHardcoded;
  if (!remotePeerId) {
    throw new Error(
      'initializeAutomergeRepoKeyhive: the subduction path syncs against a single remote, so syncServer "none" requires an explicit remotePeerId'
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

  setupEventFlushListener(bootstrap, networkAdapter, {
    automaticArchiveIngestion,
  });

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

  const idFactory = async (
    _heads: import("@automerge/automerge-repo/slim").Heads
  ) => {
    const doc = await generateDoc(keyhive);
    return doc.doc_id.toBytes();
  };

  const blobInterceptor = new KeyhiveBlobInterceptor(
    keyhive,
    keyhiveQueue,
    () => {
      emitter.emit("encrypt");
    },
    options.storage
  );
  await blobInterceptor.loadPersistedPcsKeyHashes();

  return new AutomergeRepoKeyhive(
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
}

/** Options shared by both init functions for wiring the hive to its repo. */
interface InitRepoLinkOptions {
  /**
   * Debounce interval for propagating keyhive membership changes to
   * `repo.shareConfigChanged()`. Default 2000 ms.
   */
  shareConfigDebounceMs?: number;
  /** Called immediately before each (debounced) `repo.shareConfigChanged()`. */
  onBeforeShareConfigChanged?: () => void;
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
  options: Parameters<typeof buildLegacyHive>[0] &
    InitRepoLinkOptions & {
      createRepo: (config: RepoConfig) => Repo;
      repo?: Omit<RepoConfig, "peerId" | "idFactory" | "network">;
    }
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
  options: Omit<Parameters<typeof buildHive>[0], "subduction"> &
    InitRepoLinkOptions & {
      createRepo: (config: RepoConfig) => Repo;
      repo?: Omit<
        RepoConfig,
        "peerId" | "idFactory" | "signer" | "subductionBlobInterceptor"
      >;
    }
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
  const agent = await keyhive.getAgent(contactCard.id);
  if (agent) {
    return await keyhive.getIndividual(contactCard.individualId);
  } else {
    if (contactCard.op) {
      log.debug(`[AMRepoKeyhive] Saving Contact Card event: ${contactCard.op}`);
      await keyhiveStorage.saveEventWithHash(contactCard.op);
    } else {
      log.error(`[AMRepoKeyhive] No op found for ${contactCard.toJson()}`);
    }
    return await keyhive.receiveContactCard(contactCard);
  }
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

  // Persist a single rotated leaf secret (the bytes returned by
  // `forcePcsUpdate`) as one content-addressed entry, so a sibling instance of
  // this identity can pick up just the new secret on an encrypt/decrypt miss.
  async saveLeafSecret(secretBytes: Uint8Array): Promise<string> {
    const canonical = new Uint8Array(secretBytes);
    const hash = await crypto.subtle.digest("SHA-256", canonical);
    const hashHex = uint8ArrayToHex(new Uint8Array(hash));
    await this.storage.save(
      [KEYHIVE_DB_KEY, KEYHIVE_LEAF_SECRETS_KEY, hashHex],
      canonical
    );
    return hashHex;
  }

  async savePrekeySecrets(kh: Keyhive): Promise<void> {
    try {
      // Load existing secrets first so we don't overwrite secrets saved by another
      // instance.
      const existing = await this.storage.load([
        KEYHIVE_DB_KEY,
        KEYHIVE_PREKEY_SECRETS_KEY,
      ]);
      if (existing) {
        await kh.importPrekeySecrets(existing);
      }
      const bytes = await kh.exportPrekeySecrets();
      await this.storage.save(
        [KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY],
        bytes
      );
    } catch (error) {
      log.error("[AMRepoKeyhive] Failed to export prekey secrets:", error);
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

    // Remove old archives (skip the one we just wrote)
    const currentCompactHash = uint8ArrayToHex(this.keyhiveStorageId);
    for (const chunk of keyhiveArchiveChunks) {
      if (chunk.key[2] !== currentCompactHash) {
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
