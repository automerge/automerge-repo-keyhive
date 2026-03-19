import {
  AutomergeUrl,
  NetworkAdapter,
  parseAutomergeUrl,
  PeerId,
  StorageAdapterInterface,
  StorageKey,
} from "@automerge/automerge-repo/slim";
import { peerIdFromSigner, uint8ArrayToHex } from "../utilities.js";
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
import { syncServerFromContactCard } from "../sync-server.js";
import { createActive, loadOrCreateSigner, storeActiveKeyPair } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { PromiseQueue } from "../network-adapter/pending.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import { AutomergeRepoKeyhive, keyhiveIdFactory } from "./automerge-repo-keyhive.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";
export const KEYHIVE_PREKEY_SECRETS_KEY = "/prekey-secrets";

export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

export async function initializeAutomergeRepoKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  networkAdapter: NetworkAdapter;
  automaticArchiveIngestion?: boolean;
  onlyShareWithHardcodedServerPeerId?: boolean;
  periodicallyRequestSync?: boolean;
  cacheHashes?: boolean;
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
    cacheHashes = false,
    syncRequestInterval = 2000,
    batchInterval,
    retryPendingFromStorage = true,
    enableCompaction = true,
    archiveThreshold = 200,
  } = options;
  const { keyPair, signer } = await loadOrCreateKeyPairAndSigner(options.storage, options.keyPair)
  const emitter = new KeyhiveEventEmitter();
  const uniqueIdHash = new Uint8Array(
    await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(options.peerIdSuffix)
    )
  );
  const keyhiveStorage = new KeyhiveStorage(uniqueIdHash, options.storage);
  const keyhive = await keyhiveStorage.loadOrCreateKeyhive(
    signer,
    uniqueIdHash,
    emitter.handleKeyhiveEvent
  );
  const active = await createActive(keyPair, signer, keyhive);
  const peerId = peerIdFromSigner(active.signer, options.peerIdSuffix);

  // TODO: Server contact card and PeerId are currently just hardcoded for the demo
  const serverContactCardJson =
    '{"Rotate":{"payload":{"old":[73,163,230,244,111,233,153,119,133,211,134,237,111,36,52,131,22,50,54,144,150,45,227,235,128,36,33,217,190,198,55,75],"new":[109,115,204,144,178,114,182,238,113,124,4,139,249,76,220,44,128,104,194,68,187,184,82,241,94,145,104,198,159,122,186,43]},"issuer":[215,244,30,111,15,78,235,218,7,241,63,222,141,131,33,22,234,116,180,208,97,235,210,55,202,209,170,178,98,37,223,159],"signature":[178,64,85,76,51,199,196,151,129,14,191,53,127,191,34,223,97,238,95,109,118,179,152,17,205,188,204,177,116,166,147,231,192,201,48,137,19,214,180,45,108,104,34,8,14,63,115,139,215,142,4,179,233,89,150,218,174,168,107,23,8,109,228,6]}}';
  const serverPeerId = "1/Qebw9O69oH8T/ejYMhFup0tNBh69I3ytGqsmIl358=" as PeerId;

  const syncServer = await syncServerFromContactCard(
    serverContactCardJson,
    serverPeerId,
    keyhive,
    keyhiveStorage
  );

  const keyhiveQueue = new PromiseQueue();

  const createKeyhiveNetworkAdapter = (networkAdapter: NetworkAdapter, onlyShareWithHardcodedServerPeerId: boolean, periodicallyRequestSync: boolean, syncRequestInterval: number, batchIntervalOverride?: number, archiveThresholdOverride?: number) => {
    let hardcodedServerPeerId = null;
    if (onlyShareWithHardcodedServerPeerId) {
      hardcodedServerPeerId = serverPeerId
    }

    return new KeyhiveNetworkAdapter(
      networkAdapter,
      active.contactCard,
      keyhive,
      keyhiveStorage,
      keyhiveQueue,
      periodicallyRequestSync,
      cacheHashes,
      hardcodedServerPeerId,
      syncRequestInterval,
      batchIntervalOverride,
      retryPendingFromStorage,
      enableCompaction,
      archiveThresholdOverride ?? archiveThreshold,
    )
  };

  const keyhiveNetworkAdapter = createKeyhiveNetworkAdapter(options.networkAdapter, onlyShareWithHardcodedServerPeerId, periodicallyRequestSync, syncRequestInterval, batchInterval, archiveThreshold);

  let syncTimeout: ReturnType<typeof setTimeout> | undefined;
  let pendingEventBytes: Uint8Array[] = [];
  let pendingPrekeySecrets = false;
  let pendingSync = false;
  let flushQueued = false;

  emitter.on("update", (event: KeyhiveEvent) => {
    // When event writes are suppressed (during bulk ingestion in the network
    // adapter), skip capturing bytes to avoid duplicate writes.
    if (!keyhiveStorage.isEventWriteSuppressed()) {
      pendingEventBytes.push(event.toBytes());
    }

    if (automaticArchiveIngestion) {
      if (
        event.variant === "PREKEY_ROTATED" ||
        event.variant === "PREKEYS_EXPANDED"
      ) {
        pendingPrekeySecrets = true;
      }
      // TODO: We are currently filtering out CGKA ops in the sync protocol but
      // will need to restore them once we add encryption.
      if (event.variant !== "CGKA_OPERATION") {
        pendingSync = true;
      }
    }

    // Queue a single flush for all events that accumulate before the queue runs.
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

        if (needSync && !syncTimeout) {
          syncTimeout = setTimeout(() => {
            syncTimeout = undefined;
            keyhiveNetworkAdapter.syncKeyhive();
          }, 1000);
        }
      });
    }
  });

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

export async function receiveContactCard(keyhive: Keyhive, contactCard: ContactCard, keyhiveStorage: KeyhiveStorage
  ): Promise<Individual | undefined> {
  let agent = await keyhive.getAgent(contactCard.id);
  if (agent) {
    return await keyhive.getIndividual(contactCard.individualId);
  } else {
    if (contactCard.op) {
      console.debug(`[AMRepoKeyhive] Saving Contact Card event: ${contactCard.op}`);
      keyhiveStorage.saveEventWithHash(contactCard.op);
    } else {
      console.error(`[AMRepoKeyhive] No op found for ${contactCard.toJson()}`);
    }
    return await keyhive.receiveContactCard(contactCard);
  }
}

export async function getPendingOpHashes(keyhive: Keyhive): Promise<Uint8Array[]> {
  let pendingOpHashes: Uint8Array[] = new Array();
  const pendingOps = await keyhive.pendingEventHashes();
  if (pendingOps) {
    pendingOpHashes = Array.from(pendingOps.keys()) as Uint8Array[];
  }
  return pendingOpHashes;
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
    const hash = await crypto.subtle.digest(
      "SHA-256",
      new Uint8Array(eventBytes)
    );
    await this.storage.save(
      [
        KEYHIVE_DB_KEY,
        KEYHIVE_EVENTS_KEY,
        uint8ArrayToHex(new Uint8Array(hash)),
      ],
      eventBytes
    );
  }

  async savePrekeySecrets(kh: Keyhive): Promise<void> {
    const bytes = await kh.exportPrekeySecrets();
    await this.storage.save([KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY], bytes);
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

    // Build map from event data to key for tracking pending events
    const dataToKey = new Map<Uint8Array, StorageKey>();
    for (const chunk of keyhiveEventsChunks) {
      if (chunk.data) {
        dataToKey.set(chunk.data, chunk.key);
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
          .map((bytes: Uint8Array) => dataToKey.get(bytes))
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
    for (const chunk of keyhiveEventsChunks) {
      const isPendingKey = pendingKeys.some(
        (pendingKey) =>
          pendingKey.length === chunk.key.length &&
          pendingKey.every((val, index) => val === chunk.key[index])
      );

      if (!isPendingKey) {
        await this.storage.remove(chunk.key);
      }
    }

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

    // Collect any individual events first
    const data_to_key: Map<Uint8Array, string[]> = new Map();
    for (const chunk of keyhiveEventsChunks) {
      if (chunk.data) {
        data_to_key.set(chunk.data, chunk.key);
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
              .map((bytes: Uint8Array) => data_to_key.get(bytes))
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
          for (const chunk of keyhiveEventsChunks) {
            const isPendingKey = pendingKeys.some(
              (pendingKey) =>
                pendingKey.length === chunk.key.length &&
                pendingKey.every((val, index) => val === chunk.key[index])
            );

            if (!isPendingKey) {
              await this.storage.remove(chunk.key);
            }
          }
          return kh;
        } catch (error: unknown) {
          // @ts-ignore
          const jsError =
            error && typeof error == "object" && "toError" in error
              ? // @ts-ignore
                error.toError()
              : error;
          console.error(
            "[AMRepoKeyhive] Failed to load Keyhive archive:",
            jsError
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
          .map((bytes: Uint8Array) => data_to_key.get(bytes))
          .filter((key): key is StorageKey => key !== undefined);

        await this.saveKeyhiveWithHash(kh);
        for (const chunk of keyhiveEventsChunks) {
          const isPendingKey = pendingKeys.some(
            (pendingKey) =>
              pendingKey.length === chunk.key.length &&
              pendingKey.every((val, index) => val === chunk.key[index])
          );

          if (!isPendingKey) {
            await this.storage.remove(chunk.key);
          }
        }
        return kh;
      } catch (e: unknown) {
        // @ts-ignore
        const jsError =
          // @ts-ignore
          e && typeof e == "object" && "toError" in e ? e.toError() : e;
        console.error(
          `[AMRepoKeyhive] Failed to ingest keyhive events from storage: ${jsError}`
        );
      }
    }

    await this.saveKeyhiveWithHash(kh);
    return kh;
  }
}

export type KeyhiveArchiveBytes = Uint8Array;
