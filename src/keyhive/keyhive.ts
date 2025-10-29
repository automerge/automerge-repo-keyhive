import { AutomergeUrl, Heads, NetworkAdapter, parseAutomergeUrl, PeerId, StorageAdapterInterface, StorageKey } from "@automerge/automerge-repo/slim";
import { peerIdFromSigner, uint8ArrayToHex } from "../utilities.js";
import {
  Archive,
  CiphertextStore,
  DocumentId as KeyhiveDocumentId,
  Event as KeyhiveEvent,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import { SyncServer, syncServerFromContactCard } from "../sync-server.js";
import { Active, createActive, loadOrCreateSigner } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { keyhiveIdFactory } from "./doc.js";
import { KeyhiveEventEmitter } from "./emitter.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";

export type AutomergeRepoKeyhive = {
  active: Active;
  keyhive: Keyhive;
  peerId: PeerId;
  syncServer: SyncServer;
  networkAdapter: KeyhiveNetworkAdapter;
  emitter: KeyhiveEventEmitter;
  idFactory: (heads: Heads) => Promise<Uint8Array>;
}

export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

export async function initializeAutomergeRepoKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  networkAdapter: NetworkAdapter;
  automaticArchiveIngestion: boolean;
}): Promise<AutomergeRepoKeyhive> {
  const { keyPair, signer } = await loadOrCreateSigner(options.storage);
  const emitter = new KeyhiveEventEmitter();
  const uniqueIdHash = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(options.peerIdSuffix)));
  const keyhive = await loadOrCreateKeyhive(
    options.storage,
    signer,
    uniqueIdHash,
    emitter.handleKeyhiveEvent,
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
    keyhive
  );

  const keyhiveNetworkAdapter = new KeyhiveNetworkAdapter(
    options.networkAdapter,
    keyhive,
    options.storage,
    peerId
  );

  if (options.automaticArchiveIngestion) {
    // TODO: This event is currently ad hoc
    (keyhiveNetworkAdapter as any).on("keyhive", async (msg: any) => {
      if (!msg) {
        console.error("[AMRepoKeyhive] Expected keyhive message not found")
        return
      }
      if (!("type" in msg)) {
        console.error("[AMRepoKeyhive] Invalid keyhive message")
        return
      }
      if (!msg.data) {
        console.error("[AMRepoKeyhive] Expected keyhive data not found")
        return
      }
      if (msg.data.length === 0) {
        console.error("[AMRepoKeyhive] Received empty archive data")
        return
      }
      console.debug(`[AMRepoKeyhive] Received keyhive from ${msg.senderId}. Archive size: ${msg.data.length} bytes. Ingesting archive.`);
      try {
        const archive = new Archive(msg.data);
        await keyhive.ingestArchive(archive);
        // TODO: Move to keyhive if missing there?
        emitter.emit("ingest");
        await saveKeyhiveWithHash(keyhive, options.storage, uniqueIdHash);
        console.debug(`[AMRepoKeyhive] Successfully ingested archive from ${msg.senderId}`);
      } catch (error) {
        console.error(`Failed to ingest archive from ${msg.senderId}:`, error)
      }
    })

    emitter.on("update", async (event: KeyhiveEvent) => {
      console.debug("[AMRepoKeyhive] Keyhive updated. Saving and syncing.");
      await saveEventWithHash(event, options.storage);
      keyhiveNetworkAdapter.syncKeyhive(keyhive);
    })
  }

  return {
    active,
    keyhive,
    peerId,
    syncServer,
    networkAdapter: keyhiveNetworkAdapter,
    emitter,
    idFactory: keyhiveIdFactory(keyhiveNetworkAdapter, keyhive),
  };
}

export async function saveKeyhiveWithHash(
  kh: Keyhive,
  db: StorageAdapterInterface,
  peerIdSuffix: Uint8Array,
) {
  const khBytes = (await kh.toArchive()).toBytes();
  const hash = uint8ArrayToHex(peerIdSuffix);
  console.debug(`[AMRepoKeyhive] Saving keyhive archive. Hash: ${hash}`);
  await db.save(
    [KEYHIVE_DB_KEY, KEYHIVE_ARCHIVES_KEY, hash],
    khBytes
  );
}

export async function saveEventWithHash(
  event: KeyhiveEvent,
  db: StorageAdapterInterface
) {
  const eventBytes = event.toBytes();
  const hash = await crypto.subtle.digest("SHA-256", new Uint8Array(eventBytes));
  await db.save(
    [KEYHIVE_DB_KEY, KEYHIVE_EVENTS_KEY, uint8ArrayToHex(new Uint8Array(hash))],
    eventBytes
  );
}

export type KeyhiveArchiveBytes = Uint8Array;

async function loadOrCreateKeyhive(
  db: StorageAdapterInterface,
  signer: Signer,
  uniqueIdHash: Uint8Array,
  event_handler: (event: KeyhiveEvent) => void
): Promise<Keyhive> {
  const keyhiveArchiveChunks = await db.loadRange([KEYHIVE_DB_KEY, KEYHIVE_ARCHIVES_KEY]);
  const keyhiveEventsChunks = await db.loadRange([KEYHIVE_DB_KEY, KEYHIVE_EVENTS_KEY]);

  // Collect any individual events first
  const data_to_key: Map<Uint8Array, string[]> = new Map();
  for (const chunk of keyhiveEventsChunks) {
    if (chunk.data) {
      data_to_key.set(chunk.data, chunk.key);
    }
  }
  const eventsBytes: Array<Uint8Array> = keyhiveEventsChunks
    .map(chunk => chunk.data)
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
        console.log(`[AMRepoKeyhive] Ingesting archive from storage (1 of ${chunk_count}). Hash: ${firstChunk.key[2]}`);

        const kh = await firstArchive.tryToKeyhive(store, signer, event_handler);

        // Ingest additional archives
        for (let idx = 1; idx < keyhiveArchiveChunks.length; idx++) {
          const chunk = keyhiveArchiveChunks[idx];
          if (chunk.data) {
            console.log(`[AMRepoKeyhive] Ingesting archive from storage (${idx + 1} of ${chunk_count}). Hash: ${chunk.key[2]}`);
            await kh.ingestArchive(new Archive(chunk.data));
          }
        }

        // Ingest individual events
        console.log(`[AMRepoKeyhive] Ingesting ${eventsBytes.length} keyhive events from storage.`)
        let pendingKeys: StorageKey[] = [];
        if (eventsBytes.length > 0) {
          pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
            .map((bytes: Uint8Array) => data_to_key.get(bytes))
            .filter((key): key is StorageKey => key !== undefined);
        }

        console.log("[AMRepoKeyhive] Successfully loaded Keyhive from archive");
        await saveKeyhiveWithHash(kh, db, uniqueIdHash);
        for (const chunk of keyhiveArchiveChunks) {
          await db.remove(chunk.key);
        }
        for (const chunk of keyhiveEventsChunks) {
          const isPendingKey = pendingKeys.some(pendingKey =>
            pendingKey.length === chunk.key.length &&
            pendingKey.every((val, index) => val === chunk.key[index])
          );

          if (!isPendingKey) {
            await db.remove(chunk.key);
          }
        }
        return kh;
      } catch (error: unknown) {
        // @ts-ignore
        const jsError = (error && typeof error == "object" && "toError" in error) ? error.toError() : error
        console.error("[AMRepoKeyhive] Failed to load Keyhive archive:", jsError);
      }
    }
  }

  // No archives in storage. Create new keyhive
  const store = CiphertextStore.newInMemory();
  console.log(`[AMRepoKeyhive] Initializing new Keyhive`);
  const kh = await Keyhive.init(signer, store, event_handler);

  if (eventsBytes.length > 0) {
    console.log(`[AMRepoKeyhive] Ingesting ${eventsBytes.length} keyhive events from storage.`)
    try {
      const pendingKeys = (await kh.ingestEventsBytes(eventsBytes))
        .map((bytes: Uint8Array) => data_to_key.get(bytes))
        .filter((key): key is StorageKey => key !== undefined);

      await saveKeyhiveWithHash(kh, db, uniqueIdHash);
      for (const chunk of keyhiveEventsChunks) {
        const isPendingKey = pendingKeys.some(pendingKey =>
          pendingKey.length === chunk.key.length &&
          pendingKey.every((val, index) => val === chunk.key[index])
        );

        if (!isPendingKey) {
          await db.remove(chunk.key);
        }
      }
      return kh;
    } catch (e: unknown) {
      // @ts-ignore
      const jsError = (e && typeof e == "object" && "toError" in e) ? e.toError() : e
      console.error(`[AMRepoKeyhive] Failed to ingest keyhive events from storage: ${jsError}`)
    }
  }

  await saveKeyhiveWithHash(kh, db, uniqueIdHash);
  return kh;
}
