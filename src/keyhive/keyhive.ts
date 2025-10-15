import { AutomergeUrl, Heads, NetworkAdapter, parseAutomergeUrl, PeerId, StorageAdapterInterface } from "@automerge/automerge-repo/slim";
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

export type AutomergeRepoKeyhive = {
  active: Active;
  keyhive: Keyhive;
  peerId: PeerId;
  syncServer: SyncServer;
  networkAdapter: KeyhiveNetworkAdapter;
  emitter: KeyhiveEventEmitter;
  idFactory: (heads: Heads) => Promise<Uint8Array>;
}

export async function initializeKeyhive(options: {
  storage: StorageAdapterInterface;
  peerIdSuffix: string;
  networkAdapter: NetworkAdapter;
  automaticArchiveIngestion: boolean;
}): Promise<AutomergeRepoKeyhive> {
  const { keyPair, signer } = await loadOrCreateSigner(options.storage);
  const emitter = new KeyhiveEventEmitter();
  const keyhive = await loadOrCreateKeyhive(
    options.storage,
    signer,
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
        console.error("[Adapter] Expected keyhive message not found")
        return
      }
      if (!("type" in msg)) {
        console.error("[Adapter] Invalid keyhive message")
        return
      }
      if (!msg.data) {
        console.error("[Adapter] Expected keyhive data not found")
        return
      }
      if (msg.data.length === 0) {
        console.error("[Adapter] Received empty archive data")
        return
      }
      console.debug(`[Adapter] Received keyhive from ${msg.senderId}. Archive size: ${msg.data.length} bytes. Ingesting archive.`);
      try {
        const archive = new Archive(msg.data);
        await keyhive.ingestArchive(archive);
        // FIXME: Move to keyhive if missing there
        emitter.emit("update");
        console.debug(`[Adapter] Successfully ingested archive from ${msg.senderId}`);
        // keyhiveNetworkAdapter.send(msg)
      } catch (error) {
        console.error(`Failed to ingest archive from ${msg.senderId}:`, error)
      }
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
  db: StorageAdapterInterface
) {
  const khBytes = (await kh.toArchive()).toBytes();
  const hash = await crypto.subtle.digest("SHA-256", new Uint8Array(khBytes));
  await db.save(
    [KEYHIVE_DB_KEY, uint8ArrayToHex(new Uint8Array(hash))],
    khBytes
  );
}

export type KeyhiveArchiveBytes = Uint8Array;

export function docIdFromAutomergeUrl(url: AutomergeUrl): KeyhiveDocumentId {
  const { binaryDocumentId } = parseAutomergeUrl(url);
  return new KeyhiveDocumentId(binaryDocumentId);
}

export async function loadOrCreateKeyhive(
  db: StorageAdapterInterface,
  signer: Signer,
  event_handler: (event: KeyhiveEvent) => void
): Promise<Keyhive> {
  const keyhiveArchiveChunks = await db.loadRange([KEYHIVE_DB_KEY]);
  if (keyhiveArchiveChunks.length > 0) {
    const firstChunk = keyhiveArchiveChunks[0];
    // TODO: Something went wrong if data is missing.
    if (firstChunk.data) {
      const firstArchive = new Archive(firstChunk.data);
      try {
        console.log("[Adapter] Attempting to load Keyhive archive");
        let store = CiphertextStore.newInMemory();
        const chunk_count = keyhiveArchiveChunks.length;
        console.log(`[Adapter] Ingesting archive from storage (1 of ${chunk_count}`);
        const kh = await firstArchive.tryToKeyhive(store, signer, event_handler);
        for (let idx = 1; idx < keyhiveArchiveChunks.length; idx++) {
          const chunk = keyhiveArchiveChunks[idx];
          if (chunk.data) {
            console.log(`[Adapter] Ingesting archive from storage (${idx + 1} of ${chunk_count}`);
            await kh.ingestArchive(new Archive(chunk.data));
          }
        }
        console.log("[Adapter] Successfully loaded Keyhive from archive");
        await saveKeyhiveWithHash(kh, db);
        for (const chunk of keyhiveArchiveChunks) {
          await db.remove(chunk.key);
        }
        return kh;
      } catch (error: unknown) {
        const jsError = (error as { toError: () => Error }).toError();
        console.error("[Adapter] Failed to load Keyhive archive:", jsError);
      }
    }
  }

  const store = CiphertextStore.newInMemory();
  const kh = await Keyhive.init(signer, store, event_handler);
  await saveKeyhiveWithHash(kh, db);
  return kh;
}
