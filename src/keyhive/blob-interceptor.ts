import { type AutomergeUrl, type BlobInterceptor, type DocumentId, type StorageAdapterInterface, parseAutomergeUrl } from "@automerge/automerge-repo/slim";
import {
  ChangeId,
  DocumentId as KeyhiveDocumentId,
  Encrypted,
  Keyhive,
} from "@keyhive/keyhive/slim";
import { blake3 } from "@noble/hashes/blake3.js";
import { PromiseQueue } from "../network-adapter/pending.js";
import { arraysEqual } from "../utilities.js";
import { KEYHIVE_DB_KEY, KEYHIVE_LEAF_SECRETS_KEY } from "./keyhive.js";

const PCS_KEY_HASHES_STORAGE_KEY = "/pcs-key-hashes";

export class KeyhiveBlobInterceptor implements BlobInterceptor {
  #keyhive: Keyhive;
  #queue: PromiseQueue;
  #onEncrypted?: () => void;
  #storage?: StorageAdapterInterface;
  #lastPcsKeyHash: Map<string, Uint8Array> = new Map();
  #persistQueued = false;

  constructor(keyhive: Keyhive, queue: PromiseQueue, onEncrypted?: () => void, storage?: StorageAdapterInterface) {
    this.#keyhive = keyhive;
    this.#queue = queue;
    this.#onEncrypted = onEncrypted;
    this.#storage = storage;
  }

  async loadPersistedPcsKeyHashes(): Promise<void> {
    if (!this.#storage) return;
    const data = await this.#storage.load([KEYHIVE_DB_KEY, PCS_KEY_HASHES_STORAGE_KEY]);
    if (!data) return;
    try {
      const decoded: Record<string, number[]> = JSON.parse(new TextDecoder().decode(data));
      for (const [docId, hashArray] of Object.entries(decoded)) {
        this.#lastPcsKeyHash.set(docId, new Uint8Array(hashArray));
      }
    } catch {
      // Ignore invalid persisted data.
    }
  }

  // Hashes of leaf-secret storage entries already imported into this keyhive,
  // so a miss only imports entries it hasn't seen.
  #importedLeafSecrets: Set<string> = new Set();

  // Import leaf secrets that another instance of this identity wrote to shared
  // storage (e.g. after a forcePcsUpdate on the tab) but this instance has not
  // yet seen. Returns true if any new secret was imported. Called only on an
  // encrypt/decrypt miss, so the common path pays nothing.
  async #importNewLeafSecrets(): Promise<{
    imported: boolean;
    total: number;
    newlyImported: number;
  }> {
    if (!this.#storage) return { imported: false, total: 0, newlyImported: 0 };
    let newlyImported = 0;
    let total = 0;
    try {
      const chunks = await this.#storage.loadRange([KEYHIVE_DB_KEY, KEYHIVE_LEAF_SECRETS_KEY]);
      for (const chunk of chunks) {
        if (!chunk.data) continue;
        total++;
        const hashKey = chunk.key[chunk.key.length - 1] as string;
        if (this.#importedLeafSecrets.has(hashKey)) continue;
        await this.#keyhive.importPrekeySecrets(chunk.data);
        this.#importedLeafSecrets.add(hashKey);
        newlyImported++;
      }
    } catch (e) {
      console.error("[KeyhiveBlobInterceptor] importNewLeafSecrets failed:", e);
    }
    return { imported: newlyImported > 0, total, newlyImported };
  }

  #queuePersist(): void {
    if (!this.#storage || this.#persistQueued) return;
    this.#persistQueued = true;
    queueMicrotask(() => {
      this.#persistQueued = false;
      void this.#persistPcsKeyHashes().catch((e) => {
        console.error("[KeyhiveBlobInterceptor] persisting PCS key hashes failed:", e);
      });
    });
  }

  async #persistPcsKeyHashes(): Promise<void> {
    if (!this.#storage) return;
    const obj: Record<string, number[]> = {};
    for (const [docId, hash] of this.#lastPcsKeyHash) {
      obj[docId] = Array.from(hash);
    }
    const bytes = new TextEncoder().encode(JSON.stringify(obj));
    await this.#storage.save([KEYHIVE_DB_KEY, PCS_KEY_HASHES_STORAGE_KEY], bytes);
  }

  get trackedDocIds(): string[] {
    return [...this.#lastPcsKeyHash.keys()];
  }

  lastPcsKeyHashForDoc(documentId: string): Uint8Array | undefined {
    return this.#lastPcsKeyHash.get(documentId);
  }

  async transformOutgoing(
    documentId: DocumentId,
    blob: Uint8Array
  ): Promise<Uint8Array | null> {
    const { binaryDocumentId, legacy } = parseDocId(documentId);
    if (legacy) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(new KeyhiveDocumentId(binaryDocumentId));
      // No keyhive doc yet: drop the outgoing blob (nothing stored or pushed).
      if (!doc) return null;
      let pcsHash = await this.#keyhive.tryPcsKeyHash(doc);
      if (!pcsHash) {
        // A sibling instance (e.g. the tab) may have rotated the key and written
        // the new leaf secret to shared storage. Import and retry once.
        await this.#importNewLeafSecrets();
        pcsHash = await this.#keyhive.tryPcsKeyHash(doc);
        if (!pcsHash) return null;
      }
      const contentRef = new ChangeId(blake3(blob));
      const result = await this.#keyhive.tryEncrypt(doc, contentRef, [], blob);
      this.#onEncrypted?.();
      const encrypted = result.encrypted_content();
      const newHash = encrypted.pcs_key_hash;
      const out = encrypted.serialize();
      const oldHash = this.#lastPcsKeyHash.get(documentId);
      if (!oldHash || !arraysEqual(oldHash, newHash)) {
        this.#lastPcsKeyHash.set(documentId, newHash);
        this.#queuePersist();
      }
      return out;
    });
  }

  async transformIncoming(
    documentId: DocumentId,
    blob: Uint8Array
  ): Promise<Uint8Array | null> {
    const { binaryDocumentId, legacy } = parseDocId(documentId);
    if (legacy) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(new KeyhiveDocumentId(binaryDocumentId));
      if (!doc) return null;
      let encrypted: Encrypted;
      try {
        encrypted = Encrypted.fromBytes(blob);
      } catch {
        return null;
      }
      try {
        return await this.#keyhive.tryDecrypt(doc, encrypted);
      } catch {
        // Miss: a sibling instance may have rotated the key. Import any new leaf
        // secrets from shared storage and retry once before giving up.
        if ((await this.#importNewLeafSecrets()).imported) {
          try {
            return await this.#keyhive.tryDecrypt(doc, encrypted);
          } catch {
            // fall through
          }
        }
        return null;
      }
    });
  }
}

// Parse the document id once, returning both the raw bytes (for building a
// KeyhiveDocumentId) and whether it is a legacy id.
function parseDocId(documentId: DocumentId): { binaryDocumentId: Uint8Array; legacy: boolean } {
  const { binaryDocumentId } = parseAutomergeUrl(`automerge:${documentId}` as AutomergeUrl);
  return { binaryDocumentId, legacy: isLegacyDocIdBytes(binaryDocumentId) };
}

function isLegacyDocIdBytes(binaryDocumentId: Uint8Array): boolean {
  if (binaryDocumentId.length < 32) return true;
  for (let i = 16; i < 32; i++) {
    if (binaryDocumentId[i] !== 0) return false;
  }
  return true;
}
