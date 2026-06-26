import { type AutomergeUrl, type BlobInterceptor, type DocumentId, type StorageAdapterInterface, parseAutomergeUrl } from "@automerge/automerge-repo/slim";
import {
  ChangeId,
  DocumentId as KeyhiveDocumentId,
  Encrypted,
  Keyhive,
  symmetricEncrypt,
  symmetricDecrypt,
} from "@keyhive/keyhive/slim";
import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex, hexToBytes } from "@noble/hashes/utils.js";
import { PromiseQueue } from "../network-adapter/pending.js";
import { arraysEqual } from "../utilities.js";
import { KEYHIVE_DB_KEY, KEYHIVE_LEAF_SECRETS_KEY, KEYHIVE_PREKEY_SECRETS_KEY } from "./keyhive.js";

const PCS_KEY_HASHES_STORAGE_KEY = "/pcs-key-hashes";

// Outer envelope wire format (around keyhive's content envelope):
//   [1] version
//   [4] inner length (uint32 LE)
//   [.] inner = keyhive EncryptedContent.serialize()
//   [.] predsCipher = symmetricEncrypt(selfKey, predecessor entries)
// Each predecessor entry (inside predsCipher, once decrypted) is 64 bytes:
//   [32] predecessor commit id  [32] that predecessor's application secret.
const ENVELOPE_VERSION = 1;
const COMMIT_ID_BYTES = 32;
const PRED_ENTRY_BYTES = COMMIT_ID_BYTES + 32;

export class KeyhiveBlobInterceptor implements BlobInterceptor {
  #keyhive: Keyhive;
  #queue: PromiseQueue;
  #onEncrypted?: () => void;
  #storage?: StorageAdapterInterface;
  #lastPcsKeyHash: Map<string, Uint8Array> = new Map();
  #persistQueued = false;

  // commitId hex -> the 32-byte application secret used to encrypt/decrypt that
  // blob. Populated on every encrypt and decrypt. On encrypt it lets us attach
  // a blob's predecessors' keys to its outgoing envelope; on decrypt it lets a
  // descendant's chain supply the key for an ancestor we could not reach via
  // CGKA (a member added later reading prior history).
  #blobKeys: Map<string, Uint8Array> = new Map();

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
  // Byte-length of the prekey-secrets archive blob last imported, so a miss
  // only re-imports it when a sibling instance has rewritten it.
  #importedPrekeyArchiveLen = -1;

  // Import secrets another instance of this identity wrote to shared storage but
  // this instance has not yet seen. Both stores hold `importPrekeySecrets`-shaped
  // blobs ("leaf secret" == prekey secret); they are the archive/events split:
  //   - KEYHIVE_LEAF_SECRETS_KEY: incremental events (e.g. a forcePcsUpdate
  //     rotation secret), cursor-tracked by entry hash.
  //   - KEYHIVE_PREKEY_SECRETS_KEY: the consolidated archive (full export),
  //     rewritten when a prekey is created/rotated. A live miss must re-read it
  //     too (not just init/refresh) to pick up a prekey a sibling just created.
  // Returns true if any new secret was imported. Called only on an
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

      // Re-read the consolidated prekey-secrets archive when it has grown/changed.
      // Length is a cheap change signal: the archive only ever accumulates
      // secrets, so a different length means a sibling added one.
      const archive = await this.#storage.load([KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY]);
      if (archive && archive.byteLength !== this.#importedPrekeyArchiveLen) {
        total++;
        await this.#keyhive.importPrekeySecrets(archive);
        this.#importedPrekeyArchiveLen = archive.byteLength;
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
    commitId: string,
    parents: string[],
    blob: Uint8Array,
    loadBlob?: (commitIdHex: string) => Promise<Uint8Array | null>
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
      const result = await this.#keyhive.tryEncryptKeyed(doc, contentRef, [], blob);
      this.#onEncrypted?.();
      const encrypted = result.encrypted_content();
      const selfKey = result.applicationSecret;

      // Attach each parent's application secret, encrypted under this blob's own
      // key: the external predecessor-secret chain. If a parent's key is not in
      // the in-memory cache (cold after a restart, or encrypted by a sibling
      // instance), recover it by decrypting the parent's stored blob rather than
      // skipping the link. A skipped link permanently strands that parent for
      // any reader that can only reach it through the chain (e.g. a public or
      // late-joining reader). Re-derivation stores no keys at rest.
      const entries: Uint8Array[] = [];
      for (const parentHex of parents) {
        const idBytes = hexToBytes(parentHex);
        if (idBytes.length !== COMMIT_ID_BYTES) continue;
        let parentKey = this.#blobKeys.get(parentHex);
        if (!parentKey && loadBlob) {
          parentKey = await this.#recoverParentKey(doc, parentHex, loadBlob);
        }
        if (!parentKey) continue;
        const entry = new Uint8Array(PRED_ENTRY_BYTES);
        entry.set(idBytes, 0);
        entry.set(parentKey, COMMIT_ID_BYTES);
        entries.push(entry);
      }
      const predsCipher = symmetricEncrypt(
        selfKey,
        concatBytes(entries),
        hexToBytes(commitId)
      );
      const out = encodeOuterEnvelope(encrypted.serialize(), predsCipher);

      this.#blobKeys.set(commitId, selfKey);

      const newHash = encrypted.pcs_key_hash;
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
    commitId: string,
    blob: Uint8Array,
    loadBlob?: (commitIdHex: string) => Promise<Uint8Array | null>
  ): Promise<Uint8Array | null> {
    const { binaryDocumentId, legacy } = parseDocId(documentId);
    if (legacy) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(new KeyhiveDocumentId(binaryDocumentId));
      if (!doc) return null;

      // Decrypt this blob and, on a cold load, walk its predecessor chain by
      // loading just the parent blobs it points at. Collect this blob's
      // plaintext plus any ancestor plaintexts the walk recovers; their
      // concatenation is a valid `loadIncremental` input.
      const out: Uint8Array[] = [];
      const seen = new Set<string>();
      const ok = await this.#decryptAndWalk(doc, commitId, blob, loadBlob, out, seen);
      if (!ok) return null;
      return out.length === 1 ? out[0] : concatBytes(out);
    });
  }

  // Decrypt one blob, then follow its predecessor-secret chain: recover each
  // parent's key from the decrypted preds and, when `loadBlob` is available,
  // load just those parent blobs and recurse. Pushes every recovered plaintext
  // (this blob first, then ancestors) into `out`. Returns false only when THIS
  // blob cannot be decrypted (the caller leaves it pending for a later pass).
  async #decryptAndWalk(
    doc: Awaited<ReturnType<Keyhive["getDocument"]>>,
    commitId: string,
    blob: Uint8Array,
    loadBlob: ((commitIdHex: string) => Promise<Uint8Array | null>) | undefined,
    out: Uint8Array[],
    seen: Set<string>
  ): Promise<boolean> {
    const envelope = decodeOuterEnvelope(blob);
    if (!envelope) return false;
    let encrypted: Encrypted;
    try {
      encrypted = Encrypted.fromBytes(envelope.inner);
    } catch {
      return false;
    }

    // Resolve this blob's application secret: from the chain map if a
    // descendant already supplied it (keyed by commit id), otherwise via a
    // single CGKA dive (the entry point). The bulk path passes an empty commit
    // id, so a cold entry point is reached only through the CGKA dive.
    let selfKey = commitId ? this.#blobKeys.get(commitId) : undefined;
    let plaintext: Uint8Array;
    if (selfKey) {
      try {
        plaintext = encrypted.decryptWithKey(selfKey);
      } catch {
        return false;
      }
    } else {
      const viaCgka = await this.#decryptViaCgka(doc, encrypted);
      if (!viaCgka) return false;
      plaintext = viaCgka.plaintext;
      selfKey = viaCgka.applicationSecret;
    }
    if (commitId) this.#blobKeys.set(commitId, selfKey);
    out.push(plaintext);

    // Recover predecessors' keys and walk into the parents we can load, so
    // earlier blobs decrypt now instead of waiting for a separate pass. A
    // corrupt or unexpected preds section must not fail the content decrypt
    // that already succeeded.
    let preds: Array<{ idHex: string; key: Uint8Array }>;
    try {
      preds = decodePreds(symmetricDecrypt(selfKey, envelope.predsCipher));
    } catch {
      return true;
    }
    for (const { idHex, key } of preds) {
      if (!this.#blobKeys.has(idHex)) this.#blobKeys.set(idHex, key);
      if (seen.has(idHex) || !loadBlob) continue;
      seen.add(idHex);
      let parentBlob: Uint8Array | null;
      try {
        parentBlob = await loadBlob(idHex);
      } catch {
        continue;
      }
      if (parentBlob) {
        await this.#decryptAndWalk(doc, idHex, parentBlob, loadBlob, out, seen);
      }
    }
    return true;
  }

  // Recover a parent blob's application secret at encrypt time by loading its
  // stored envelope and decrypting it (CGKA dive). Caches the result and any of
  // the parent's own predecessor keys it carries. Returns undefined if the blob
  // is absent or cannot be decrypted (then the link is skipped as before).
  async #recoverParentKey(
    doc: Awaited<ReturnType<Keyhive["getDocument"]>>,
    parentHex: string,
    loadBlob: (commitIdHex: string) => Promise<Uint8Array | null>
  ): Promise<Uint8Array | undefined> {
    let parentBlob: Uint8Array | null;
    try {
      parentBlob = await loadBlob(parentHex);
    } catch {
      return undefined;
    }
    if (!parentBlob) return undefined;
    const envelope = decodeOuterEnvelope(parentBlob);
    if (!envelope) return undefined;
    let encrypted: Encrypted;
    try {
      encrypted = Encrypted.fromBytes(envelope.inner);
    } catch {
      return undefined;
    }
    const viaCgka = await this.#decryptViaCgka(doc, encrypted);
    if (!viaCgka) return undefined;
    const key = viaCgka.applicationSecret;
    this.#blobKeys.set(parentHex, key);
    // Seed the parent's own predecessors so deeper links also fill in.
    try {
      const predsPlain = symmetricDecrypt(key, envelope.predsCipher);
      for (const { idHex, key: k } of decodePreds(predsPlain)) {
        if (!this.#blobKeys.has(idHex)) this.#blobKeys.set(idHex, k);
      }
    } catch {
      // Ignore a corrupt preds section.
    }
    return key;
  }

  // Recover a blob's application secret through keyhive/CGKA (the chain entry
  // point), importing sibling-written leaf secrets and retrying once on a miss.
  async #decryptViaCgka(
    doc: Awaited<ReturnType<Keyhive["getDocument"]>>,
    encrypted: Encrypted
  ): Promise<{ plaintext: Uint8Array; applicationSecret: Uint8Array } | null> {
    try {
      const res = await this.#keyhive.tryDecryptKeyed(doc!, encrypted);
      return { plaintext: res.plaintext, applicationSecret: res.applicationSecret };
    } catch {
      if ((await this.#importNewLeafSecrets()).imported) {
        try {
          const res = await this.#keyhive.tryDecryptKeyed(doc!, encrypted);
          return { plaintext: res.plaintext, applicationSecret: res.applicationSecret };
        } catch {
          return null;
        }
      }
      return null;
    }
  }
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function encodeOuterEnvelope(inner: Uint8Array, predsCipher: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + inner.length + predsCipher.length);
  out[0] = ENVELOPE_VERSION;
  new DataView(out.buffer).setUint32(1, inner.length, true);
  out.set(inner, 5);
  out.set(predsCipher, 5 + inner.length);
  return out;
}

function decodeOuterEnvelope(
  bytes: Uint8Array
): { inner: Uint8Array; predsCipher: Uint8Array } | null {
  if (bytes.length < 5 || bytes[0] !== ENVELOPE_VERSION) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const innerLen = view.getUint32(1, true);
  if (5 + innerLen > bytes.length) return null;
  return {
    inner: bytes.subarray(5, 5 + innerLen),
    predsCipher: bytes.subarray(5 + innerLen),
  };
}

function decodePreds(plain: Uint8Array): Array<{ idHex: string; key: Uint8Array }> {
  const out: Array<{ idHex: string; key: Uint8Array }> = [];
  for (let off = 0; off + PRED_ENTRY_BYTES <= plain.length; off += PRED_ENTRY_BYTES) {
    out.push({
      idHex: bytesToHex(plain.subarray(off, off + COMMIT_ID_BYTES)),
      key: plain.slice(off + COMMIT_ID_BYTES, off + PRED_ENTRY_BYTES),
    });
  }
  return out;
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
