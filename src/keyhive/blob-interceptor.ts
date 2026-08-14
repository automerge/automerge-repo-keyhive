import { log } from "../logging.js";
import {
  type AutomergeUrl,
  type BlobInterceptor,
  type DocumentId,
  type StorageAdapterInterface,
  parseAutomergeUrl,
} from "@automerge/automerge-repo/slim";
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
import { arraysEqual, isUnprotectedDocId } from "../utilities.js";
import {
  KEYHIVE_DB_KEY,
  KEYHIVE_LEAF_SECRETS_KEY,
  KEYHIVE_PREKEY_SECRETS_KEY,
} from "./keyhive.js";

const PCS_KEY_HASHES_STORAGE_KEY = "/pcs-key-hashes";

// Outer envelope wire format (around keyhive's content envelope):
//   [1] version
//   [4] inner length (uint32 LE)
//   [.] inner = keyhive EncryptedContent.serialize()
//   [.] predsCipher = symmetricEncrypt(selfKey, predecessor entries)
// Each predecessor entry (inside predsCipher, once decrypted) is 64 bytes:
//   [32] predecessor commit id  [32] that predecessor's application secret.
/** @internal Exported for tests. */
export const ENVELOPE_VERSION = 1;
/** @internal Exported for tests. */
export const COMMIT_ID_BYTES = 32;
/** @internal Exported for tests. */
export const PRED_ENTRY_BYTES = COMMIT_ID_BYTES + 32;

type KeyhiveDoc = NonNullable<Awaited<ReturnType<Keyhive["getDocument"]>>>;

export class KeyhiveBlobInterceptor implements BlobInterceptor {
  #keyhive: Keyhive;
  #queue: PromiseQueue;
  #onEncrypted?: () => void;
  #storage?: StorageAdapterInterface;
  #lastPcsKeyHash: Map<string, Uint8Array> = new Map();
  #persistQueued = false;
  // Docs dropped because of a missing PCS key. A rotation will restore it.
  #docsAwaitingPcsKey: Set<string> = new Set();

  // Hashes of leaf-secret storage entries already imported into this keyhive,
  // so a miss only imports entries it hasn't seen.
  #importedLeafSecrets: Set<string> = new Set();
  // Byte-length of the prekey-secrets archive blob last imported, so a miss
  // only re-imports it when a sibling instance has rewritten it.
  #importedPrekeyArchiveLen = -1;

  // commitId hex -> the 32-byte application secret used to encrypt/decrypt that
  // blob. Populated on every encrypt and decrypt. On encrypt it lets us attach
  // a blob's predecessors' keys to its outgoing envelope; on decrypt it lets a
  // descendant's chain supply the key for an ancestor we could not reach via
  // CGKA (a member added later reading prior history).
  #blobKeys: Map<string, Uint8Array> = new Map();

  // commitId hexes whose plaintext has already been returned to
  // `loadIncremental` (and so applied to the doc). Once a blob is delivered its
  // whole loadable ancestor subtree was delivered with it, so a later incoming
  // descendant can stop the predecessor walk here instead of re-loading and
  // re-decrypting the entire history on every message.
  #delivered: Set<string> = new Set();

  constructor(
    keyhive: Keyhive,
    queue: PromiseQueue,
    onEncrypted?: () => void,
    storage?: StorageAdapterInterface
  ) {
    this.#keyhive = keyhive;
    this.#queue = queue;
    this.#onEncrypted = onEncrypted;
    this.#storage = storage;
  }

  /**
   * Drop the cached key material and its delivery bookkeeping.
   */
  clearCachedKeyMaterial(): void {
    this.#blobKeys.clear();
    this.#delivered.clear();
    this.#importedLeafSecrets.clear();
  }

  async loadPersistedPcsKeyHashes(): Promise<void> {
    if (!this.#storage) return;
    const data = await this.#storage.load([
      KEYHIVE_DB_KEY,
      PCS_KEY_HASHES_STORAGE_KEY,
    ]);
    if (!data) return;
    try {
      const decoded: Record<string, number[]> = JSON.parse(
        new TextDecoder().decode(data)
      );
      for (const [docId, hashArray] of Object.entries(decoded)) {
        this.#lastPcsKeyHash.set(docId, new Uint8Array(hashArray));
      }
    } catch {
      // Ignore invalid persisted data.
    }
  }

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
  async #importNewLeafSecrets(): Promise<boolean> {
    if (!this.#storage) return false;
    let newlyImported = 0;
    try {
      const chunks = await this.#storage.loadRange([
        KEYHIVE_DB_KEY,
        KEYHIVE_LEAF_SECRETS_KEY,
      ]);
      for (const chunk of chunks) {
        if (!chunk.data) continue;
        const hashKey = chunk.key[chunk.key.length - 1] as string;
        if (this.#importedLeafSecrets.has(hashKey)) continue;
        await this.#keyhive.importPrekeySecrets(chunk.data);
        this.#importedLeafSecrets.add(hashKey);
        newlyImported++;
      }

      // Re-read the consolidated prekey-secrets archive when it has grown/changed.
      // Length is a cheap change signal: the archive only ever accumulates
      // secrets, so a different length means a sibling added one.
      const archive = await this.#storage.load([
        KEYHIVE_DB_KEY,
        KEYHIVE_PREKEY_SECRETS_KEY,
      ]);
      if (archive && archive.byteLength !== this.#importedPrekeyArchiveLen) {
        await this.#keyhive.importPrekeySecrets(archive);
        this.#importedPrekeyArchiveLen = archive.byteLength;
        newlyImported++;
      }
    } catch (e) {
      log.error("[KeyhiveBlobInterceptor] importNewLeafSecrets failed:", e);
    }
    return newlyImported > 0;
  }

  #queuePersist(): void {
    if (!this.#storage || this.#persistQueued) return;
    this.#persistQueued = true;
    queueMicrotask(() => {
      this.#persistQueued = false;
      void this.#persistPcsKeyHashes().catch((e) => {
        log.error(
          "[KeyhiveBlobInterceptor] persisting PCS key hashes failed:",
          e
        );
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
    await this.#storage.save(
      [KEYHIVE_DB_KEY, PCS_KEY_HASHES_STORAGE_KEY],
      bytes
    );
  }

  get trackedDocIds(): string[] {
    return [...this.#lastPcsKeyHash.keys()];
  }

  get docIdsAwaitingPcsKey(): string[] {
    return [...this.#docsAwaitingPcsKey];
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
    const { binaryDocumentId, unprotected } = parseDocId(documentId);
    if (unprotected) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(
        new KeyhiveDocumentId(binaryDocumentId)
      );
      // No keyhive doc yet: drop the outgoing blob (nothing stored or pushed).
      if (!doc) {
        log.debug(
          `[KeyhiveBlobInterceptor] transformOutgoing: no keyhive document for ${documentId}; dropping outgoing blob (a later save retries)`
        );
        return null;
      }
      let pcsHash = await this.#keyhive.tryPcsKeyHash(doc);
      if (!pcsHash) {
        // A sibling instance (e.g. the tab) may have rotated the key and written
        // the new leaf secret to shared storage. Import and retry once.
        await this.#importNewLeafSecrets();
        pcsHash = await this.#keyhive.tryPcsKeyHash(doc);
        if (!pcsHash) {
          // Either a sibling's rotation we cannot see yet or a member add
          // that invalidated the key (the membership nudge rotation restores
          // the key in that case).
          this.#docsAwaitingPcsKey.add(documentId);
          log.debug(
            `[KeyhiveBlobInterceptor] transformOutgoing: no derivable PCS key for ${documentId} (even after importing leaf secrets); dropping outgoing blob (a later save retries)`
          );
          return null;
        }
      }
      this.#docsAwaitingPcsKey.delete(documentId);
      const contentRef = new ChangeId(blake3(blob));
      const result = await this.#keyhive.tryEncryptKeyed(
        doc,
        contentRef,
        [],
        blob
      );
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
      // The commit id rides along as "associated data", but it is never
      // authenticated or checked on decrypt. Its only effect is as one input
      // to the deterministic nonce derivation (key, plaintext, associated
      // data). The derived nonce is carried in the sealed blob, so
      // symmetricDecrypt reads it from there and needs no associated data.
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
    const { binaryDocumentId, unprotected } = parseDocId(documentId);
    if (unprotected) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(
        new KeyhiveDocumentId(binaryDocumentId)
      );
      if (!doc) return null;

      // Decrypt this blob and, on a cold load, walk its predecessor chain by
      // loading just the parent blobs it points at. Collect this blob's
      // plaintext plus any ancestor plaintexts the walk recovers. Their
      // concatenation is a valid `loadIncremental` input.
      const out: Uint8Array[] = [];
      const seen = new Set<string>();
      const ok = await this.#decryptAndWalk(
        doc,
        commitId,
        blob,
        loadBlob,
        out,
        seen
      );
      if (!ok) return null;
      return out.length === 1 ? out[0] : concatBytes(out);
    });
  }

  // Decrypt one blob, then follow its predecessor-secret chain. Recover each
  // parent's key from the decrypted preds and, when `loadBlob` is available,
  // load just those parent blobs and recurse. Pushes every recovered plaintext
  // (this blob first, then ancestors) into `out`. Returns false only when this
  // blob cannot be decrypted (the caller leaves it pending for a later pass).
  async #decryptAndWalk(
    doc: KeyhiveDoc,
    commitId: string,
    blob: Uint8Array,
    loadBlob: ((commitIdHex: string) => Promise<Uint8Array | null>) | undefined,
    out: Uint8Array[],
    seen: Set<string>
  ): Promise<boolean> {
    const parsed = parseEnvelope(blob);
    if (!parsed) {
      log.debug(
        `[KeyhiveBlobInterceptor] unrecognized blob envelope${
          commitId ? ` for commit ${commitId}` : " during bulk document load"
        }; leaving pending`
      );
      return false;
    }
    const { envelope, encrypted } = parsed;

    // Resolve this blob's application secret: from the chain map if a
    // descendant already supplied it (keyed by commit id), otherwise via a
    // single CGKA lookup (the entry point). The bulk path passes an empty commit
    // id, so a cold entry point is reached only through the CGKA lookup. If a
    // cached key fails to decrypt, we fall back to a CGKA lookup.
    const cachedKey = commitId ? this.#blobKeys.get(commitId) : undefined;
    let decrypted: Uint8Array | undefined;
    if (cachedKey) {
      try {
        decrypted = encrypted.decryptWithKey(cachedKey);
      } catch {
        log.debug(
          `[KeyhiveBlobInterceptor] cached key failed for commit ${commitId}; retrying via CGKA`
        );
      }
    }
    let plaintext: Uint8Array;
    let selfKey: Uint8Array;
    if (decrypted !== undefined && cachedKey) {
      plaintext = decrypted;
      selfKey = cachedKey;
    } else {
      const viaCgka = await this.#decryptViaCgka(doc, encrypted);
      if (!viaCgka) return false;
      plaintext = viaCgka.plaintext;
      selfKey = viaCgka.applicationSecret;
    }
    if (commitId) this.#blobKeys.set(commitId, selfKey);
    out.push(plaintext);
    if (commitId) this.#delivered.add(commitId);

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
      // Already delivered on a prior pass: its subtree is in the doc, so don't
      // reload or recurse. The key is recorded above for any direct decrypt.
      if (this.#delivered.has(idHex)) continue;
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
    doc: KeyhiveDoc,
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
    const parsed = parseEnvelope(parentBlob);
    if (!parsed) return undefined;
    const { envelope, encrypted } = parsed;
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
    doc: KeyhiveDoc,
    encrypted: Encrypted
  ): Promise<{ plaintext: Uint8Array; applicationSecret: Uint8Array } | null> {
    try {
      const res = await this.#keyhive.tryDecryptKeyed(doc, encrypted);
      return {
        plaintext: res.plaintext,
        applicationSecret: res.applicationSecret,
      };
    } catch (firstError) {
      if (await this.#importNewLeafSecrets()) {
        try {
          const res = await this.#keyhive.tryDecryptKeyed(doc, encrypted);
          return {
            plaintext: res.plaintext,
            applicationSecret: res.applicationSecret,
          };
        } catch (retryError) {
          log.debug(
            "[KeyhiveBlobInterceptor] CGKA decrypt failed after importing new leaf secrets; leaving pending:",
            retryError
          );
          return null;
        }
      }
      log.debug(
        "[KeyhiveBlobInterceptor] CGKA decrypt failed (no new leaf secrets to import); leaving pending:",
        firstError
      );
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

/**
 * Frame the keyhive-encrypted blob and its encrypted predecessor-key table:
 * a version byte, the inner length as a little-endian uint32, then the two
 * payloads. Everything after the inner payload is the predecessor ciphertext.
 *
 * @internal Exported for tests.
 */
export function encodeOuterEnvelope(
  inner: Uint8Array,
  predsCipher: Uint8Array
): Uint8Array {
  const out = new Uint8Array(5 + inner.length + predsCipher.length);
  out[0] = ENVELOPE_VERSION;
  new DataView(out.buffer, out.byteOffset, out.byteLength).setUint32(
    1,
    inner.length,
    true
  );
  out.set(inner, 5);
  out.set(predsCipher, 5 + inner.length);
  return out;
}

/**
 * The two payloads an outer envelope carries: the keyhive-encrypted blob, and
 * the encrypted predecessor-key table that follows it.
 *
 * @internal Exported for tests.
 */
export interface OuterEnvelope {
  inner: Uint8Array;
  predsCipher: Uint8Array;
}

/**
 * Inverse of {@link encodeOuterEnvelope}.
 *
 * @internal Exported for tests.
 */
export function decodeOuterEnvelope(bytes: Uint8Array): OuterEnvelope | null {
  if (bytes.length < 5 || bytes[0] !== ENVELOPE_VERSION) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const innerLen = view.getUint32(1, true);
  if (5 + innerLen > bytes.length) return null;
  return {
    inner: bytes.subarray(5, 5 + innerLen),
    predsCipher: bytes.subarray(5 + innerLen),
  };
}

// Decode the outer envelope and parse its inner keyhive content envelope.
// Null for any blob this interceptor did not produce.
function parseEnvelope(
  blob: Uint8Array
): { envelope: OuterEnvelope; encrypted: Encrypted } | null {
  const envelope = decodeOuterEnvelope(blob);
  if (!envelope) return null;
  try {
    return { envelope, encrypted: Encrypted.fromBytes(envelope.inner) };
  } catch {
    return null;
  }
}

/**
 * Parse the decrypted predecessor-key table: a flat run of 64-byte entries,
 * each a 32-byte commit id followed by that predecessor's 32-byte application
 * secret. A trailing partial entry is ignored rather than treated as an error.
 *
 * @internal Exported for tests.
 */
export function decodePreds(
  plain: Uint8Array
): Array<{ idHex: string; key: Uint8Array }> {
  const out: Array<{ idHex: string; key: Uint8Array }> = [];
  for (
    let off = 0;
    off + PRED_ENTRY_BYTES <= plain.length;
    off += PRED_ENTRY_BYTES
  ) {
    out.push({
      idHex: bytesToHex(plain.subarray(off, off + COMMIT_ID_BYTES)),
      key: plain.slice(off + COMMIT_ID_BYTES, off + PRED_ENTRY_BYTES),
    });
  }
  return out;
}

// Parse the document id once, returning both the raw bytes (for building a
// KeyhiveDocumentId) and whether it is an unprotected id.
function parseDocId(documentId: DocumentId): {
  binaryDocumentId: Uint8Array;
  unprotected: boolean;
} {
  const { binaryDocumentId } = parseAutomergeUrl(
    `automerge:${documentId}` as AutomergeUrl
  );
  return {
    binaryDocumentId,
    unprotected: isUnprotectedDocId(binaryDocumentId),
  };
}
