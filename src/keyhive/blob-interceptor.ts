import { log } from "../logging.js";
import {
  type AutomergeUrl,
  type BlobInterceptor,
  type DocumentId,
  type StorageAdapterInterface,
  parseAutomergeUrl,
} from "@automerge/automerge-repo/slim";
import {
  CausalDecryption,
  ChangeId,
  DocumentId as KeyhiveDocumentId,
  Encrypted,
  Keyhive,
} from "@keyhive/keyhive/slim";
import { bytesToHex, hexToBytes } from "@noble/hashes/utils.js";
import { PromiseQueue } from "../network-adapter/pending.js";
import { isUnprotectedDocId } from "../utilities.js";
import { KEYHIVE_DB_KEY, KEYHIVE_PREKEY_SECRETS_KEY } from "./keyhive.js";

type LoadBlob = (commitIdHex: string) => Promise<Uint8Array | null>;

// Each blob is one keyhive block whose content ref is the blob's commit id
// and whose predecessors are the commit's parents. The block contains its
// parents' keys, so a reader who can decrypt it can decrypt them, and theirs
// in turn, with `tryCausalDecrypt`.
export class KeyhiveBlobInterceptor implements BlobInterceptor {
  #keyhive: Keyhive;
  #queue: PromiseQueue;
  #onEncrypted?: () => void;
  #storage?: StorageAdapterInterface;

  // Byte-length of the prekey-secrets archive blob last imported, so a miss
  // only re-imports it when a sibling instance has rewritten it.
  #importedPrekeyArchiveLen = -1;

  // commitId hexes whose plaintext has already been returned to
  // `loadIncremental` (and so applied to the doc). Once a blob is delivered its
  // whole loadable ancestor subtree was delivered with it, so a later incoming
  // descendant can stop the walk here instead of re-loading and re-decrypting
  // the entire history on every message.
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
   * Drop the delivery bookkeeping.
   */
  clearCachedKeyMaterial(): void {
    this.#delivered.clear();
  }

  // Import the prekey-secrets archive (KEYHIVE_PREKEY_SECRETS_KEY) if another
  // instance of this identity has rewritten it since this instance last read
  // it. It is rewritten when a prekey is created or rotated, so a live miss has
  // to re-read it (not just init/refresh) to pick up a prekey a sibling just
  // created. Returns true if it imported anything. Called only on an
  // encrypt/decrypt miss, so the common path pays nothing.
  async #importNewPrekeySecrets(): Promise<boolean> {
    if (!this.#storage) return false;
    try {
      // Length is a cheap change signal: the archive only ever accumulates
      // secrets, so a different length means a sibling added one.
      const archive = await this.#storage.load([
        KEYHIVE_DB_KEY,
        KEYHIVE_PREKEY_SECRETS_KEY,
      ]);
      if (!archive || archive.byteLength === this.#importedPrekeyArchiveLen) {
        return false;
      }
      await this.#keyhive.importPrekeySecrets(archive);
      this.#importedPrekeyArchiveLen = archive.byteLength;
      return true;
    } catch (e) {
      log.error("[KeyhiveBlobInterceptor] importNewPrekeySecrets failed:", e);
      return false;
    }
  }

  async transformOutgoing(
    documentId: DocumentId,
    commitId: string,
    parents: string[],
    blob: Uint8Array,
    loadBlob?: LoadBlob
  ): Promise<Uint8Array | null> {
    const { binaryDocumentId, unprotected } = parseDocId(documentId);
    if (unprotected) return blob;
    return this.#queue.run(async () => {
      const docId = new KeyhiveDocumentId(binaryDocumentId);
      // No keyhive doc yet: drop the outgoing blob (nothing stored or pushed).
      if (!(await this.#keyhive.hasDocument(docId))) {
        log.debug(
          `[KeyhiveBlobInterceptor] transformOutgoing: no keyhive document for ${documentId}; dropping outgoing blob (a later save retries)`
        );
        return null;
      }
      let pcsHash = await this.#keyhive.tryPcsKeyHash(docId);
      if (!pcsHash) {
        // A sibling instance (e.g. the tab) may have created a prekey this
        // instance has not seen. Import and retry once.
        await this.#importNewPrekeySecrets();
        pcsHash = await this.#keyhive.tryPcsKeyHash(docId);
        if (!pcsHash) {
          // Encrypt anyway. `tryEncrypt` rotates when there is no derivable
          // key.
          log.debug(
            `[KeyhiveBlobInterceptor] transformOutgoing: no derivable PCS key for ${documentId} (even after importing prekey secrets); encrypting anyway so keyhive rotates`
          );
        }
      }
      let encrypted: Encrypted;
      try {
        encrypted = await this.#encrypt(
          docId,
          commitId,
          parents,
          blob,
          loadBlob
        );
      } catch (e) {
        if (pcsHash) throw e;
        // We had no derivable key and the rotation did not work either, so
        // this agent cannot encrypt for this document at all (perhaps it
        // is not yet in the document's CGKA tree).
        log.debug(
          `[KeyhiveBlobInterceptor] transformOutgoing: could not rotate to get a PCS key for ${documentId}; dropping outgoing blob (a later save retries): %O`,
          e
        );
        return null;
      }
      this.#onEncrypted?.();
      return encrypted.toBytes();
    });
  }

  // Encrypt `blob` after `parents`. Keyhive refuses a parent whose key it does
  // not have, which happens when a sibling instance wrote it or this keyhive
  // has not read it yet. Then each parent's stored blob is read, so keyhive
  // learns its key, and a parent that still cannot be read is left out. In
  // that case, a reader of this blob can reach it only through CGKA.
  async #encrypt(
    docId: KeyhiveDocumentId,
    commitId: string,
    parents: string[],
    blob: Uint8Array,
    loadBlob: LoadBlob | undefined
  ): Promise<Encrypted> {
    const write = async (preds: string[]) =>
      (
        await this.#keyhive.tryEncrypt(
          docId,
          changeId(commitId),
          preds.map(changeId),
          blob
        )
      ).encryptedContent();
    try {
      return await write(parents);
    } catch (e) {
      if (parents.length === 0 || !loadBlob) throw e;
    }
    const readable: string[] = [];
    for (const parentHex of parents) {
      if (await this.#readParent(docId, parentHex, loadBlob)) {
        readable.push(parentHex);
      } else {
        log.debug(
          `[KeyhiveBlobInterceptor] transformOutgoing: cannot read parent ${parentHex} of ${commitId}; leaving it out of the blob's ancestors`
        );
      }
    }
    return await write(readable);
  }

  // Read a parent's stored blob through CGKA, so keyhive learns its key.
  async #readParent(
    docId: KeyhiveDocumentId,
    parentHex: string,
    loadBlob: LoadBlob
  ): Promise<boolean> {
    let parentBlob: Uint8Array | null;
    try {
      parentBlob = await loadBlob(parentHex);
    } catch {
      return false;
    }
    const encrypted = parentBlob && parseBlob(parentBlob);
    if (!encrypted || bytesToHex(encrypted.contentRef()) !== parentHex) {
      return false;
    }
    return (
      (await this.#viaCgka(() =>
        this.#keyhive.tryDecrypt(docId, encrypted)
      )) !== null
    );
  }

  async transformIncoming(
    documentId: DocumentId,
    commitId: string,
    blob: Uint8Array,
    loadBlob?: LoadBlob
  ): Promise<Uint8Array | null> {
    const { binaryDocumentId, unprotected } = parseDocId(documentId);
    if (unprotected) return blob;
    return this.#queue.run(async () => {
      const docId = new KeyhiveDocumentId(binaryDocumentId);
      if (!(await this.#keyhive.hasDocument(docId))) return null;

      const encrypted = parseBlob(blob);
      if (!encrypted) {
        log.debug(
          `[KeyhiveBlobInterceptor] unrecognized blob${
            commitId ? ` for commit ${commitId}` : " during bulk document load"
          }; leaving pending`
        );
        return null;
      }
      // The bulk path passes an empty commit id, so there is nothing to check.
      const ownHex = bytesToHex(encrypted.contentRef());
      if (commitId && commitId !== ownHex) {
        log.debug(
          `[KeyhiveBlobInterceptor] blob for commit ${commitId} is the block for ${ownHex}; leaving pending`
        );
        return null;
      }

      // Decrypt this blob and, on a cold load, the ancestors it lists that
      // `loadBlob` has and that have not been delivered. Their concatenation
      // is a valid `loadIncremental` input.
      const load = async (ref: ChangeId) => {
        const hex = bytesToHex(ref.toBytes());
        if (this.#delivered.has(hex) || !loadBlob) return undefined;
        return await loadBlob(hex);
      };
      const decryption = await this.#viaCgka(() =>
        this.#keyhive.tryCausalDecrypt(docId, encrypted, load)
      );
      if (!decryption) return null;
      return this.#deliver(decryption, ownHex);
    });
  }

  // Record what `decryption` read and return the plaintexts: this blob's,
  // which is read even if it was delivered before, and its ancestors' not
  // yet delivered.
  #deliver(decryption: CausalDecryption, ownHex: string): Uint8Array {
    for (const failure of decryption.failed()) {
      log.debug(
        `[KeyhiveBlobInterceptor] could not read ancestor ${bytesToHex(
          failure.contentRef.toBytes()
        )}: ${failure.reason}`
      );
    }
    const out: Uint8Array[] = [];
    for (const block of decryption.complete()) {
      const hex = bytesToHex(block.contentRef.toBytes());
      if (this.#delivered.has(hex) && hex !== ownHex) continue;
      this.#delivered.add(hex);
      out.push(block.content());
    }
    return out.length === 1 ? out[0] : concatBytes(out);
  }

  // Run a keyhive call that needs this agent's CGKA key, importing
  // sibling-written prekey secrets and retrying once on a miss. Returns null
  // if it still throws (the caller leaves the blob pending).
  async #viaCgka<T>(call: () => Promise<T>): Promise<T | null> {
    try {
      return await call();
    } catch (firstError) {
      if (await this.#importNewPrekeySecrets()) {
        try {
          return await call();
        } catch (retryError) {
          log.debug(
            "[KeyhiveBlobInterceptor] CGKA decrypt failed after importing new prekey secrets; leaving pending:",
            retryError
          );
          return null;
        }
      }
      log.debug(
        "[KeyhiveBlobInterceptor] CGKA decrypt failed (no new prekey secrets to import); leaving pending:",
        firstError
      );
      return null;
    }
  }
}

function changeId(commitIdHex: string): ChangeId {
  return new ChangeId(hexToBytes(commitIdHex));
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

// Null for any blob that is not a keyhive block.
function parseBlob(blob: Uint8Array): Encrypted | null {
  try {
    return Encrypted.fromBytes(blob);
  } catch {
    return null;
  }
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
