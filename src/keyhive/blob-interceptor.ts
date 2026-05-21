import { type BlobInterceptor, type DocumentId, parseAutomergeUrl } from "@automerge/automerge-repo/slim";
import {
  ChangeId,
  DocumentId as KeyhiveDocumentId,
  Encrypted,
  Keyhive,
} from "@keyhive/keyhive/slim";
import { blake3 } from "@noble/hashes/blake3.js";
import { PromiseQueue } from "../network-adapter/pending.js";
import { docIdFromAutomergeUrl } from "./keyhive.js";

export class KeyhiveBlobInterceptor implements BlobInterceptor {
  #keyhive: Keyhive;
  #queue: PromiseQueue;

  constructor(keyhive: Keyhive, queue: PromiseQueue) {
    this.#keyhive = keyhive;
    this.#queue = queue;
  }

  async transformOutgoing(
    documentId: DocumentId,
    blob: Uint8Array
  ): Promise<Uint8Array> {
    if (isLegacyDocId(documentId)) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(
        toKeyhiveDocId(documentId)
      );
      if (!doc) {
        throw new Error(
          `[KeyhiveBlobInterceptor] encrypt failed: doc not found for ${documentId}`
        );
      }
      const contentRef = new ChangeId(blake3(blob));
      const result = await this.#keyhive.tryEncrypt(doc, contentRef, [], blob);
      const encrypted = result.encrypted_content();
      return encrypted.serialize();
    });
  }

  async transformIncoming(
    documentId: DocumentId,
    blob: Uint8Array
  ): Promise<Uint8Array | null> {
    if (isLegacyDocId(documentId)) return blob;
    return this.#queue.run(async () => {
      const doc = await this.#keyhive.getDocument(
        toKeyhiveDocId(documentId)
      );
      if (!doc) {
        console.warn(`[BlobInterceptor] decrypt: doc not found for ${documentId}`);
        return null;
      }
      let encrypted: Encrypted;
      try {
        encrypted = Encrypted.fromBytes(blob);
      } catch (e) {
        console.warn(`[BlobInterceptor] decrypt: fromBytes failed (${blob.length} bytes):`, e);
        return null;
      }
      try {
        return await this.#keyhive.tryDecrypt(doc, encrypted);
      } catch (e) {
        console.warn(`[BlobInterceptor] decrypt: tryDecrypt failed:`, e);
        return null;
      }
    });
  }
}

function toKeyhiveDocId(documentId: DocumentId): KeyhiveDocumentId {
  return docIdFromAutomergeUrl(`automerge:${documentId}` as any);
}

function isLegacyDocId(documentId: DocumentId): boolean {
  const { binaryDocumentId } = parseAutomergeUrl(`automerge:${documentId}` as any);
  if (binaryDocumentId.length < 32) return true;
  for (let i = 16; i < 32; i++) {
    if (binaryDocumentId[i] !== 0) return false;
  }
  return true;
}
