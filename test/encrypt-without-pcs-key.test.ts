import { describe, it, expect, beforeAll } from "vitest";
import { KeyhiveBlobInterceptor } from "../src/keyhive/blob-interceptor.js";
import { PromiseQueue } from "../src/network-adapter/pending.js";
import { stringifyAutomergeUrl } from "@automerge/automerge-repo/slim";
import type { DocumentId } from "@automerge/automerge-repo/slim";
import { Signer, type Keyhive } from "@keyhive/keyhive/slim";
import { initKeyhiveWasm } from "../src/index.js";

beforeAll(async () => {
  await initKeyhiveWasm();
});

/**
 * A keyhive-protected document id.
 */
function docId(): DocumentId {
  // A keyhive DocumentId is an Ed25519 verifying key, so arbitrary bytes do
  // not parse. Use `Signer` to get a real one.
  const bytes = Signer.generateMemory().verifyingKey;
  return stringifyAutomergeUrl({
    documentId: bytes as never,
  }).replace(/^automerge:/, "") as DocumentId;
}

interface Calls {
  pcsKeyHash: number;
  encrypt: number;
}

/**
 * A keyhive whose tree has no derivable key. `tryEncryptKeyed` throws, which
 * lets the test see whether it was reached at all without having to build a
 * valid encryption result.
 */
function keyhiveWithNoPcsKey(calls: Calls, pcsHash?: Uint8Array): Keyhive {
  return {
    async getDocument() {
      return { id: "doc" };
    },
    async tryPcsKeyHash() {
      calls.pcsKeyHash++;
      return pcsHash;
    },
    async tryEncryptKeyed() {
      calls.encrypt++;
      throw new Error("rotation not possible for this agent");
    },
  } as unknown as Keyhive;
}

describe("transformOutgoing when no PCS key is derivable", () => {
  it("still attempts to encrypt, so keyhive can rotate", async () => {
    const calls: Calls = { pcsKeyHash: 0, encrypt: 0 };
    const interceptor = new KeyhiveBlobInterceptor(
      keyhiveWithNoPcsKey(calls),
      new PromiseQueue()
    );

    const result = await interceptor.transformOutgoing(
      docId(),
      "aa".repeat(32),
      [],
      new Uint8Array([1, 2, 3])
    );

    expect(calls.encrypt).toBe(1);
    expect(result).toBeNull();
  });

  it("records the document as awaiting a key only when rotation fails", async () => {
    const calls: Calls = { pcsKeyHash: 0, encrypt: 0 };
    const interceptor = new KeyhiveBlobInterceptor(
      keyhiveWithNoPcsKey(calls),
      new PromiseQueue()
    );
    const id = docId();

    expect(interceptor.docIdsAwaitingPcsKey).toEqual([]);
    await interceptor.transformOutgoing(
      id,
      "bb".repeat(32),
      [],
      new Uint8Array([1])
    );
    expect(interceptor.docIdsAwaitingPcsKey).toEqual([id]);
  });

  it("rethrows an encryption failure when a key was available", async () => {
    const calls: Calls = { pcsKeyHash: 0, encrypt: 0 };
    const interceptor = new KeyhiveBlobInterceptor(
      keyhiveWithNoPcsKey(calls, new Uint8Array(32)),
      new PromiseQueue()
    );

    await expect(
      interceptor.transformOutgoing(
        docId(),
        "cc".repeat(32),
        [],
        new Uint8Array([1])
      )
    ).rejects.toThrow("rotation not possible");
    expect(calls.encrypt).toBe(1);
  });
});
