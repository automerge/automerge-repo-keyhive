import { describe, it, expect, beforeAll } from "vitest";
import { CiphertextStore, Keyhive, Signer } from "@keyhive/keyhive/slim";
import { stringifyAutomergeUrl } from "@automerge/automerge-repo/slim";
import type { DocumentId } from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import { KeyhiveBlobInterceptor } from "../src/keyhive/blob-interceptor.js";
import { generateDoc } from "../src/keyhive/automerge-repo-keyhive.js";
import { PromiseQueue } from "../src/network-adapter/pending.js";

const text = (s: string) => new TextEncoder().encode(s);
const parentId = "aa".repeat(32);
const childId = "bb".repeat(32);

/**
 * A parent and a child written to one document, and a second interceptor on
 * the same keyhive that has delivered nothing, reading from a store holding
 * both.
 */
async function storedParentAndChild() {
  const keyhive = await Keyhive.init(
    Signer.generateMemory(),
    CiphertextStore.newInMemory(),
    () => {}
  );
  const docId = await generateDoc(keyhive);
  const doc = stringifyAutomergeUrl({
    documentId: docId.toBytes() as never,
  }).replace(/^automerge:/, "") as DocumentId;

  const writer = new KeyhiveBlobInterceptor(keyhive, new PromiseQueue());
  const stored = new Map<string, Uint8Array>();
  for (const [id, parents] of [
    [parentId, []],
    [childId, [parentId]],
  ] as const) {
    const blob = await writer.transformOutgoing(
      doc,
      id,
      [...parents],
      text(id)
    );
    if (!blob) throw new Error(`could not encrypt ${id}`);
    stored.set(id, blob);
  }
  const loadBlob = async (id: string) => stored.get(id) ?? null;
  const reader = new KeyhiveBlobInterceptor(keyhive, new PromiseQueue());
  const read = async (id: string, blob = stored.get(id)!) =>
    reader.transformIncoming(doc, id, blob, loadBlob);
  return { read, stored };
}

describe("KeyhiveBlobInterceptor", () => {
  beforeAll(async () => {
    await initKeyhiveWasm();
  });

  it("returns a blob's stored ancestors with it", async () => {
    const { read } = await storedParentAndChild();

    expect(await read(childId)).toEqual(text(childId + parentId));
  });

  it("does not return an ancestor it has already delivered", async () => {
    const { read } = await storedParentAndChild();

    await read(parentId);

    expect(await read(childId)).toEqual(text(childId));
  });

  it("does not read a blob that arrives under another commit id", async () => {
    const { read, stored } = await storedParentAndChild();

    expect(await read(parentId, stored.get(childId)!)).toBeNull();
  });
});
