import { describe, it, expect, beforeAll } from "vitest";
import {
  CiphertextStore,
  Identifier,
  Keyhive,
  MemberedId,
  Power,
  Signer,
  type DocumentId as KeyhiveDocumentId,
} from "@keyhive/keyhive/slim";
import { stringifyAutomergeUrl } from "@automerge/automerge-repo/slim";
import type { DocumentId } from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import { KeyhiveBlobInterceptor } from "../src/keyhive/blob-interceptor.js";
import { generateDoc } from "../src/keyhive/automerge-repo-keyhive.js";
import { PromiseQueue } from "../src/network-adapter/pending.js";

interface Peer {
  keyhive: Keyhive;
  id: Identifier;
  interceptor: KeyhiveBlobInterceptor;
}

async function makePeer(): Promise<Peer> {
  const keyhive = await Keyhive.init(
    Signer.generateMemory(),
    CiphertextStore.newInMemory(),
    () => {}
  );
  const id = (await keyhive.contactCard()).id;
  const interceptor = new KeyhiveBlobInterceptor(keyhive, new PromiseQueue());
  return { keyhive, id, interceptor };
}

/** Deliver every keyhive event `from` has for `to`, as a sync would. */
async function sync(from: Peer, to: Peer): Promise<void> {
  const agent = await from.keyhive.getPrincipal(to.id);
  const events = await from.keyhive.eventsForAgent(agent);
  await to.keyhive.ingestEventsBytes([...events.values()]);
}

function repoDocId(docId: KeyhiveDocumentId): DocumentId {
  return stringifyAutomergeUrl({
    documentId: docId.toBytes() as never,
  }).replace(/^automerge:/, "") as DocumentId;
}

/**
 * Alice writes to a new document, grants bob access with `grant`, and writes
 * again. Returns what bob decrypts of each write, each blob on its own, so
 * neither can reach the other through ARK's predecessor envelope.
 */
async function bobReadsAroundGrant(
  grant: (alice: Peer, bob: Peer, docId: KeyhiveDocumentId) => Promise<void>
): Promise<{ before: string | null; after: string | null }> {
  const alice = await makePeer();
  const bob = await makePeer();
  await alice.keyhive.receiveContactCard(await bob.keyhive.contactCard());
  const docId = await generateDoc(alice.keyhive);
  const doc = repoDocId(docId);

  const write = (commitId: string, text: string) =>
    alice.interceptor.transformOutgoing(
      doc,
      commitId,
      [],
      new TextEncoder().encode(text)
    );
  const read = async (commitId: string, blob: Uint8Array | null) => {
    if (!blob) throw new Error(`alice could not encrypt ${commitId}`);
    const plain = await bob.interceptor.transformIncoming(doc, commitId, blob);
    return plain ? new TextDecoder().decode(plain) : null;
  };

  const beforeId = "aa".repeat(32);
  const before = await write(beforeId, "before bob was added");
  await grant(alice, bob, docId);
  const afterId = "bb".repeat(32);
  const after = await write(afterId, "after bob was added");
  await sync(alice, bob);

  return {
    before: await read(beforeId, before),
    after: await read(afterId, after),
  };
}

describe("a member added to a document", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("reads content written before and after being added", async () => {
    const read = await bobReadsAroundGrant(async (alice, bob, docId) => {
      await alice.keyhive.addMember(
        bob.id,
        MemberedId.document(docId),
        Power.read()
      );
    });

    expect(read).toEqual({
      before: "before bob was added",
      after: "after bob was added",
    });
  });

  it("reads both when added to a group that already has access", async () => {
    const read = await bobReadsAroundGrant(async (alice, bob, docId) => {
      const group = await alice.keyhive.generateGroup([]);
      await alice.keyhive.addMember(
        group.toIdentifier(),
        MemberedId.document(docId),
        Power.read()
      );
      await alice.keyhive.addMember(
        bob.id,
        MemberedId.group(group),
        Power.read()
      );
    });

    expect(read).toEqual({
      before: "before bob was added",
      after: "after bob was added",
    });
  });
});
