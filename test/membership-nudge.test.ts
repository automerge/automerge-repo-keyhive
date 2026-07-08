import { describe, it, expect, beforeAll } from "vitest";
import {
  Access,
  CiphertextStore,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import { stringifyAutomergeUrl } from "@automerge/automerge-repo/slim";
import type { AutomergeUrl, PeerId, Repo } from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import {
  AutomergeRepoKeyhive,
  generateDoc,
  NUDGE_FIELD,
} from "../src/keyhive/automerge-repo-keyhive.js";
import { KeyhiveEventEmitter } from "../src/keyhive/emitter.js";

/**
 * A subduction hive backed by a real Keyhive, with the repo, the network
 * adapter, storage, and the blob interceptor stubbed out. The interceptor
 * reports no tracked documents, so the nudge check can only find a document
 * through the local-membership-change set that `addMemberToDoc` and
 * `setPublicAccess` populate.
 */
async function buildHive() {
  const keyPair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ]);
  const signer = await Signer.webCryptoSigner(keyPair);
  const keyhive = await Keyhive.init(
    signer,
    CiphertextStore.newInMemory(),
    () => {},
  );

  const nudges: Record<string, unknown>[] = [];
  const repo = {
    shareConfigChanged() {},
    async find() {
      return {
        change(fn: (d: Record<string, unknown>) => void) {
          const d: Record<string, unknown> = {};
          fn(d);
          nudges.push(d);
        },
      };
    },
  } as unknown as Repo;

  const hive = new AutomergeRepoKeyhive(
    {} as any,
    keyhive,
    { async saveLeafSecret() {} } as any,
    "test-peer" as PeerId,
    new KeyhiveEventEmitter(),
    { on() {}, disconnect() {} } as any,
    (async () => new Uint8Array()) as any,
    (() => {
      throw new Error("createKeyhiveNetworkAdapter is unused in this test");
    }) as any,
    { trackedDocIds: [] as string[] } as any,
  );
  hive.linkRepo(repo, { debounceMs: 0 });

  return { hive, keyhive, nudges };
}

async function createDoc(keyhive: Keyhive): Promise<AutomergeUrl> {
  const doc = await generateDoc(keyhive);
  return stringifyAutomergeUrl({
    documentId: doc.doc_id.toBytes() as any,
  }) as AutomergeUrl;
}

/** Run the debounced schedule that `linkRepo` installed and let it settle. */
async function flush(hive: AutomergeRepoKeyhive): Promise<void> {
  hive.notifySameAgentKeyhiveChange();
  await new Promise((r) => setTimeout(r, 50));
}

describe("checkForMembershipNudges", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("nudges a document the blob interceptor has never tracked", async () => {
    const { hive, keyhive, nudges } = await buildHive();
    const docUrl = await createDoc(keyhive);

    await hive.setPublicAccess(docUrl, Access.read());
    await flush(hive);

    expect(nudges.length).toBe(1);
    expect(typeof nudges[0][NUDGE_FIELD]).toBe("number");
  });

  it("nudges once per membership change, not on every flush", async () => {
    const { hive, keyhive, nudges } = await buildHive();
    const docUrl = await createDoc(keyhive);

    await hive.setPublicAccess(docUrl, Access.read());
    await flush(hive);
    await flush(hive);

    expect(nudges.length).toBe(1);
  });
});
