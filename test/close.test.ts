import { describe, it, expect, beforeAll } from "vitest";
import { CiphertextStore, Keyhive, Signer } from "@keyhive/keyhive/slim";
import type { PeerId, Repo } from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import { AutomergeRepoKeyhive } from "../src/keyhive/automerge-repo-keyhive.js";
import { KeyhiveEventEmitter } from "../src/keyhive/emitter.js";

/**
 * A subduction hive whose repo records every `shareConfigChanged` call and
 * whose network adapter records whether it was disconnected.
 */
async function buildHive(debounceMs: number) {
  const keyPair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ]);
  const signer = await Signer.webCryptoSigner(keyPair);
  const keyhive = await Keyhive.init(
    signer,
    CiphertextStore.newInMemory(),
    () => {}
  );

  let shareConfigChangedCount = 0;
  const repo = {
    shareConfigChanged() {
      shareConfigChangedCount++;
    },
  } as unknown as Repo;

  const adapter = {
    disconnected: false,
    on() {},
    disconnect() {
      this.disconnected = true;
    },
  };
  const emitter = new KeyhiveEventEmitter();

  const hive = new AutomergeRepoKeyhive(
    {} as any,
    keyhive,
    { async saveLeafSecret() {} } as any,
    "test-peer" as PeerId,
    emitter,
    adapter as any,
    (async () => new Uint8Array()) as any,
    (() => {
      throw new Error("createKeyhiveNetworkAdapter is unused in this test");
    }) as any,
    {
      trackedDocIds: [] as string[],
      docIdsAwaitingPcsKey: [] as string[],
    } as any
  );
  hive.linkRepo(repo, { debounceMs });

  return {
    hive,
    adapter,
    emitter,
    shareConfigChangedCount: () => shareConfigChangedCount,
  };
}

describe("close", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("disconnects the network adapter and removes emitter listeners", async () => {
    const { hive, adapter, emitter } = await buildHive(0);
    emitter.on("update", () => {});

    hive.close();

    expect(adapter.disconnected).toBe(true);
    expect(emitter.listenerCount("update")).toBe(0);
  });

  it("runs registered cleanups, and only once", async () => {
    const { hive } = await buildHive(0);
    let cleaned = 0;
    hive.registerCleanup(() => cleaned++);

    hive.close();
    expect(cleaned).toBe(1);

    hive.close();
    expect(cleaned).toBe(1);
  });

  it("cancels a debounced shareConfigChanged that has not fired yet", async () => {
    const { hive, shareConfigChangedCount } = await buildHive(50);

    hive.notifySameAgentKeyhiveChange();
    hive.close();
    await new Promise((r) => setTimeout(r, 100));

    expect(shareConfigChangedCount()).toBe(0);
  });

  it("still notifies the repo when the hive is left open", async () => {
    const { hive, shareConfigChangedCount } = await buildHive(50);

    hive.notifySameAgentKeyhiveChange();
    await new Promise((r) => setTimeout(r, 100));

    expect(shareConfigChangedCount()).toBe(1);
  });
});
