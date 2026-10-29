import { describe, it, expect, beforeAll } from "vitest";
import {
  CiphertextStore,
  Keyhive,
  MemberedId,
  Power,
  Signer,
} from "@keyhive/keyhive/slim";
import { initKeyhiveWasm } from "../src/index.js";
import { generateDoc } from "../src/keyhive/automerge-repo-keyhive.js";
import { peerIdFromSigner } from "../src/utilities.js";
import { PeriodicEventCache } from "../src/network-adapter/periodic-event-cache.js";
import { EventBytesOnlyEventCache } from "../src/network-adapter/event-bytes-only-event-cache.js";

describe("PeriodicEventCache", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("provides a revoked member the same hashes as the per-agent lookup", async () => {
    const newKeyhive = (signer: Signer) =>
      Keyhive.init(signer, CiphertextStore.newInMemory(), () => {});
    const alice = await newKeyhive(Signer.generateMemory());
    const bobSigner = Signer.generateMemory();
    const bob = await (await newKeyhive(bobSigner)).contactCard();
    const bobPeerId = peerIdFromSigner(bobSigner);
    await alice.receiveContactCard(bob);
    const docId = MemberedId.document(await generateDoc(alice));
    await alice.addMember(bob.id, docId, Power.read());
    await alice.revokeMember(bob.id, true, docId);

    const periodic = new PeriodicEventCache();
    await periodic.refresh(alice);
    const cached = await periodic.getHashesForPeer(alice, bobPeerId);
    const direct = await new EventBytesOnlyEventCache().getHashesForPeer(
      alice,
      bobPeerId
    );
    expect(direct?.size).toBeGreaterThan(0);
    expect(new Set(cached?.keys())).toEqual(new Set(direct?.keys()));
  });
});
