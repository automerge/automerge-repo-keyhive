import { describe, it, expect, beforeAll, vi } from "vitest";
import {
  CiphertextStore,
  Keyhive,
  MemberedId,
  Power,
  Signer,
  type SignedRevocation,
} from "@keyhive/keyhive/slim";
import {
  decodeHeads,
  initSubduction,
  Repo,
  type PeerId,
} from "@automerge/automerge-repo";
import { initKeyhiveWasm } from "../src/index.js";
import {
  keyhiveIdFactory,
  LegacyAutomergeRepoKeyhive,
} from "../src/keyhive/automerge-repo-keyhive.js";
import { KeyhiveEventEmitter } from "../src/keyhive/emitter.js";
import { docIdFromAutomergeUrl } from "../src/keyhive/keyhive.js";
import { uint8ArrayToHex } from "../src/utilities.js";

const newKeyhive = () =>
  Keyhive.init(
    Signer.generateMemory(),
    CiphertextStore.newInMemory(),
    () => {}
  );

describe("revokeMemberFromDoc", () => {
  beforeAll(async () => {
    initKeyhiveWasm();
    await initSubduction();
  });

  it("records the document's current heads as the revocation's retained heads", async () => {
    const keyhive = await newKeyhive();
    const hive = new LegacyAutomergeRepoKeyhive(
      {} as never,
      keyhive,
      {} as never,
      "local" as PeerId,
      null,
      { on() {}, disconnect() {} } as never,
      new KeyhiveEventEmitter(),
      keyhiveIdFactory(keyhive),
      {} as never
    );
    const repo = new Repo({ idFactory: hive.idFactory });
    hive.linkRepo(repo);
    const handle = await repo.create2({ n: 0 });
    handle.change((d) => {
      d.n = 1;
    });
    const docId = docIdFromAutomergeUrl(handle.url);
    const bob = await (await newKeyhive()).contactCard();
    await keyhive.receiveContactCard(bob);
    await keyhive.addMember(bob.id, MemberedId.document(docId), Power.read());
    const revokeMember = vi.spyOn(keyhive, "revokeMember");

    await hive.revokeMemberFromDoc(handle.url, bob.id);

    const [signed]: SignedRevocation[] =
      await revokeMember.mock.results[0].value;
    const retained = signed.revocation?.retained(docId);
    expect(retained?.map((id) => uint8ArrayToHex(id.toBytes()))).toEqual(
      decodeHeads(handle.heads())
    );
    hive.close();
    await repo.shutdown();
  });
});
