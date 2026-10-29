import { describe, it, expect, beforeAll } from "vitest";
import {
  Archive,
  CiphertextStore,
  GroupId,
  Keyhive,
  Signer,
} from "@keyhive/keyhive/slim";
import type {
  Chunk,
  StorageAdapterInterface,
  StorageKey,
} from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import {
  KEYHIVE_ARCHIVES_KEY,
  KEYHIVE_DB_KEY,
  KEYHIVE_EVENTS_KEY,
  KeyhiveStorage,
} from "../src/keyhive/keyhive.js";

class MemoryStorage implements StorageAdapterInterface {
  data = new Map<string, { key: StorageKey; bytes: Uint8Array }>();
  async load(key: StorageKey) {
    return this.data.get(key.join("\0"))?.bytes;
  }
  async save(key: StorageKey, bytes: Uint8Array) {
    this.data.set(key.join("\0"), { key, bytes });
  }
  async remove(key: StorageKey) {
    this.data.delete(key.join("\0"));
  }
  async loadRange(prefix: StorageKey): Promise<Chunk[]> {
    return [...this.data.values()]
      .filter(({ key }) => prefix.every((p, i) => key[i] === p))
      .map(({ key, bytes }) => ({ key, data: bytes }));
  }
  async removeRange(prefix: StorageKey) {
    for (const { key } of await this.loadRange(prefix)) await this.remove(key);
  }
}

function newKeyhive(seed: Uint8Array) {
  return Keyhive.init(
    Signer.memorySignerFromBytes(seed),
    CiphertextStore.newInMemory(),
    () => {}
  );
}

async function storedStateHas(
  seed: Uint8Array,
  storage: MemoryStorage,
  group: GroupId
) {
  const reader = await newKeyhive(seed);
  const archives = await storage.loadRange([
    KEYHIVE_DB_KEY,
    KEYHIVE_ARCHIVES_KEY,
  ]);
  for (const chunk of archives) {
    await reader.ingestArchive(new Archive(chunk.data!));
  }
  const events = await storage.loadRange([KEYHIVE_DB_KEY, KEYHIVE_EVENTS_KEY]);
  if (events.length > 0) {
    await reader.ingestEventsBytes(events.map((c) => c.data!));
  }
  return (await reader.getGroup(group)) !== undefined;
}

describe("KeyhiveStorage.compact", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("leaves both instances' state in storage when two instances compact at once", async () => {
    const seed = crypto.getRandomValues(new Uint8Array(32));
    const storage = new MemoryStorage();
    const khA = await newKeyhive(seed);
    const khB = await newKeyhive(seed);
    const storeA = new KeyhiveStorage(new Uint8Array(32).fill(1), storage);
    const storeB = new KeyhiveStorage(new Uint8Array(32).fill(2), storage);
    const groupA = await khA.generateGroup([]);
    const groupB = await khB.generateGroup([]);
    await storeA.saveKeyhiveWithHash(khA);
    await storeB.saveKeyhiveWithHash(khB);

    await Promise.all([storeA.compact(khA), storeB.compact(khB)]);

    expect(await storedStateHas(seed, storage, groupA)).toBe(true);
    expect(await storedStateHas(seed, storage, groupB)).toBe(true);
  });
});
