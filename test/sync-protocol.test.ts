import { describe, it, expect, beforeAll, vi } from "vitest";
import { encode } from "cbor-x";
import { CiphertextStore, Keyhive, Signer } from "@keyhive/keyhive/slim";
import type { Message, PeerId } from "@automerge/automerge-repo/slim";
import { initKeyhiveWasm } from "../src/index.js";
import { SyncProtocol } from "../src/network-adapter/sync-protocol.js";
import { PromiseQueue } from "../src/network-adapter/pending.js";
import { Metrics } from "../src/network-adapter/metrics.js";

describe("SyncProtocol", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  it("does not store event bytes from a peer that fail to decode", async () => {
    const keyhive = await Keyhive.init(
      Signer.generateMemory(),
      CiphertextStore.newInMemory(),
      () => {}
    );
    const saveEventBytesWithHash = vi.fn();
    const protocol = new SyncProtocol(
      {
        keyhive,
        keyhiveStorage: {
          withSuppressedEventWrites: (fn: () => unknown) => fn(),
          saveEventBytesWithHash,
        } as never,
        keyhiveQueue: new PromiseQueue(),
        peers: new Map(),
        contactCard: {} as never,
        cache: { onKeyhiveChanged() {} } as never,
        getPeerId: () => "local" as PeerId,
        getMetrics: () => new Metrics(),
        send: () => {},
        emit: () => {},
      },
      {
        archiveThreshold: 100,
        retryPendingFromStorage: false,
        minSyncRequestInterval: 0,
        minSyncResponseInterval: 0,
      }
    );

    await protocol.dispatchByType(
      {
        type: "keyhive-sync-ops",
        senderId: "peer" as PeerId,
        targetId: "local" as PeerId,
        data: encode([new Uint8Array([1, 2, 3])]),
      } as Message,
      new Metrics()
    );

    expect(saveEventBytesWithHash).not.toHaveBeenCalled();
  });
});
