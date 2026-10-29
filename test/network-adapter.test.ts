import { describe, it, expect } from "vitest";
import {
  NetworkAdapter,
  type Message,
  type PeerId,
} from "@automerge/automerge-repo/slim";
import { KeyhiveNetworkAdapter } from "../src/network-adapter/network-adapter.js";
import { PromiseQueue } from "../src/network-adapter/pending.js";

class RawAdapter extends NetworkAdapter {
  sent: Message[] = [];
  connect() {}
  disconnect() {}
  send(message: Message) {
    this.sent.push(message);
  }
  isReady() {
    return true;
  }
  whenReady() {
    return Promise.resolve();
  }
}

describe("KeyhiveNetworkAdapter", () => {
  it("neither announces nor sends to a peer other than the hardcoded remote", () => {
    const raw = new RawAdapter();
    const adapter = new KeyhiveNetworkAdapter({
      networkAdapter: raw,
      contactCard: {} as never,
      keyhive: {} as never,
      keyhiveStorage: {} as never,
      keyhiveQueue: new PromiseQueue(),
      periodicallyRequestSync: false,
      hardcodedRemoteId: "server" as PeerId,
      syncRequestInterval: 60_000,
      enableCompaction: false,
    });
    adapter.connect("local" as PeerId);
    const announced: PeerId[] = [];
    adapter.on("peer-candidate", ({ peerId }) => announced.push(peerId));

    for (const peerId of ["server", "other"] as PeerId[]) {
      raw.emit("peer-candidate", { peerId, peerMetadata: {} });
      adapter.send({
        type: "sync",
        senderId: "local",
        targetId: peerId,
      } as Message);
    }
    adapter.disconnect();

    expect(announced).toEqual(["server"]);
    expect(raw.sent.map((m) => m.targetId)).toEqual(["server"]);
  });
});
