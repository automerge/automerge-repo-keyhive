import { describe, it, expect, beforeAll } from "vitest";
import { Signer } from "@keyhive/keyhive/slim";
import { PeerId } from "@automerge/automerge-repo/slim";
import {
  peerIdFromSigner,
  verifyingKeyPeerIdWithoutSuffix,
} from "../src/utilities.js";
import { peerIdFromVerifyingKey } from "../src/messages.js";

describe("Suffix support for peerIds", () => {
  let signer: Signer;
  let verifyingKey: Uint8Array;

  beforeAll(async () => {
    signer = Signer.generateMemory();
    verifyingKey = signer.verifyingKey;
  });

  describe("peerIdFromSigner", () => {
    it("should create peerId without suffix when none provided", () => {
      const peerId = peerIdFromSigner(signer);
      const expectedBase = btoa(String.fromCharCode(...verifyingKey));
      expect(peerId).toBe(`${expectedBase}`);
    });

    it("should create peerId with suffix", () => {
      const suffix = "a_test-suffix.123";
      const peerId = peerIdFromSigner(signer, suffix);
      const expectedBase = btoa(String.fromCharCode(...verifyingKey));
      expect(peerId).toBe(`${expectedBase}-${suffix}`);
    });
  });

  describe("peerIdFromVerifyingKey", () => {
    it("should create peerId without suffix when none provided", () => {
      const peerId = peerIdFromVerifyingKey(verifyingKey);
      const expectedBase = btoa(String.fromCharCode(...verifyingKey));
      expect(peerId).toBe(`${expectedBase}`);
    });

    it("should create peerId with suffix", () => {
      const suffix = "a_test-suffix.123";
      const peerId = peerIdFromVerifyingKey(verifyingKey, suffix);
      const expectedBase = btoa(String.fromCharCode(...verifyingKey));
      expect(peerId).toBe(`${expectedBase}-${suffix}`);
    });
  });

  describe("verifyingKeyPeerIdWithoutSuffix", () => {
    it("should extract base peerId without suffix", () => {
      const peerId =
        "8ytQQ094vxrS7a3N7Z1tA+ZRQP5yrruvK5A5fC49FMQ=-test-suffix_123" as PeerId;
      const baseId = verifyingKeyPeerIdWithoutSuffix(peerId);
      expect(baseId).toBe("8ytQQ094vxrS7a3N7Z1tA+ZRQP5yrruvK5A5fC49FMQ=");
    });

    it("should handle peerId with empty suffix", () => {
      const peerId = "8ytQQ094vxrS7a3N7Z1tA+ZRQP5yrruvK5A5fC49FMQ=" as PeerId;
      const baseId = verifyingKeyPeerIdWithoutSuffix(peerId);
      expect(baseId).toBe("8ytQQ094vxrS7a3N7Z1tA+ZRQP5yrruvK5A5fC49FMQ=");
    });
  });
});
