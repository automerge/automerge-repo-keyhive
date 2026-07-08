import { describe, it, expect, beforeAll } from "vitest";
import { Signer } from "@keyhive/keyhive/slim";
import type { PeerId } from "@automerge/automerge-repo/slim";
import { encode as cborEncode, decode as cborDecode } from "cbor-x";
import { initKeyhiveWasm } from "../src/index.js";
import { peerIdFromVerifyingKey } from "../src/network-adapter/messages.js";
import {
  decodeSubductionKeyhiveMessage,
  decodeSukFrame,
  encodeSubductionKeyhiveMessage,
  encodeSignedMessage,
  encodeSukFrame,
  peerIdFromSubduction,
  peerIdToSubduction,
  SUK_SCHEMA,
  type SubductionEncodeInput,
} from "../src/network-adapter/subduction-transport/index.js";

// Rust source of truth for the wire shape:
//   subduction_keyhive/src/wire.rs              SUK\0 + 4B BE length frame
//   subduction_keyhive/src/signed_message.rs    {contactCard: String, signed: bytes}
//   subduction_keyhive/src/message.rs           externally-tagged enum, snake_case fields
//   subduction_keyhive/src/peer_id.rs           {verifying_key: [u8;32], suffix: Option<String>}
//
// The tests below check our codec produces bytes whose structural
// invariants match those Rust types, not just that we're internally
// self-consistent.

describe("subduction-transport codec", () => {
  beforeAll(() => {
    initKeyhiveWasm();
  });

  describe("SUK frame", () => {
    it("encodes the SUK\\0 magic + big-endian length matching the buffer", () => {
      const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0x42]);
      const framed = encodeSukFrame(payload);

      expect(framed.length).toBe(8 + payload.length);
      // SUK\0 magic
      expect(Array.from(framed.subarray(0, 4))).toEqual(Array.from(SUK_SCHEMA));
      // Big-endian total size
      const declared =
        (framed[4] << 24) | (framed[5] << 16) | (framed[6] << 8) | framed[7];
      expect(declared).toBe(framed.length);
      // Payload follows
      expect(Array.from(framed.subarray(8))).toEqual(Array.from(payload));
    });

    it("decode rejects a tampered total-size field", () => {
      const framed = encodeSukFrame(new Uint8Array([1, 2, 3]));
      // Truncate one byte → declared length no longer matches buffer length.
      const truncated = framed.slice(0, framed.length - 1);
      expect(() => decodeSukFrame(truncated)).toThrow(/size mismatch/);
    });

    it("decode rejects a foreign schema (e.g. SUM frame)", () => {
      const framed = encodeSukFrame(new Uint8Array([1]));
      framed[2] = 0x4d; // 'K' -> 'M'
      expect(() => decodeSukFrame(framed)).toThrow(/bad schema header/);
    });
  });

  describe("SignedMessage CBOR map shape", () => {
    it("uses camelCase wire keys 'contactCard' and 'signed' (not snake_case)", () => {
      const cbor = encodeSignedMessage({
        contactCard: '{"some":"json"}',
        signed: new Uint8Array([0x01, 0x02, 0x03]),
      });
      const raw = cborDecode(cbor) as Record<string, unknown>;

      const keys = Object.keys(raw).sort();
      expect(keys).toEqual(["contactCard", "signed"]);
      // contactCard must be a JSON text string, not bytes or null.
      expect(typeof raw.contactCard).toBe("string");
      expect(raw.contactCard).toBe('{"some":"json"}');
      // signed must be a CBOR byte string (decoded as Uint8Array/Buffer).
      expect(raw.signed instanceof Uint8Array).toBe(true);
      expect(Array.from(raw.signed as Uint8Array)).toEqual([0x01, 0x02, 0x03]);
    });

    it("absent contact card is the empty-string sentinel, not null/missing", () => {
      const cbor = encodeSignedMessage({
        contactCard: "",
        signed: new Uint8Array([0xab]),
      });
      const raw = cborDecode(cbor) as Record<string, unknown>;
      // The "contactCard" key must be present and be the empty string.
      // Rust's `from_cbor` deserialises into `String` (not `Option<String>`)
      // and treats `""` as "no contact card".
      expect(Object.keys(raw)).toContain("contactCard");
      expect(raw.contactCard).toBe("");
    });
  });

  describe("PeerId conversion", () => {
    it("base64-decodes the verifying key bytes and preserves them through Rust shape", () => {
      const signer = Signer.generateMemory();
      const peerId = peerIdFromVerifyingKey(signer.verifyingKey);
      const rust = peerIdToSubduction(peerId);
      // The Rust shape carries the raw 32-byte Ed25519 verifying key,
      // not the base64 form. Bytes must match the signer's key exactly.
      expect(rust.verifying_key.length).toBe(32);
      expect(Array.from(rust.verifying_key)).toEqual(
        Array.from(signer.verifyingKey),
      );
      expect(rust.suffix).toBe(null);
      // Round-trip back to TS PeerId.
      expect(peerIdFromSubduction(rust)).toBe(peerId);
    });

    it("drops the dash-suffix from the wire form (suffix is local-only)", () => {
      const signer = Signer.generateMemory();
      const peerId = peerIdFromVerifyingKey(signer.verifyingKey, "my-suffix");
      const rust = peerIdToSubduction(peerId);
      // The verifying-key bytes must be the raw key, not include the suffix.
      expect(rust.verifying_key.length).toBe(32);
      expect(Array.from(rust.verifying_key)).toEqual(
        Array.from(signer.verifyingKey),
      );
      // The Rust server keys peers by verifying_key alone; we must not put
      // the TS-only suffix on the wire or its peer registry rejects us.
      expect(rust.suffix).toBe(null);
      // Round-trip drops the suffix. peerIdFromSubduction returns the no-suffix
      // form regardless of what was originally encoded.
      expect(peerIdFromSubduction(rust)).toBe(peerIdFromVerifyingKey(signer.verifyingKey));
    });

    it("rejects an obviously-malformed base64 portion", () => {
      // Two base64 chars decode to ~1 byte, far short of the 32 required.
      expect(() => peerIdToSubduction("AB" as PeerId)).toThrow(
        /verifying-key length/,
      );
    });
  });

  describe("Rust KeyhiveMessage envelope", () => {
    let signerA: Signer;
    let signerB: Signer;
    let senderId: PeerId;
    let targetId: PeerId;

    beforeAll(() => {
      signerA = Signer.generateMemory();
      signerB = Signer.generateMemory();
      senderId = peerIdFromVerifyingKey(signerA.verifyingKey);
      targetId = peerIdFromVerifyingKey(signerB.verifyingKey);
    });

    it("emits the externally-tagged variant name as the sole top-level CBOR map key", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-request",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({ found: [], pending: [] }),
      });
      const raw = cborDecode(cbor) as Record<string, unknown>;
      // Rust serde-CBOR enums are externally tagged: { "SyncRequest": {...} }.
      // Exactly one key, equal to the Rust variant name (PascalCase).
      expect(Object.keys(raw)).toEqual(["SyncRequest"]);
    });

    it("places sender_id/target_id INSIDE the variant (matching the Rust enum), with raw key bytes", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-request",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({ found: [], pending: [] }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const inner = raw.SyncRequest;
      // Snake_case field names (matches `serde::Serialize` default for
      // `KeyhivePeerId { verifying_key, suffix }` and the variant fields).
      expect(Object.keys(inner).sort()).toEqual(
        ["found", "pending", "sender_id", "target_id"].sort(),
      );
      const sender = inner.sender_id as { verifying_key: Uint8Array; suffix: unknown };
      // The verifying_key must be the raw 32 bytes, not the base64 form
      // we hold on the TS side.
      expect(sender.verifying_key.length).toBe(32);
      expect(Array.from(sender.verifying_key)).toEqual(
        Array.from(signerA.verifyingKey),
      );
      expect(sender.suffix).toBeNull();
    });

    it("renames camelCase inline-data totals into snake_case on the Rust wire", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-response",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({
          requested: [],
          found: [],
          syncResponderTotal: 17,
          syncRequesterTotal: 9,
        }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const inner = raw.SyncResponse;
      // Rust expects sync_responder_total / sync_requester_total
      // (snake_case). Catching this rename failure is the whole point.
      // The Rust side will reject the message if the keys are wrong.
      expect(inner.sync_responder_total).toBe(17);
      expect(inner.sync_requester_total).toBe(9);
      expect(inner.syncResponderTotal).toBeUndefined();
      expect(inner.syncRequesterTotal).toBeUndefined();
    });

    it("renames sync-check fields (senderTotal/senderSyncpoint → sender_total/sender_syncpoint)", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-check",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({ senderTotal: 42, senderSyncpoint: 41 }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const inner = raw.SyncCheck;
      expect(inner.sender_total).toBe(42);
      expect(inner.sender_syncpoint).toBe(41);
      expect(inner.senderTotal).toBeUndefined();
      expect(inner.senderSyncpoint).toBeUndefined();
    });

    it("emits sender_digest as the raw 32-byte op-set XOR (no 8-byte fold)", () => {
      // The Rust side deserializes sender_digest into a fixed [u8; 32], so the
      // wire value must be exactly the 32-byte XOR, not the old folded 8 bytes.
      const digest = Uint8Array.from({ length: 32 }, (_, i) => i + 1);
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-check",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({
          senderTotal: 42,
          senderSyncpoint: 41,
          senderDigest: digest,
        }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const wireDigest = raw.SyncCheck.sender_digest;
      expect(wireDigest).toBeInstanceOf(Uint8Array);
      expect((wireDigest as Uint8Array).length).toBe(32);
      expect(Array.from(wireDigest as Uint8Array)).toEqual(Array.from(digest));
    });

    it("defaults an absent sender_digest to 32 zero bytes", () => {
      // A SyncCheck built without a digest must still carry a 32-byte
      // all-zero field, so an old Rust peer never receives a short byte
      // string it would reject, and an all-zero digest never matches a
      // non-empty op-set.
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-check",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({ senderTotal: 42, senderSyncpoint: 41 }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const wireDigest = raw.SyncCheck.sender_digest as Uint8Array;
      expect(wireDigest).toBeInstanceOf(Uint8Array);
      expect(wireDigest.length).toBe(32);
      expect(wireDigest.every((b) => b === 0)).toBe(true);
    });

    it("renames sync-confirmation field (confirmerTotal → confirmer_total)", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-confirmation",
        senderId,
        targetId,
        inlineDataCbor: cborEncode({ confirmerTotal: 5 }),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const inner = raw.SyncConfirmation;
      expect(inner.confirmer_total).toBe(5);
      expect(inner.confirmerTotal).toBeUndefined();
    });

    it("contact-card variants carry no extra fields besides sender/target", () => {
      const cbor = encodeSubductionKeyhiveMessage({
        type: "keyhive-sync-request-contact-card",
        senderId,
        targetId,
        inlineDataCbor: new Uint8Array(),
      });
      const raw = cborDecode(cbor) as Record<string, Record<string, unknown>>;
      const inner = raw.RequestContactCard;
      // Rust defines RequestContactCard { sender_id, target_id }, exactly
      // two fields. Anything else means we leaked inline data on the wire.
      expect(Object.keys(inner).sort()).toEqual(["sender_id", "target_id"]);
    });

    it("decode of an unknown variant tag is rejected (not silently coerced)", () => {
      const bogus = cborEncode({ "NotARealVariant": { sender_id: { verifying_key: new Uint8Array(32), suffix: null }, target_id: { verifying_key: new Uint8Array(32), suffix: null } } });
      expect(() => decodeSubductionKeyhiveMessage(bogus)).toThrow(
        /unknown KeyhiveMessage variant/,
      );
    });

    it("decode produces TS-shape inline data with camelCase keys (so SyncProtocol handlers see what they expect)", () => {
      // Build a Rust-shape payload by hand, decode it, and verify the
      // inline CBOR our adapter hands to SyncProtocol uses camelCase.
      const rustWireBytes = cborEncode({
        SyncResponse: {
          sender_id: { verifying_key: signerA.verifyingKey, suffix: null },
          target_id: { verifying_key: signerB.verifyingKey, suffix: null },
          requested: [new Uint8Array([0xaa])],
          found: [new Uint8Array([0xbb])],
          sync_responder_total: 7,
          sync_requester_total: 3,
        },
      });
      const out = decodeSubductionKeyhiveMessage(rustWireBytes);
      expect(out.type).toBe("keyhive-sync-response");
      expect(out.senderId).toBe(senderId);
      expect(out.targetId).toBe(targetId);
      const inline = cborDecode(out.inlineDataCbor) as Record<string, unknown>;
      // Inline must use the camelCase shape the existing SyncProtocol
      // handlers (e.g. sendKeyhiveSyncOps) decode and read.
      expect(inline.syncResponderTotal).toBe(7);
      expect(inline.syncRequesterTotal).toBe(3);
      expect(inline.sync_responder_total).toBeUndefined();
    });
  });
});
