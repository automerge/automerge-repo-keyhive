// Wire codec for talking to the Rust subduction-keyhive server.
// TODO: Replace this with a subduction keyhive WASM API to avoid duplicate
// implementations.
//
// Rust source of truth, in https://github.com/inkandswitch/subduction:
//   subduction_keyhive/src/wire.rs            (SUK frame)
//   subduction_keyhive/src/signed_message.rs
//   subduction_keyhive/src/message.rs         (KeyhiveMessage enum)
//   subduction_keyhive/src/peer_id.rs         (KeyhivePeerId)
//
// Wire envelope:
//   +-------+-----------+---------------------------+
//   | "SUK\0"| TotalSize | CBOR(SignedMessage)        |
//   |  4 B  |  4 B BE   | variable                   |
//   +-------+-----------+---------------------------+
//
// SignedMessage (CBOR map, camelCase keys):
//   { contactCard: <text-string of JSON or "">, signed: <bincode bytes> }
//
// signed is bincode-1.3 encoded `Signed<Vec<u8>>` from keyhive-wasm
// (matches `Signed.toBytes()`). Its payload is a CBOR-encoded
// KeyhiveMessage (externally-tagged enum, snake_case field names).
//
// KeyhiveMessage CBOR shape (one of):
//   { "SyncRequest":        { sender_id, target_id, found, pending } }
//   { "SyncResponse":       { sender_id, target_id, requested, found,
//                             sync_responder_total, sync_requester_total } }
//   { "SyncOps":            { sender_id, target_id, ops,
//                             sync_responder_total, sync_requester_total } }
//   { "RequestContactCard": { sender_id, target_id } }
//   { "MissingContactCard": { sender_id, target_id } }
//   { "SyncCheck":          { sender_id, target_id, sender_total, sender_syncpoint,
//                             sender_digest (32-byte byte string) } }
//   { "SyncConfirmation":   { sender_id, target_id, confirmer_total } }
//
// KeyhivePeerId (CBOR map):
//   { verifying_key: <32-byte byte string>, suffix: null | <text string> }

import { decode, encode } from "cbor-x";
import type { PeerId } from "@automerge/automerge-repo/slim";

export const SUK_SCHEMA = new Uint8Array([0x53, 0x55, 0x4b, 0x00]); // "SUK\0"

export class SukFrameError extends Error {
  constructor(message: string) {
    super(`[suk-frame] ${message}`);
    this.name = "SukFrameError";
  }
}

/**
 * Detect a SUK frame by its 4-byte schema header.
 */
export function isSukFrame(bytes: Uint8Array): boolean {
  if (bytes.length < 4) return false;
  return (
    bytes[0] === SUK_SCHEMA[0] &&
    bytes[1] === SUK_SCHEMA[1] &&
    bytes[2] === SUK_SCHEMA[2] &&
    bytes[3] === SUK_SCHEMA[3]
  );
}

/**
 * Wrap a CBOR-encoded SignedMessage in the SUK envelope.
 */
export function encodeSukFrame(payload: Uint8Array): Uint8Array {
  const totalSize = 4 + 4 + payload.length;
  const buf = new Uint8Array(totalSize);
  buf.set(SUK_SCHEMA, 0);
  // 4-byte big-endian total size
  buf[4] = (totalSize >>> 24) & 0xff;
  buf[5] = (totalSize >>> 16) & 0xff;
  buf[6] = (totalSize >>> 8) & 0xff;
  buf[7] = totalSize & 0xff;
  buf.set(payload, 8);
  return buf;
}

/**
 * Strip the SUK envelope and return the inner CBOR-encoded SignedMessage bytes.
 */
export function decodeSukFrame(bytes: Uint8Array): Uint8Array {
  if (!isSukFrame(bytes)) {
    throw new SukFrameError(
      `bad schema header: expected SUK\\0 got ${[...bytes.slice(0, 4)]
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("")}`
    );
  }
  if (bytes.length < 8) {
    throw new SukFrameError(`message too short: ${bytes.length} bytes`);
  }
  const totalSize =
    ((bytes[4] << 24) | (bytes[5] << 16) | (bytes[6] << 8) | bytes[7]) >>> 0;
  if (totalSize !== bytes.length) {
    throw new SukFrameError(
      `size mismatch: declared ${totalSize}, actual ${bytes.length}`
    );
  }
  return bytes.slice(8);
}

/**
 * Wire shape of the outer SignedMessage map. `signed` is bincode bytes,
 * `contactCard` is a JSON string (empty string when absent).
 */
export interface SubductionSignedMessage {
  contactCard: string;
  signed: Uint8Array;
}

export function encodeSignedMessage(msg: SubductionSignedMessage): Uint8Array {
  return encode(msg);
}

export function decodeSignedMessage(
  bytes: Uint8Array
): SubductionSignedMessage {
  const obj = decode(bytes) as { contactCard?: unknown; signed?: unknown };
  if (typeof obj?.contactCard !== "string") {
    throw new SukFrameError(
      `decoded SignedMessage missing string contactCard field`
    );
  }
  // ciborium's default `Vec<u8>` serialization on the server side emits a
  // CBOR array (major type 4) rather than a byte string (major type 2), which
  // cbor-x decodes as `number[]`. Tolerate either shape until the server
  // pins `serde(with = "serde_bytes")` on `SignedMessage::signed`.
  let signedBytes: Uint8Array;
  if (obj?.signed instanceof Uint8Array) {
    signedBytes = obj.signed;
  } else if (Array.isArray(obj?.signed)) {
    const arr = obj.signed as number[];
    signedBytes = new Uint8Array(arr.length);
    for (let i = 0; i < arr.length; i++) signedBytes[i] = arr[i];
  } else {
    throw new SukFrameError(`decoded SignedMessage missing bytes signed field`);
  }
  return { contactCard: obj.contactCard, signed: signedBytes };
}

// ---------------------------------------------------------------------------
// PeerId (TS) ↔ KeyhivePeerId (Rust)
// ---------------------------------------------------------------------------

/**
 * Decoded form of `KeyhivePeerId` from the Rust crate.
 */
export interface SubductionPeerId {
  verifying_key: Uint8Array; // 32 bytes
  suffix: string | null;
}

/**
 * Convert a TS PeerId (`base64(verifyingKey)[-suffix]`) into the
 * Rust-shape `KeyhivePeerId`.
 *
 * The TS suffix is a local-only identifier (used for keyhive storage
 * uniqueness and multi-tab demos); the Rust server keys peers by the
 * 32-byte verifying_key alone and rejects messages that arrive with a
 * suffix that wasn't introduced via its own connection bridge. We drop
 * the suffix on the wire so the server's lookup succeeds.
 */
export function peerIdToSubduction(peerId: PeerId): SubductionPeerId {
  const dashIdx = peerId.indexOf("-");
  const b64 = dashIdx === -1 ? peerId : peerId.slice(0, dashIdx);

  const binStr = atob(b64);
  if (binStr.length !== 32) {
    throw new SukFrameError(
      `peerId verifying-key length: expected 32, got ${binStr.length}`
    );
  }
  const verifying_key = new Uint8Array(32);
  for (let i = 0; i < 32; i++) verifying_key[i] = binStr.charCodeAt(i);
  return { verifying_key, suffix: null };
}

/**
 * Convert a Rust-shape `KeyhivePeerId` back into a TS PeerId string.
 *
 * The wire form has no suffix (see {@link peerIdToSubduction}). We mirror that
 * here. Incoming messages always produce a no-suffix PeerId, matching
 * what the SyncProtocol's `verifyingKeyPeer` comparisons expect.
 */
export function peerIdFromSubduction(rust: SubductionPeerId): PeerId {
  if (rust.verifying_key.length !== 32) {
    throw new SukFrameError(
      `Rust peer-id verifying_key length: expected 32, got ${rust.verifying_key.length}`
    );
  }
  let bin = "";
  for (let i = 0; i < 32; i++)
    bin += String.fromCharCode(rust.verifying_key[i]);
  return btoa(bin) as PeerId;
}

// ---------------------------------------------------------------------------
// KeyhiveMessage enum (Rust shape)
// ---------------------------------------------------------------------------

export type KeyhiveMessageType =
  | "keyhive-sync-request"
  | "keyhive-sync-response"
  | "keyhive-sync-ops"
  | "keyhive-sync-request-contact-card"
  | "keyhive-sync-missing-contact-card"
  | "keyhive-sync-check"
  | "keyhive-sync-confirmation";

const RUST_VARIANT_BY_TYPE: Record<KeyhiveMessageType, string> = {
  "keyhive-sync-request": "SyncRequest",
  "keyhive-sync-response": "SyncResponse",
  "keyhive-sync-ops": "SyncOps",
  "keyhive-sync-request-contact-card": "RequestContactCard",
  "keyhive-sync-missing-contact-card": "MissingContactCard",
  "keyhive-sync-check": "SyncCheck",
  "keyhive-sync-confirmation": "SyncConfirmation",
};

const TYPE_BY_RUST_VARIANT: Record<string, KeyhiveMessageType> =
  Object.fromEntries(
    Object.entries(RUST_VARIANT_BY_TYPE).map(([k, v]) => [
      v,
      k as KeyhiveMessageType,
    ])
  );

// Sent for a SyncCheck whose op-set digest is absent (a peer predating the
// digest field). All-zero never matches a non-empty pair-set digest, so the
// receiver falls back to a full sync rather than wrongly short-circuiting.
const ZERO_DIGEST = new Uint8Array(32);

/**
 * The fields-only shape used in the existing TS adapter (camelCase) for
 * each keyhive message type. We only handle the fields that travel inside
 * `message.data`. Sender/target are pulled up to the Message level.
 */
export type TsInnerData =
  | { found: Uint8Array[]; pending: Uint8Array[] } // sync-request
  | {
      requested: Uint8Array[];
      found: Uint8Array[];
      syncResponderTotal?: number;
      syncRequesterTotal?: number;
    } // sync-response
  | {
      ops: Uint8Array[];
      syncResponderTotal?: number;
      syncRequesterTotal?: number;
    } // sync-ops
  // The wire message genuinely carries no fields, so `{}` is the shape;
  // `object` / `unknown` would not describe it.
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  | {} // request/missing contact card
  | { senderTotal: number; senderSyncpoint: number; senderDigest?: Uint8Array } // sync-check
  | { confirmerTotal: number }; // sync-confirmation

export interface SubductionEncodeInput {
  type: KeyhiveMessageType;
  senderId: PeerId;
  targetId: PeerId;
  /** The CBOR-encoded inline fields, as the existing SyncProtocol produces. */
  inlineDataCbor: Uint8Array;
}

export interface SubductionDecodeOutput {
  type: KeyhiveMessageType;
  senderId: PeerId;
  targetId: PeerId;
  /** The CBOR-encoded inline fields (camelCase, as the SyncProtocol expects). */
  inlineDataCbor: Uint8Array;
}

/**
 * Build the Rust-shape KeyhiveMessage payload bytes (CBOR) given the
 * existing TS Message shape (type/senderId/targetId/inline-CBOR data).
 *
 * Performs camelCase→snake_case conversion for the inline fields and
 * splices in the sender/target ids.
 */
export function encodeSubductionKeyhiveMessage(
  input: SubductionEncodeInput
): Uint8Array {
  const sender = peerIdToSubduction(input.senderId);
  const target = peerIdToSubduction(input.targetId);
  const inner = decodeInlineForType(input.type, input.inlineDataCbor);
  const variant = RUST_VARIANT_BY_TYPE[input.type];

  const variantContents: Record<string, unknown> = {
    sender_id: sender,
    target_id: target,
    ...inner,
  };

  return encode({ [variant]: variantContents });
}

/**
 * Decode a Rust-shape KeyhiveMessage CBOR payload into the TS Message-like
 * shape the SyncProtocol expects.
 */
export function decodeSubductionKeyhiveMessage(
  payload: Uint8Array
): SubductionDecodeOutput {
  const obj = decode(payload) as Record<string, unknown>;
  const variants = Object.keys(obj);
  if (variants.length !== 1) {
    throw new SukFrameError(
      `KeyhiveMessage envelope has ${variants.length} variants (expected 1)`
    );
  }
  const variant = variants[0];
  const type = TYPE_BY_RUST_VARIANT[variant];
  if (!type) {
    throw new SukFrameError(`unknown KeyhiveMessage variant: ${variant}`);
  }
  const v = obj[variant] as Record<string, unknown>;
  if (!v || typeof v !== "object") {
    throw new SukFrameError(`variant ${variant} payload is not a map`);
  }
  const senderId = peerIdFromSubduction(v.sender_id as SubductionPeerId);
  const targetId = peerIdFromSubduction(v.target_id as SubductionPeerId);
  const inlineDataCbor = encodeInlineForType(type, v);
  return { type, senderId, targetId, inlineDataCbor };
}

// ---------------------------------------------------------------------------
// Inline field shape conversion (camelCase ↔ snake_case)
// ---------------------------------------------------------------------------

function decodeInlineForType(
  type: KeyhiveMessageType,
  inlineDataCbor: Uint8Array
): Record<string, unknown> {
  if (
    type === "keyhive-sync-request-contact-card" ||
    type === "keyhive-sync-missing-contact-card"
  ) {
    return {};
  }
  const obj = (
    inlineDataCbor.length === 0 ? {} : decode(inlineDataCbor)
  ) as Record<string, unknown>;
  switch (type) {
    case "keyhive-sync-request":
      return {
        found: obj.found ?? [],
        pending: obj.pending ?? [],
      };
    case "keyhive-sync-response":
      return {
        requested: obj.requested ?? [],
        found: obj.found ?? [],
        sync_responder_total: obj.syncResponderTotal ?? 0,
        sync_requester_total: obj.syncRequesterTotal ?? 0,
      };
    case "keyhive-sync-ops":
      return {
        ops: obj.ops ?? [],
        sync_responder_total: obj.syncResponderTotal ?? 0,
        sync_requester_total: obj.syncRequesterTotal ?? 0,
      };
    case "keyhive-sync-check":
      return {
        sender_total: obj.senderTotal ?? 0,
        sender_syncpoint: obj.senderSyncpoint ?? 0,
        sender_digest: obj.senderDigest ?? ZERO_DIGEST,
      };
    case "keyhive-sync-confirmation":
      return { confirmer_total: obj.confirmerTotal ?? 0 };
    default: {
      const _exhaustive: never = type;
      void _exhaustive;
      return {};
    }
  }
}

function encodeInlineForType(
  type: KeyhiveMessageType,
  v: Record<string, unknown>
): Uint8Array {
  let inline: Record<string, unknown>;
  switch (type) {
    case "keyhive-sync-request":
      inline = { found: v.found ?? [], pending: v.pending ?? [] };
      break;
    case "keyhive-sync-response":
      inline = {
        requested: v.requested ?? [],
        found: v.found ?? [],
        syncResponderTotal: Number(v.sync_responder_total ?? 0),
        syncRequesterTotal: Number(v.sync_requester_total ?? 0),
      };
      break;
    case "keyhive-sync-ops":
      inline = {
        ops: v.ops ?? [],
        syncResponderTotal: Number(v.sync_responder_total ?? 0),
        syncRequesterTotal: Number(v.sync_requester_total ?? 0),
      };
      break;
    case "keyhive-sync-request-contact-card":
    case "keyhive-sync-missing-contact-card":
      return new Uint8Array();
    case "keyhive-sync-check":
      inline = {
        senderTotal: Number(v.sender_total ?? 0),
        senderSyncpoint: Number(v.sender_syncpoint ?? 0),
        senderDigest: (v.sender_digest as Uint8Array) ?? ZERO_DIGEST,
      };
      break;
    case "keyhive-sync-confirmation":
      inline = { confirmerTotal: Number(v.confirmer_total ?? 0) };
      break;
    default: {
      const _exhaustive: never = type;
      void _exhaustive;
      inline = {};
    }
  }
  return encode(inline);
}
