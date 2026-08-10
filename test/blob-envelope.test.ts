import { describe, it, expect } from "vitest";
import {
  COMMIT_ID_BYTES,
  ENVELOPE_VERSION,
  PRED_ENTRY_BYTES,
  decodeOuterEnvelope,
  decodePreds,
  encodeOuterEnvelope,
} from "../src/keyhive/blob-interceptor.js";

/** Deterministic filler so failures are reproducible. */
function bytes(length: number, seed: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) out[i] = (seed + i * 31) & 0xff;
  return out;
}

function hex(b: Uint8Array): string {
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

/** A predecessor table as `transformOutgoing` builds it, before encryption. */
function predsPlaintext(
  entries: Array<{ id: Uint8Array; key: Uint8Array }>
): Uint8Array {
  const out = new Uint8Array(entries.length * PRED_ENTRY_BYTES);
  entries.forEach(({ id, key }, i) => {
    out.set(id, i * PRED_ENTRY_BYTES);
    out.set(key, i * PRED_ENTRY_BYTES + COMMIT_ID_BYTES);
  });
  return out;
}

describe("outer envelope", () => {
  it("round-trips inner and predecessor payloads", () => {
    const inner = bytes(200, 7);
    const predsCipher = bytes(137, 91);

    const decoded = decodeOuterEnvelope(
      encodeOuterEnvelope(inner, predsCipher)
    );

    expect(decoded).not.toBeNull();
    expect(decoded!.inner).toEqual(inner);
    expect(decoded!.predsCipher).toEqual(predsCipher);
  });

  it("round-trips empty payloads on either side", () => {
    const inner = bytes(64, 3);

    const noPreds = decodeOuterEnvelope(
      encodeOuterEnvelope(inner, new Uint8Array(0))
    );
    expect(noPreds!.inner).toEqual(inner);
    expect(noPreds!.predsCipher).toHaveLength(0);

    const noInner = decodeOuterEnvelope(
      encodeOuterEnvelope(new Uint8Array(0), inner)
    );
    expect(noInner!.inner).toHaveLength(0);
    expect(noInner!.predsCipher).toEqual(inner);
  });

  it("writes a 5-byte header: version, then inner length little-endian", () => {
    const encoded = encodeOuterEnvelope(bytes(300, 1), bytes(4, 2));

    expect(encoded[0]).toBe(ENVELOPE_VERSION);
    // 300 == 0x012c, little-endian.
    expect([...encoded.slice(1, 5)]).toEqual([0x2c, 0x01, 0x00, 0x00]);
    expect(encoded).toHaveLength(5 + 300 + 4);
  });

  it("decodes a blob that sits at a non-zero offset in its buffer", () => {
    // The interceptor hands decode whatever the storage layer produced, which
    // may be a view into a larger buffer rather than its own allocation.
    const inner = bytes(48, 11);
    const predsCipher = bytes(16, 12);
    const encoded = encodeOuterEnvelope(inner, predsCipher);

    const backing = new Uint8Array(encoded.length + 9);
    backing.fill(0xee);
    backing.set(encoded, 9);
    const view = backing.subarray(9);

    const decoded = decodeOuterEnvelope(view);

    expect(decoded).not.toBeNull();
    expect(decoded!.inner).toEqual(inner);
    expect(decoded!.predsCipher).toEqual(predsCipher);
  });

  it("returns null for input too short to hold a header", () => {
    for (let length = 0; length < 5; length++) {
      const truncated = new Uint8Array(length);
      truncated[0] = ENVELOPE_VERSION;
      expect(decodeOuterEnvelope(truncated)).toBeNull();
    }
  });

  it("returns null on a version mismatch", () => {
    const encoded = encodeOuterEnvelope(bytes(32, 5), bytes(8, 6));
    encoded[0] = ENVELOPE_VERSION + 1;

    expect(decodeOuterEnvelope(encoded)).toBeNull();
  });

  it("returns null when the body is truncated below the declared length", () => {
    const encoded = encodeOuterEnvelope(bytes(64, 5), new Uint8Array(0));

    // One byte short of the declared inner length.
    expect(decodeOuterEnvelope(encoded.subarray(0, encoded.length - 1))).toBe(
      null
    );
    // Header intact, body gone entirely.
    expect(decodeOuterEnvelope(encoded.subarray(0, 5))).toBeNull();
  });

  it("returns null for an inner length that overflows a signed int", () => {
    // getUint32 is unsigned, so a high bit set must not read back negative and
    // slip past the bounds check.
    const encoded = encodeOuterEnvelope(bytes(16, 5), new Uint8Array(0));
    new DataView(encoded.buffer).setUint32(1, 0xffffffff, true);

    expect(decodeOuterEnvelope(encoded)).toBeNull();
  });

  it("treats an unencoded blob from another producer as not ours", () => {
    expect(decodeOuterEnvelope(new Uint8Array([0, 1, 2, 3, 4, 5]))).toBeNull();
  });
});

describe("decodePreds", () => {
  it("round-trips predecessor id/key pairs in order", () => {
    const entries = [
      { id: bytes(COMMIT_ID_BYTES, 1), key: bytes(32, 2) },
      { id: bytes(COMMIT_ID_BYTES, 3), key: bytes(32, 4) },
      { id: bytes(COMMIT_ID_BYTES, 5), key: bytes(32, 6) },
    ];

    const decoded = decodePreds(predsPlaintext(entries));

    expect(decoded).toHaveLength(3);
    decoded.forEach((got, i) => {
      expect(got.idHex).toBe(hex(entries[i].id));
      expect(got.key).toEqual(entries[i].key);
    });
  });

  it("returns nothing for an empty table", () => {
    expect(decodePreds(new Uint8Array(0))).toEqual([]);
  });

  it("ignores a trailing partial entry", () => {
    const entries = [{ id: bytes(COMMIT_ID_BYTES, 1), key: bytes(32, 2) }];
    const table = predsPlaintext(entries);
    const withTail = new Uint8Array(table.length + PRED_ENTRY_BYTES - 1);
    withTail.set(table);

    const decoded = decodePreds(withTail);

    expect(decoded).toHaveLength(1);
    expect(decoded[0].idHex).toBe(hex(entries[0].id));
  });

  it("ignores input shorter than a single entry", () => {
    expect(decodePreds(bytes(PRED_ENTRY_BYTES - 1, 1))).toEqual([]);
  });

  it("copies keys rather than viewing the source buffer", () => {
    // The caller caches these keys; a view would alias a buffer it does not own.
    const table = predsPlaintext([
      { id: bytes(COMMIT_ID_BYTES, 1), key: bytes(32, 2) },
    ]);
    const decoded = decodePreds(table);
    const before = new Uint8Array(decoded[0].key);

    table.fill(0);

    expect(decoded[0].key).toEqual(before);
  });
});
