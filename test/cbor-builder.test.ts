import { describe, it, expect } from "vitest";
import { encode, decode } from "cbor-x";
import {
  cborByteString,
  buildSyncResponseCbor,
  buildCborByteStringArray,
} from "../src/network-adapter/cbor-builder";

// Helper: generate a Uint8Array of given length with pseudo-random content
function randomBytes(length: number, seed: number = 0): Uint8Array {
  const bytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    bytes[i] = (seed + i * 37 + 13) & 0xff;
  }
  return bytes;
}

// Compare two Uint8Arrays by value (handles Buffer vs Uint8Array differences)
function expectBytesEqual(actual: Uint8Array | Buffer, expected: Uint8Array) {
  expect(new Uint8Array(actual)).toEqual(expected);
}

describe("cborByteString", () => {
  const sizes = [0, 10, 23, 24, 100, 255, 256, 1000, 65535, 65536, 70000];

  for (const size of sizes) {
    it(`round-trips correctly for ${size} bytes`, () => {
      const bytes = randomBytes(size);
      const encoded = cborByteString(bytes);
      const decoded = decode(Buffer.from(encoded));
      expectBytesEqual(decoded, bytes);
    });
  }
});

describe("buildCborByteStringArray", () => {
  it("decodes to empty array", () => {
    const manual = buildCborByteStringArray([]);
    const decoded = decode(Buffer.from(manual));
    expect(decoded).toEqual([]);
  });

  it("decodes to same values as cbor-x encode", () => {
    const events = [randomBytes(50), randomBytes(100), randomBytes(200)];
    const cborEvents = events.map(cborByteString);
    const manual = buildCborByteStringArray(cborEvents);
    const decoded = decode(Buffer.from(manual)) as Uint8Array[];
    const reference = decode(encode(events)) as Uint8Array[];

    expect(decoded.length).toBe(reference.length);
    for (let i = 0; i < events.length; i++) {
      expectBytesEqual(decoded[i], events[i]);
      expectBytesEqual(reference[i], events[i]);
    }
  });

  it("handles large arrays (> 24 elements)", () => {
    const events = Array.from({ length: 30 }, (_, i) => randomBytes(50 + i, i));
    const cborEvents = events.map(cborByteString);
    const manual = buildCborByteStringArray(cborEvents);
    const decoded = decode(Buffer.from(manual)) as Uint8Array[];

    expect(decoded.length).toBe(30);
    for (let i = 0; i < events.length; i++) {
      expectBytesEqual(decoded[i], events[i]);
    }
  });

  it("works with mix of cached and fresh events", () => {
    // Simulate: some events pre-encoded (from persistent map), some just encoded
    const cached = [randomBytes(80, 1), randomBytes(120, 2)];
    const fresh = [randomBytes(90, 3), randomBytes(110, 4)];
    const allEvents = [...cached, ...fresh];

    const cachedCbor = cached.map(cborByteString);
    const freshCbor = fresh.map(cborByteString);

    const manual = buildCborByteStringArray([...cachedCbor, ...freshCbor]);
    const decoded = decode(Buffer.from(manual)) as Uint8Array[];

    expect(decoded.length).toBe(4);
    for (let i = 0; i < allEvents.length; i++) {
      expectBytesEqual(decoded[i], allEvents[i]);
    }
  });
});

describe("buildSyncResponseCbor", () => {
  it("decodes to empty response", () => {
    const manual = buildSyncResponseCbor([], []);
    const decoded = decode(Buffer.from(manual)) as { requested: any[]; found: any[] };
    expect(decoded.requested).toEqual([]);
    expect(decoded.found).toEqual([]);
  });

  it("decodes to same values as cbor-x encode", () => {
    const requested = [randomBytes(32, 10), randomBytes(32, 20)];
    const found = [randomBytes(150, 30), randomBytes(200, 40), randomBytes(100, 50)];
    const cborFound = found.map(cborByteString);

    const manual = buildSyncResponseCbor(requested, cborFound);
    const decoded = decode(Buffer.from(manual)) as { requested: Uint8Array[]; found: Uint8Array[] };
    const reference = decode(encode({ requested, found })) as { requested: Uint8Array[]; found: Uint8Array[] };

    expect(decoded.requested.length).toBe(reference.requested.length);
    expect(decoded.found.length).toBe(reference.found.length);
    for (let i = 0; i < requested.length; i++) {
      expectBytesEqual(decoded.requested[i], requested[i]);
    }
    for (let i = 0; i < found.length; i++) {
      expectBytesEqual(decoded.found[i], found[i]);
    }
  });

  it("handles only requested (no found)", () => {
    const requested = [randomBytes(32, 1), randomBytes(32, 2)];
    const manual = buildSyncResponseCbor(requested, []);
    const decoded = decode(Buffer.from(manual)) as { requested: Uint8Array[]; found: any[] };

    expect(decoded.requested.length).toBe(2);
    for (let i = 0; i < requested.length; i++) {
      expectBytesEqual(decoded.requested[i], requested[i]);
    }
    expect(decoded.found).toEqual([]);
  });

  it("handles only found (no requested)", () => {
    const found = [randomBytes(300, 1), randomBytes(400, 2)];
    const cborFound = found.map(cborByteString);
    const manual = buildSyncResponseCbor([], cborFound);
    const decoded = decode(Buffer.from(manual)) as { requested: any[]; found: Uint8Array[] };

    expect(decoded.requested).toEqual([]);
    expect(decoded.found.length).toBe(2);
    for (let i = 0; i < found.length; i++) {
      expectBytesEqual(decoded.found[i], found[i]);
    }
  });

  it("handles many events (> 24)", () => {
    const requested = Array.from({ length: 5 }, (_, i) => randomBytes(32, i));
    const found = Array.from({ length: 30 }, (_, i) => randomBytes(100 + i, i + 100));
    const cborFound = found.map(cborByteString);

    const manual = buildSyncResponseCbor(requested, cborFound);
    const decoded = decode(Buffer.from(manual)) as { requested: Uint8Array[]; found: Uint8Array[] };

    expect(decoded.requested.length).toBe(5);
    expect(decoded.found.length).toBe(30);
    for (let i = 0; i < found.length; i++) {
      expectBytesEqual(decoded.found[i], found[i]);
    }
  });

  it("works with mix of cached and fresh found events", () => {
    const requested = [randomBytes(32, 1)];

    // 3 events from persistent map + 2 just fetched from WASM
    const cachedEvents = [randomBytes(100, 10), randomBytes(150, 20), randomBytes(200, 30)];
    const freshEvents = [randomBytes(120, 40), randomBytes(180, 50)];
    const allFound = [...cachedEvents, ...freshEvents];

    const cachedCbor = cachedEvents.map(cborByteString);
    const freshCbor = freshEvents.map(cborByteString);
    const allCborFound = [...cachedCbor, ...freshCbor];

    const manual = buildSyncResponseCbor(requested, allCborFound);
    const decoded = decode(Buffer.from(manual)) as { requested: Uint8Array[]; found: Uint8Array[] };

    expectBytesEqual(decoded.requested[0], requested[0]);
    expect(decoded.found.length).toBe(5);
    for (let i = 0; i < allFound.length; i++) {
      expectBytesEqual(decoded.found[i], allFound[i]);
    }
  });
});
