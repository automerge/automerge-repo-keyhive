// Manual CBOR construction for pre-encoded event bytes.
// Avoids re-encoding immutable events on every sync response.

export function cborByteString(bytes: Uint8Array): Uint8Array {
  const len = bytes.length;
  let header: Uint8Array;
  if (len < 24) {
    header = new Uint8Array([0x40 | len]);
  } else if (len < 0x100) {
    header = new Uint8Array([0x58, len]);
  } else if (len < 0x10000) {
    header = new Uint8Array([0x59, len >> 8, len & 0xff]);
  } else {
    header = new Uint8Array([0x5a, (len >>> 24) & 0xff, (len >>> 16) & 0xff, (len >>> 8) & 0xff, len & 0xff]);
  }
  const result = new Uint8Array(header.length + len);
  result.set(header);
  result.set(bytes, header.length);
  return result;
}

function cborArrayHeader(length: number): Uint8Array {
  if (length < 24) return new Uint8Array([0x80 | length]);
  if (length < 0x100) return new Uint8Array([0x98, length]);
  if (length < 0x10000) return new Uint8Array([0x99, length >> 8, length & 0xff]);
  return new Uint8Array([0x9a, (length >>> 24) & 0xff, (length >>> 16) & 0xff, (length >>> 8) & 0xff, length & 0xff]);
}

function cborMapHeader(length: number): Uint8Array {
  if (length < 24) return new Uint8Array([0xa0 | length]);
  if (length < 0x100) return new Uint8Array([0xb8, length]);
  if (length < 0x10000) return new Uint8Array([0xb9, length >> 8, length & 0xff]);
  return new Uint8Array([0xba, (length >>> 24) & 0xff, (length >>> 16) & 0xff, (length >>> 8) & 0xff, length & 0xff]);
}

function cborTextString(str: string): Uint8Array {
  const bytes = new TextEncoder().encode(str);
  const len = bytes.length;
  let header: Uint8Array;
  if (len < 24) {
    header = new Uint8Array([0x60 | len]);
  } else if (len < 0x100) {
    header = new Uint8Array([0x78, len]);
  } else if (len < 0x10000) {
    header = new Uint8Array([0x79, len >> 8, len & 0xff]);
  } else {
    header = new Uint8Array([0x7a, (len >>> 24) & 0xff, (len >>> 16) & 0xff, (len >>> 8) & 0xff, len & 0xff]);
  }
  const result = new Uint8Array(header.length + len);
  result.set(header);
  result.set(bytes, header.length);
  return result;
}

// Pre-encode the static CBOR text string keys
const CBOR_KEY_REQUESTED = cborTextString("requested");
const CBOR_KEY_FOUND = cborTextString("found");
const CBOR_KEY_SENDER_TOTAL = cborTextString("senderTotal");
const CBOR_KEY_RECEIVER_TOTAL = cborTextString("receiverTotal");
const CBOR_KEY_OPS = cborTextString("ops");

export function cborUint(value: number): Uint8Array {
  if (value < 24) return new Uint8Array([value]);
  if (value < 0x100) return new Uint8Array([0x18, value]);
  if (value < 0x10000) return new Uint8Array([0x19, value >> 8, value & 0xff]);
  return new Uint8Array([0x1a, (value >>> 24) & 0xff, (value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff]);
}

function concatParts(parts: Uint8Array[]): Uint8Array {
  let totalLength = 0;
  for (const p of parts) totalLength += p.length;
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const p of parts) {
    result.set(p, offset);
    offset += p.length;
  }
  return result;
}

// Build CBOR: { "requested": [...], "found": [...], "senderTotal"?: N, "receiverTotal"?: N }
export function buildSyncResponseCbor(
  requested: Uint8Array[],
  cborFoundEvents: Uint8Array[],
  senderTotal?: number,
  receiverTotal?: number,
): Uint8Array {
  const parts: Uint8Array[] = [];

  const hasMetadata = senderTotal !== undefined && receiverTotal !== undefined;
  parts.push(cborMapHeader(hasMetadata ? 4 : 2));

  // "requested": [byte strings]
  parts.push(CBOR_KEY_REQUESTED);
  parts.push(cborArrayHeader(requested.length));
  for (const item of requested) {
    parts.push(cborByteString(item));
  }

  // "found": [pre-encoded byte strings]
  parts.push(CBOR_KEY_FOUND);
  parts.push(cborArrayHeader(cborFoundEvents.length));
  for (const item of cborFoundEvents) {
    parts.push(item);
  }

  if (hasMetadata) {
    parts.push(CBOR_KEY_SENDER_TOTAL);
    parts.push(cborUint(senderTotal));
    parts.push(CBOR_KEY_RECEIVER_TOTAL);
    parts.push(cborUint(receiverTotal));
  }

  return concatParts(parts);
}

// Build CBOR: { "ops": [...pre-encoded byte strings...], "senderTotal": N, "receiverTotal": N }
export function buildSyncOpsCbor(
  cborItems: Uint8Array[],
  senderTotal: number,
  receiverTotal: number,
): Uint8Array {
  const parts: Uint8Array[] = [];

  parts.push(cborMapHeader(3));

  parts.push(CBOR_KEY_OPS);
  parts.push(cborArrayHeader(cborItems.length));
  for (const item of cborItems) {
    parts.push(item);
  }

  parts.push(CBOR_KEY_SENDER_TOTAL);
  parts.push(cborUint(senderTotal));
  parts.push(CBOR_KEY_RECEIVER_TOTAL);
  parts.push(cborUint(receiverTotal));

  return concatParts(parts);
}

// Build CBOR: [pre-encoded byte strings]
export function buildCborByteStringArray(cborItems: Uint8Array[]): Uint8Array {
  const header = cborArrayHeader(cborItems.length);
  let totalLength = header.length;
  for (const item of cborItems) totalLength += item.length;
  const result = new Uint8Array(totalLength);
  result.set(header);
  let offset = header.length;
  for (const item of cborItems) {
    result.set(item, offset);
    offset += item.length;
  }
  return result;
}
