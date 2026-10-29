import { decode } from "cbor-x";

/**
 * Decode CBOR received from the network. Use this instead of cbor-x's
 * `decode` for any bytes another party could have produced.
 *
 * cbor-x reads past the end of its input as zeros and does not bounds-check
 * while filling arrays, so a few bytes declaring a huge or unterminated array
 * make it run for tens of seconds and allocate gigabytes before it throws.
 * Checking the structure first, in time linear in the input, prevents that.
 */
export function decodeCbor(bytes: Uint8Array): ReturnType<typeof decode> {
  assertWellFormedCbor(bytes);
  return decode(bytes);
}

/**
 * Throw unless `bytes` is exactly one well-formed CBOR data item: every
 * string fits in the remaining input, every container's items are present,
 * and every indefinite-length item ends with a break.
 */
function assertWellFormedCbor(bytes: Uint8Array): void {
  let pos = 0;
  const fail = (reason: string): never => {
    throw new Error(`[cbor] ${reason} at byte ${pos}`);
  };
  // Items still expected by each open container, innermost last. An
  // indefinite-length container expects Infinity and is closed by a break.
  const open: number[] = [1];
  while (open.length > 0) {
    if (pos >= bytes.length) fail("unexpected end of input");
    const initial = bytes[pos++];
    if (initial === 0xff) {
      if (open.pop() !== Infinity) fail("unexpected break");
    } else {
      open[open.length - 1]--;
      const major = initial >> 5;
      const info = initial & 0x1f;
      if (info === 31) {
        if (major < 2 || major > 5) fail("invalid indefinite length");
        open.push(Infinity);
      } else {
        let arg = info;
        if (info >= 28) fail("reserved additional information");
        if (info >= 24) {
          const size = 1 << (info - 24);
          if (size > bytes.length - pos) fail("unexpected end of input");
          arg = 0;
          for (let i = 0; i < size; i++) arg = arg * 256 + bytes[pos++];
        }
        if (major === 2 || major === 3) {
          if (arg > bytes.length - pos) fail("string longer than input");
          pos += arg;
        } else if (major === 4) {
          open.push(arg);
        } else if (major === 5) {
          open.push(arg * 2);
        } else if (major === 6) {
          open.push(1);
        }
      }
    }
    while (open.length > 0 && open[open.length - 1] === 0) open.pop();
  }
  if (pos !== bytes.length) fail("trailing bytes");
}
