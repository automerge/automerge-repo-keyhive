import { Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { cborByteString } from "./cbor-builder.js";
import { getEventsForAgent, hasIndividual } from "../utilities.js";

const MAX_ENTRIES = 100000;

interface CachedEventBytes {
  raw: Uint8Array;
  cbor: Uint8Array;
}

// Cache for event bytes and their pre-encoded CBOR representations.
// Events are immutable, so entries never need invalidation, but we do
// evict past a maximum number of entries.
export class EventBytesCache {
  private entries: Map<string, CachedEventBytes> = new Map();

  // Check which hashes have cached bytes. Returns found results and the set
  // of hash strings still missing.
  getBytesFor(hashStrings: Set<string>): {
    events: Uint8Array[];
    cborEvents: Uint8Array[];
    missingHashes: Set<string>;
  } {
    const events: Uint8Array[] = [];
    const cborEvents: Uint8Array[] = [];
    const missingHashes = new Set<string>();
    for (const hashStr of hashStrings) {
      const entry = this.entries.get(hashStr);
      if (entry) {
        events.push(entry.raw);
        cborEvents.push(entry.cbor);
      } else {
        missingHashes.add(hashStr);
      }
    }
    return { events, cborEvents, missingHashes };
  }

  // Store WASM-fetched events in the cache and append those matching
  // missingHashes to the provided events/cborEvents arrays.
  storeAndCollect(
    wasmEvents: Map<Uint8Array, Uint8Array>,
    missingHashes: Set<string>,
    events: Uint8Array[],
    cborEvents: Uint8Array[]
  ): void {
    for (const [hash, eventBytes] of wasmEvents.entries()) {
      const hashStr = hash.toString();
      let entry = this.entries.get(hashStr);
      if (!entry) {
        entry = { raw: eventBytes, cbor: cborByteString(eventBytes) };
        this.entries.set(hashStr, entry);
      }
      if (missingHashes.has(hashStr)) {
        events.push(entry.raw);
        cborEvents.push(entry.cbor);
      }
    }
    // Batch eviction after all inserts
    while (this.entries.size > MAX_ENTRIES) {
      const oldest = this.entries.keys().next().value;
      if (oldest !== undefined) {
        this.entries.delete(oldest);
      }
    }
  }
}

// Fetch events for a specific agent from keyhive WASM API.
export async function fetchAgentEvents(
  keyhive: Keyhive,
  agentId: Identifier
): Promise<Map<Uint8Array, Uint8Array>> {
  if (!(await hasIndividual(keyhive, agentId))) {
    return new Map<Uint8Array, Uint8Array>();
  }
  return await getEventsForAgent(keyhive, await keyhive.getPrincipal(agentId));
}
