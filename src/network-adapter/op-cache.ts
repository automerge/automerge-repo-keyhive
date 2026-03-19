import { Keyhive, Identifier } from "@keyhive/keyhive/slim";
import { cborByteString } from "./cbor-builder.js";
import { getPendingOpHashes } from "../keyhive/keyhive.js";

// Map from hash string to hash bytes
type PeerHashes = Map<string, Uint8Array>;

interface EventBytesResult {
  events: Uint8Array[];
  cborEvents: Uint8Array[];
}

// Periodically-refreshed cache of all agent event hashes and event bytes.
// Avoids re-fetching from WASM on every sync message. Pre-encodes event
// bytes as CBOR byte strings for efficiently constructing responses.
//
// Both prekey and membership ops use two-tier indirection in the WASM API
// (agent -> source identifiers -> hashes). This cache resolves both during
// refresh into flat per-agent PeerHashes maps.

export class OpCache {
  // Pre-computed per-agent hash maps (rebuilt on refresh)
  private agentHashes: Map<string, PeerHashes> = new Map();
  private publicHashes: PeerHashes = new Map();

  // hash string -> raw event bytes
  // NOTE: events are immutable so both eventBytes and eventCborBytes grow
  // monotonically as new events arrive.
  private eventBytes: Map<string, Uint8Array> = new Map();
  // hash string -> pre-encoded CBOR byte strings
  private eventCborBytes: Map<string, Uint8Array> = new Map();
  // Pending op hashes
  private pendingOpHashes: Uint8Array[] = [];
  // totalOps at last refresh (for change detection)
  private lastTotalOps: bigint = 0n;

  private publicIdStr: string = Identifier.publicId().toBytes().toString();

  getHashesForAgent(agentIdStr: string): PeerHashes | null {
    return this.agentHashes.get(agentIdStr) ?? null;
  }

  getPublicHashes(): PeerHashes {
    return this.publicHashes;
  }

  agentHasHash(agentIdStr: string, hashStr: string): boolean {
    return this.agentHashes.get(agentIdStr)?.has(hashStr) ?? false;
  }

  getPendingOpHashes(): Uint8Array[] {
    return this.pendingOpHashes;
  }

  getEventBytesForHashes(hashStrings: Set<string>): EventBytesResult | null {
    const events: Uint8Array[] = [];
    const cborEvents: Uint8Array[] = [];
    for (const hashStr of hashStrings) {
      const bytes = this.eventBytes.get(hashStr);
      const cbor = this.eventCborBytes.get(hashStr);
      if (bytes && cbor) {
        events.push(bytes);
        cborEvents.push(cbor);
      } else {
        return null;
      }
    }
    return { events, cborEvents };
  }

  async refresh(keyhive: Keyhive): Promise<boolean> {
    const stats = await keyhive.stats();
    // NOTE: This is imperfect, because the events for an agent can reduce after a
    // revocation and build up again with separate ops.
    if (stats.totalOps === this.lastTotalOps) {
      return false;
    }

    const allAgentEvents = await keyhive.allAgentEvents();

    // Build hash lookup: hashStr -> hashBytes
    const allHashes = new Map<string, Uint8Array>();
    allAgentEvents.events.forEach((eventBytesVal: Uint8Array, hash: Uint8Array) => {
      const hashStr = hash.toString();
      allHashes.set(hashStr, hash);
      if (!this.eventBytes.has(hashStr)) {
        this.eventBytes.set(hashStr, eventBytesVal);
        this.eventCborBytes.set(hashStr, cborByteString(eventBytesVal));
      }
    });

    // Build source -> hashes indexes
    const prekeySourceHashes = buildSourceHashes(allAgentEvents.prekeySources);
    const membershipSourceHashes = buildSourceHashes(allAgentEvents.membershipSources);

    // Build agent -> sources indexes
    const agentPrekeySources = buildAgentSources(allAgentEvents.agentPrekeySources);
    const agentMembershipSources = buildAgentSources(allAgentEvents.agentMembershipSources);

    // Pre-compute per-agent PeerHashes maps
    const newAgentHashes = new Map<string, PeerHashes>();
    const allAgentIds = new Set([
      ...agentPrekeySources.keys(),
      ...agentMembershipSources.keys(),
    ]);
    for (const agentIdStr of allAgentIds) {
      const peerHashes: PeerHashes = new Map();

      collectSourceHashes(agentPrekeySources.get(agentIdStr), prekeySourceHashes, allHashes, peerHashes);
      collectSourceHashes(agentMembershipSources.get(agentIdStr), membershipSourceHashes, allHashes, peerHashes);

      newAgentHashes.set(agentIdStr, peerHashes);
    }

    const newPendingOpHashes = await getPendingOpHashes(keyhive);

    // "Atomic" swap
    this.agentHashes = newAgentHashes;
    this.publicHashes = newAgentHashes.get(this.publicIdStr) ?? new Map();
    this.pendingOpHashes = newPendingOpHashes;
    this.lastTotalOps = stats.totalOps;

    return true;
  }
}

// Build source -> Set<hashStr> from a WASM sources map
function buildSourceHashes(
  sourcesMap: Map<Uint8Array, Uint8Array[]>,
): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>();
  sourcesMap.forEach((hashes: Uint8Array[], sourceIdBytes: Uint8Array) => {
    const sourceKey = sourceIdBytes.toString();
    const hashSet = new Set<string>();
    for (const hash of hashes) {
      hashSet.add(hash.toString());
    }
    result.set(sourceKey, hashSet);
  });
  return result;
}

// Build agent -> sourceKey[] from a WASM agent-sources map
function buildAgentSources(
  agentSourcesMap: Map<Uint8Array, Uint8Array[]>,
): Map<string, string[]> {
  const result = new Map<string, string[]>();
  agentSourcesMap.forEach((sourceIdBytes: Uint8Array[], agentIdBytes: Uint8Array) => {
    const agentIdStr = agentIdBytes.toString();
    const sourceKeys: string[] = [];
    for (const idBytes of sourceIdBytes) {
      sourceKeys.push(idBytes.toString());
    }
    result.set(agentIdStr, sourceKeys);
  });
  return result;
}

// Collect hashes from source keys into a PeerHashes map
function collectSourceHashes(
  sourceKeys: string[] | undefined,
  sourceHashes: Map<string, Set<string>>,
  allHashes: Map<string, Uint8Array>,
  peerHashes: PeerHashes,
): void {
  if (!sourceKeys) return;
  for (const sourceKey of sourceKeys) {
    const hashStrs = sourceHashes.get(sourceKey);
    if (hashStrs) {
      for (const hashStr of hashStrs) {
        const hashBytes = allHashes.get(hashStr);
        if (hashBytes) peerHashes.set(hashStr, hashBytes);
      }
    }
  }
}
