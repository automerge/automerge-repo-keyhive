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
// Prekey ops use two-tier indirection in the WASM API (agent -> source
// identifiers -> hashes). Membership ops are flat per-agent. This cache
// resolves both during refresh into flat per-agent PeerHashes maps.

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

    // allAgentEvents() returns:
    //   events: Map<Uint8Array(hash), Uint8Array(eventBytes)>
    //   prekeySources: Map<Uint8Array(identifierBytes), Uint8Array[](hashes)>
    //   agentPrekeySources: Map<Uint8Array(agentId), Uint8Array[](identifierBytes)>
    //   agentMembershipHashes: Map<Uint8Array(agentId), Uint8Array[](hashes)>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const result = await (keyhive as any).allAgentEvents();

    // Build hash lookup: hashStr -> hashBytes
    const allHashes = new Map<string, Uint8Array>();
    result.events.forEach((eventBytesVal: Uint8Array, hash: Uint8Array) => {
      const hashStr = hash.toString();
      allHashes.set(hashStr, hash);
      if (!this.eventBytes.has(hashStr)) {
        this.eventBytes.set(hashStr, eventBytesVal);
        this.eventCborBytes.set(hashStr, cborByteString(eventBytesVal));
      }
    });

    // Build prekey sources: sourceKey -> Set<hashStr>
    const prekeySourceHashes = new Map<string, Set<string>>();
    result.prekeySources.forEach((hashes: Uint8Array[], idBytes: Uint8Array) => {
      const sourceKey = idBytes.toString();
      const hashSet = new Set<string>();
      for (const hash of hashes) {
        hashSet.add(hash.toString());
      }
      prekeySourceHashes.set(sourceKey, hashSet);
    });

    // Build agent prekey source index: agentIdStr -> sourceKey[]
    const agentPrekeySources = new Map<string, string[]>();
    result.agentPrekeySources.forEach((sourceIdBytes: Uint8Array[], agentIdBytes: Uint8Array) => {
      const agentIdStr = agentIdBytes.toString();
      const sourceKeys: string[] = [];
      for (const idBytes of sourceIdBytes) {
        sourceKeys.push(idBytes.toString());
      }
      agentPrekeySources.set(agentIdStr, sourceKeys);
    });

    // Build agent membership hash sets: agentIdStr -> Set<hashStr>
    const agentMembershipHashSets = new Map<string, Set<string>>();
    result.agentMembershipHashes.forEach((hashes: Uint8Array[], agentIdBytes: Uint8Array) => {
      const agentIdStr = agentIdBytes.toString();
      const hashSet = new Set<string>();
      for (const hash of hashes) {
        hashSet.add(hash.toString());
      }
      agentMembershipHashSets.set(agentIdStr, hashSet);
    });

    // Pre-compute per-agent PeerHashes maps
    const newAgentHashes = new Map<string, PeerHashes>();
    const allAgentIds = new Set([
      ...agentPrekeySources.keys(),
      ...agentMembershipHashSets.keys(),
    ]);
    for (const agentIdStr of allAgentIds) {
      const peerHashes: PeerHashes = new Map();

      // Add prekey hashes (agent -> source keys -> hash sets)
      const sources = agentPrekeySources.get(agentIdStr);
      if (sources) {
        for (const sourceKey of sources) {
          const sourceHashStrs = prekeySourceHashes.get(sourceKey);
          if (sourceHashStrs) {
            for (const hashStr of sourceHashStrs) {
              const hashBytes = allHashes.get(hashStr);
              if (hashBytes) peerHashes.set(hashStr, hashBytes);
            }
          }
        }
      }

      // Add membership hashes (agent -> hash sets)
      const membershipHashStrs = agentMembershipHashSets.get(agentIdStr);
      if (membershipHashStrs) {
        for (const hashStr of membershipHashStrs) {
          const hashBytes = allHashes.get(hashStr);
          if (hashBytes) peerHashes.set(hashStr, hashBytes);
        }
      }

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
