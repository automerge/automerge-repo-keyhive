import { Keyhive, Identifier } from "@keyhive/keyhive/slim";
import { cborByteString } from "./cbor-builder.js";
import { getPendingOpHashes } from "../keyhive/keyhive.js";

// Map from hash string to hash bytes
type PeerHashes = Map<string, Uint8Array>;

interface EventBytesResult {
  events: Uint8Array[];
  cborEvents: Uint8Array[];
}

// Memory-efficient hash storage with two-tier prekey sharing.
//
// Prekey ops use indirection: each group/doc/individual identifier maps to a set of
// hash IDs (stored once), and each agent maps to a list of identifier keys. On lookup,
// we union the hash sets of the agent's prekey sources.
//
// Membership ops are stored flat per-agent (no sharing, since AllMembershipOps
// flattens to per-agent digest sets in keyhive_core).
//
// All hashes are interned to integer IDs to save memory.

export class OpCache {
  // Hash interning: hash string → integer ID, and reverse lookup
  private hashToId: Map<string, number> = new Map();
  private idToHashStr: string[] = [];
  private idToHashBytes: Uint8Array[] = [];
  private nextHashId: number = 0;

  // Prekey tier 1: identifier string → Set<hash ID> (shared across agents)
  private prekeySourceHashes: Map<string, Set<number>> = new Map();

  // Prekey tier 2: agent identifier string → list of prekey source identifier strings
  private agentPrekeySources: Map<string, string[]> = new Map();

  // Membership: agent identifier string → Set<hash ID> (flat, per-agent)
  private agentMembershipHashes: Map<string, Set<number>> = new Map();

  // hash string → raw event bytes (grows monotonically, events are immutable)
  private eventBytes: Map<string, Uint8Array> = new Map();
  // hash string → pre-encoded CBOR byte strings
  private eventCborBytes: Map<string, Uint8Array> = new Map();
  // Pending op hashes
  private pendingOpHashes: Uint8Array[] = [];
  // totalOps at last refresh (for change detection)
  private lastTotalOps: bigint = 0n;

  private publicIdStr: string = Identifier.publicId().toBytes().toString();

  private internHash(hashStr: string, hashBytes: Uint8Array): number {
    let id = this.hashToId.get(hashStr);
    if (id === undefined) {
      id = this.nextHashId++;
      this.hashToId.set(hashStr, id);
      this.idToHashStr[id] = hashStr;
      this.idToHashBytes[id] = hashBytes;
    }
    return id;
  }

  // Collect all hash IDs reachable by an agent (prekey sources + membership)
  private collectAgentHashIds(agentIdStr: string): Set<number> | null {
    const prekeySources = this.agentPrekeySources.get(agentIdStr);
    const membershipHashes = this.agentMembershipHashes.get(agentIdStr);
    if (!prekeySources && !membershipHashes) return null;

    const result = new Set<number>();

    if (prekeySources) {
      for (const sourceKey of prekeySources) {
        const sourceHashIds = this.prekeySourceHashes.get(sourceKey);
        if (sourceHashIds) {
          for (const hashId of sourceHashIds) {
            result.add(hashId);
          }
        }
      }
    }

    if (membershipHashes) {
      for (const hashId of membershipHashes) {
        result.add(hashId);
      }
    }

    return result;
  }

  // Returns a PeerHashes map for the given agent
  getHashesForAgent(agentIdStr: string): PeerHashes | null {
    const hashIds = this.collectAgentHashIds(agentIdStr);
    if (!hashIds) return null;

    const result: PeerHashes = new Map();
    for (const hashId of hashIds) {
      result.set(this.idToHashStr[hashId], this.idToHashBytes[hashId]);
    }
    return result;
  }

  getPublicHashes(): PeerHashes {
    return this.getHashesForAgent(this.publicIdStr) ?? new Map();
  }

  agentHasHash(agentIdStr: string, hashStr: string): boolean {
    const hashId = this.hashToId.get(hashStr);
    if (hashId === undefined) return false;

    const membershipHashes = this.agentMembershipHashes.get(agentIdStr);
    if (membershipHashes?.has(hashId)) return true;

    const prekeySources = this.agentPrekeySources.get(agentIdStr);
    if (prekeySources) {
      for (const sourceKey of prekeySources) {
        const sourceHashIds = this.prekeySourceHashes.get(sourceKey);
        if (sourceHashIds?.has(hashId)) return true;
      }
    }

    return false;
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

    // Reset interning tables (rebuild from scratch each refresh)
    this.hashToId = new Map();
    this.idToHashStr = [];
    this.idToHashBytes = [];
    this.nextHashId = 0;

    // Intern all events and store event bytes (monotonically growing)
    result.events.forEach((eventBytesVal: Uint8Array, hash: Uint8Array) => {
      const hashStr = hash.toString();
      this.internHash(hashStr, hash);
      if (!this.eventBytes.has(hashStr)) {
        this.eventBytes.set(hashStr, eventBytesVal);
        this.eventCborBytes.set(hashStr, cborByteString(eventBytesVal));
      }
    });

    // Build prekey sources: identifier string → Set<hash ID>
    const newPrekeySourceHashes = new Map<string, Set<number>>();
    result.prekeySources.forEach((hashes: Uint8Array[], idBytes: Uint8Array) => {
      const sourceKey = idBytes.toString();
      const hashIds = new Set<number>();
      for (const hash of hashes) {
        const hashId = this.hashToId.get(hash.toString());
        if (hashId !== undefined) hashIds.add(hashId);
      }
      newPrekeySourceHashes.set(sourceKey, hashIds);
    });

    // Build agent prekey source index: agent string → list of source key strings
    const newAgentPrekeySources = new Map<string, string[]>();
    result.agentPrekeySources.forEach((sourceIdBytes: Uint8Array[], agentIdBytes: Uint8Array) => {
      const agentIdStr = agentIdBytes.toString();
      const sourceKeys: string[] = [];
      for (const idBytes of sourceIdBytes) {
        sourceKeys.push(idBytes.toString());
      }
      newAgentPrekeySources.set(agentIdStr, sourceKeys);
    });

    // Build agent membership hashes: agent string → Set<hash ID>
    const newAgentMembershipHashes = new Map<string, Set<number>>();
    result.agentMembershipHashes.forEach((hashes: Uint8Array[], agentIdBytes: Uint8Array) => {
      const agentIdStr = agentIdBytes.toString();
      const hashIds = new Set<number>();
      for (const hash of hashes) {
        const hashId = this.hashToId.get(hash.toString());
        if (hashId !== undefined) hashIds.add(hashId);
      }
      newAgentMembershipHashes.set(agentIdStr, hashIds);
    });

    const newPendingOpHashes = await getPendingOpHashes(keyhive);

    // Atomic swap
    this.prekeySourceHashes = newPrekeySourceHashes;
    this.agentPrekeySources = newAgentPrekeySources;
    this.agentMembershipHashes = newAgentMembershipHashes;
    this.pendingOpHashes = newPendingOpHashes;
    this.lastTotalOps = stats.totalOps;

    return true;
  }
}
