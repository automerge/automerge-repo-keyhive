import { Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { PeerId } from "@automerge/automerge-repo/slim";
import { cborByteString } from "./cbor-builder.js";
import { getPendingOpHashes } from "../keyhive/keyhive.js";
import { keyhiveIdentifierFromPeerId } from "../utilities.js";
import { Metrics } from "./metrics.js";
import { EventBytesCache, fetchAgentAndPublicEvents } from "./event-bytes-cache.js";
import type { EventCache } from "./event-cache.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";

// Periodically-refreshed cache of all agent event hashes and event bytes.
// Avoids re-fetching from WASM on every sync message. Pre-encodes event
// bytes as CBOR byte strings for efficiently constructing responses.
//
// Both prekey and membership ops use two-tier indirection in the WASM API
// (agent -> source identifiers -> hashes). This cache resolves both during
// refresh into flat per-agent PeerHashes maps.

export class PeriodicEventCache implements EventCache {
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

  // Fallback cache for event bytes misses
  private eventBytesCache = new EventBytesCache();

  private publicIdStr: string = Identifier.publicId().toBytes().toString();
  // Cache PeerId -> agentIdStr to avoid repeated hex decode and WASM call
  private peerIdToAgentIdStr: Map<PeerId, string> = new Map();

  private agentIdStrForPeer(peerId: PeerId): string {
    let agentIdStr = this.peerIdToAgentIdStr.get(peerId);
    if (agentIdStr === undefined) {
      agentIdStr = keyhiveIdentifierFromPeerId(peerId).toBytes().toString();
      this.peerIdToAgentIdStr.set(peerId, agentIdStr);
    }
    return agentIdStr;
  }

  getPendingOpHashes(_keyhive: Keyhive, metrics?: Metrics): Promise<Uint8Array[]> {
    metrics?.recordCacheHit();
    return Promise.resolve(this.pendingOpHashes);
  }

  getPublicHashes(_keyhive: Keyhive, metrics?: Metrics): Promise<PeerHashes> {
    metrics?.recordCacheHit();
    return Promise.resolve(this.publicHashes);
  }

  getHashesForPeer(_keyhive: Keyhive, peerId: PeerId, metrics?: Metrics): Promise<PeerHashes | null> {
    const cached = this.agentHashes.get(this.agentIdStrForPeer(peerId));
    if (cached) {
      metrics?.recordCacheHit();
      return Promise.resolve(cached);
    }
    // Agent not in cache
    metrics?.recordCacheMiss();
    return Promise.resolve(null);
  }

  async getEventBytesForPeer(
    keyhive: Keyhive,
    peerId: PeerId,
    hashStrings: Set<string>,
    metrics?: Metrics,
  ): Promise<EventBytesResult> {
    const eventLookupStart = Date.now();

    // Try periodic cache first
    const periodicResult = this.getEventBytesFromPeriodicCache(hashStrings);
    if (periodicResult) {
      metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
      return periodicResult;
    }

    console.debug(`[AMRepoKeyhive] PeriodicEventCache miss for ${hashStrings.size} hashes, falling back to EventBytesCache/WASM`);

    // Fall back to event bytes cache and keyhive WASM API
    const { events, cborEvents, missingHashes } = this.eventBytesCache.getBytesFor(hashStrings);

    if (missingHashes.size === 0) {
      metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
      return { events, cborEvents };
    }

    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const fetchedEvents = await fetchAgentAndPublicEvents(keyhive, keyhiveId);
    this.eventBytesCache.storeAndCollect(fetchedEvents, missingHashes, events, cborEvents);

    metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
    return { events, cborEvents };
  }

  onKeyhiveChanged(): void {
    // no-op: refreshes on its own timer
  }

  // TODO: This is called when attempting recovery from storage before
  // intiating sync protocol. Refreshing here is playing it safe but we
  // might want to just wait until the next scheduled refresh.
  async onMaybeChanged(keyhive: Keyhive): Promise<void> {
    await this.refresh(keyhive);
  }

  onPeerDisconnected(_peerId: PeerId): void {
    // no-op: data rebuilt on next refresh
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

  private getEventBytesFromPeriodicCache(hashStrings: Set<string>): EventBytesResult | null {
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
}

// Build source -> Set<hashStr> from a sources map
function buildSourceHashes(
  sourcesMap: Map<Uint8Array, Uint8Array[]>,
): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>();
  sourcesMap.forEach((hashes: Uint8Array[], sourceIdBytes: Uint8Array) => {
    result.set(sourceIdBytes.toString(), new Set(hashes.map(h => h.toString())));
  });
  return result;
}

// Build agent -> sourceKey[] from an agent-sources map
function buildAgentSources(
  agentSourcesMap: Map<Uint8Array, Uint8Array[]>,
): Map<string, string[]> {
  const result = new Map<string, string[]>();
  agentSourcesMap.forEach((sourceIdBytes: Uint8Array[], agentIdBytes: Uint8Array) => {
    result.set(agentIdBytes.toString(), sourceIdBytes.map(id => id.toString()));
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
