import { Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { PeerId } from "@automerge/automerge-repo/slim";
import { getPendingOpHashes } from "../keyhive/keyhive.js";
import { getEventHashesForAgent, keyhiveIdentifierFromPeerId } from "../utilities.js";
import { Metrics } from "./metrics.js";
import { EventBytesCache, fetchAgentAndPublicEvents } from "./event-bytes-cache.js";
import type { EventCache } from "./event-cache.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";

export class StandardEventCache implements EventCache {
  private hashesCache: Map<PeerId, PeerHashes> = new Map();
  private publicHashesCache: PeerHashes | null = null;
  private pendingOpHashesCache: Uint8Array[] | null = null;
  private lastKnownTotalOps: bigint = 0n;
  private eventBytesCache = new EventBytesCache();

  async getPendingOpHashes(keyhive: Keyhive, metrics?: Metrics): Promise<Uint8Array[]> {
    if (this.pendingOpHashesCache !== null) {
      metrics?.recordCacheHit();
      return this.pendingOpHashesCache;
    }
    metrics?.recordCacheMiss();
    const hashes = await getPendingOpHashes(keyhive);
    this.pendingOpHashesCache = hashes;
    return hashes;
  }

  async getPublicHashes(keyhive: Keyhive, metrics?: Metrics): Promise<PeerHashes> {
    if (this.publicHashesCache !== null) {
      metrics?.recordCacheHit();
      return this.publicHashesCache;
    }
    metrics?.recordCacheMiss();
    const agent = await keyhive.getAgent(Identifier.publicId());
    const hashes = agent ? await getEventHashesForAgent(keyhive, agent) : new Map();
    this.publicHashesCache = hashes;
    return hashes;
  }

  async getHashesForPeer(keyhive: Keyhive, peerId: PeerId, metrics?: Metrics): Promise<PeerHashes | null> {
    const cached = this.hashesCache.get(peerId);
    if (cached) {
      metrics?.recordCacheHit();
      return cached;
    }
    metrics?.recordCacheMiss();

    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await keyhive.getAgent(keyhiveId);
    if (!agent) {
      return null;
    }
    const hashes = await getEventHashesForAgent(keyhive, agent);
    this.hashesCache.set(peerId, hashes);
    return hashes;
  }

  async getEventBytesForPeer(
    keyhive: Keyhive,
    peerId: PeerId,
    hashStrings: Set<string>,
    metrics?: Metrics,
  ): Promise<EventBytesResult> {
    const eventLookupStart = Date.now();
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
    this.hashesCache.clear();
    this.publicHashesCache = null;
    this.pendingOpHashesCache = null;
  }

  async onMaybeChanged(keyhive: Keyhive): Promise<void> {
    const stats = await keyhive.stats();
    const currentTotalOps = stats.totalOps;
    if (currentTotalOps !== this.lastKnownTotalOps) {
      console.debug(
        `[AMRepoKeyhive] Total ops changed from ${this.lastKnownTotalOps} to ${currentTotalOps}, invalidating cache`
      );
      this.lastKnownTotalOps = currentTotalOps;
      this.onKeyhiveChanged();
    }
  }

  onPeerDisconnected(peerId: PeerId): void {
    this.hashesCache.delete(peerId);
  }
}
