import { Identifier, Keyhive } from "@keyhive/keyhive/slim";
import { PeerId } from "@automerge/automerge-repo/slim";
import { getPendingOpHashes } from "../keyhive/keyhive.js";
import {
  getEventHashesForAgent,
  keyhiveIdentifierFromPeerId,
} from "../utilities.js";
import { Metrics } from "./metrics.js";
import {
  EventBytesCache,
  fetchAgentAndPublicEvents,
} from "./event-bytes-cache.js";
import type { EventCache } from "./event-cache.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";

export class EventBytesOnlyEventCache implements EventCache {
  private eventBytesCache = new EventBytesCache();

  async getPendingOpHashes(
    keyhive: Keyhive,
    _metrics?: Metrics
  ): Promise<Uint8Array[]> {
    return await getPendingOpHashes(keyhive);
  }

  async getPublicHashes(
    keyhive: Keyhive,
    _metrics?: Metrics
  ): Promise<PeerHashes> {
    const agent = await keyhive.getAgent(Identifier.publicId());
    if (!agent) {
      return new Map();
    }
    return await getEventHashesForAgent(keyhive, agent);
  }

  async getHashesForPeer(
    keyhive: Keyhive,
    peerId: PeerId,
    _metrics?: Metrics
  ): Promise<PeerHashes | null> {
    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const agent = await keyhive.getAgent(keyhiveId);
    if (!agent) {
      return null;
    }
    return await getEventHashesForAgent(keyhive, agent);
  }

  async getEventBytesForPeer(
    keyhive: Keyhive,
    peerId: PeerId,
    hashStrings: Set<string>,
    metrics?: Metrics
  ): Promise<EventBytesResult> {
    const eventLookupStart = Date.now();
    const { events, cborEvents, missingHashes } =
      this.eventBytesCache.getBytesFor(hashStrings);

    if (missingHashes.size === 0) {
      metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
      return { events, cborEvents };
    }

    const keyhiveId = keyhiveIdentifierFromPeerId(peerId);
    const fetchedEvents = await fetchAgentAndPublicEvents(keyhive, keyhiveId);
    this.eventBytesCache.storeAndCollect(
      fetchedEvents,
      missingHashes,
      events,
      cborEvents
    );

    metrics?.recordEventLookupTime(Date.now() - eventLookupStart);
    return { events, cborEvents };
  }

  onKeyhiveChanged(): void {
    // no-op: nothing cached beyond event bytes
  }

  async onMaybeChanged(_keyhive: Keyhive): Promise<void> {
    // no-op: nothing cached
  }

  onPeerDisconnected(_peerId: PeerId): void {
    // no-op: nothing cached
  }
}
