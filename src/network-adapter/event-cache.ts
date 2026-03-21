import { Keyhive } from "@keyhive/keyhive/slim";
import { PeerId } from "@automerge/automerge-repo/slim";
import { Metrics } from "./metrics.js";
import type { PeerHashes, EventBytesResult } from "./sync-data.js";

export interface EventCache {
  getPendingOpHashes(keyhive: Keyhive, metrics?: Metrics): Promise<Uint8Array[]>;
  getPublicHashes(keyhive: Keyhive, metrics?: Metrics): Promise<PeerHashes>;
  getHashesForPeer(keyhive: Keyhive, peerId: PeerId, metrics?: Metrics): Promise<PeerHashes | null>;
  getEventBytesForPeer(keyhive: Keyhive, peerId: PeerId, hashStrings: Set<string>, metrics?: Metrics): Promise<EventBytesResult>;

  // Signal that keyhive state has changed (e.g., after ingesting ops or local events).
  onKeyhiveChanged(): void;

  // Signal that keyhive state may have changed (e.g., after storage ingestion).
  onMaybeChanged(keyhive: Keyhive): Promise<void>;

  onPeerDisconnected(peerId: PeerId): void;
}
