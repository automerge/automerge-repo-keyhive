// Map from hash string to hash bytes
export type PeerHashes = Map<string, Uint8Array>;

export interface EventBytesResult {
  events: Uint8Array[];
  cborEvents: Uint8Array[];
}
