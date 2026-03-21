export class Peer {
  lastKeyhiveRequestRcvd = Date.now();
  lastKeyhiveRequestSent = Date.now();
  // The remote peer's hash count (for our shared peer pair) at the last sync.
  // null before first full sync is completed
  syncpoint: number | null = null;
}
