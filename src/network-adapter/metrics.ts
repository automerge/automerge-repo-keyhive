export class Metrics {
  private msgTypeCounts: Record<string, number> = {};
  private totalPayloadBytes = 0;
  private uniqueSenders = new Set<string>();
  private nonKeyhiveCount = 0;
  private droppedSyncRequests = 0;
  private messageCount = 0;
  private totalProcessingTimeMs = 0;

  private processingTimeByType: Record<string, number> = {};
  private totalOps: bigint = 0n;
  private hashLookupTimeMs = 0;
  private eventLookupTimeMs = 0;
  private cacheHits = 0;
  private cacheMisses = 0;
  private totalQueueWaitMs = 0;
  private ingestCount = 0;
  private eventsIngested = 0;
  private pendingAfterIngest = 0;
  private storageRetries = 0;
  private opsSent = 0;
  private opsRequested = 0;
  private syncChecksSent = 0;
  private syncChecksReceived = 0;
  private syncChecksShortCircuited = 0;
  private syncChecksFallback = 0;
  private syncConfirmationsSent = 0;
  private syncConfirmationsReceived = 0;

  recordMessage(msgType: string | undefined, senderId: string | undefined, payloadBytes: number) {
    const type = msgType ?? "unknown";
    this.msgTypeCounts[type] = (this.msgTypeCounts[type] ?? 0) + 1;
    this.totalPayloadBytes += payloadBytes;
    if (senderId) this.uniqueSenders.add(senderId);
    this.messageCount++;
  }

  recordNonKeyhive() {
    this.nonKeyhiveCount++;
  }

  recordDroppedSyncRequest() {
    this.droppedSyncRequests++;
  }

  recordProcessingTime(ms: number) {
    this.totalProcessingTimeMs += ms;
  }

  recordProcessingTimeByType(msgType: string, ms: number) {
    this.processingTimeByType[msgType] = (this.processingTimeByType[msgType] ?? 0) + ms;
  }

  recordTotalOps(ops: bigint) {
    this.totalOps = ops;
  }

  recordHashLookupTime(ms: number) {
    this.hashLookupTimeMs += ms;
  }

  recordEventLookupTime(ms: number) {
    this.eventLookupTimeMs += ms;
  }

  recordCacheHit() {
    this.cacheHits++;
  }

  recordCacheMiss() {
    this.cacheMisses++;
  }

  recordQueueWait(ms: number) {
    this.totalQueueWaitMs += ms;
  }

  recordIngestion(eventsCount: number, pendingCount: number) {
    this.ingestCount++;
    this.eventsIngested += eventsCount;
    this.pendingAfterIngest = pendingCount;
  }

  recordStorageRetry() {
    this.storageRetries++;
  }

  recordOpsSent(count: number) {
    this.opsSent += count;
  }

  recordOpsRequested(count: number) {
    this.opsRequested += count;
  }

  recordSyncCheckSent() { this.syncChecksSent++; }
  recordSyncCheckReceived() { this.syncChecksReceived++; }
  recordSyncCheckShortCircuited() { this.syncChecksShortCircuited++; }
  recordSyncCheckFallback() { this.syncChecksFallback++; }
  recordSyncConfirmationSent() { this.syncConfirmationsSent++; }
  recordSyncConfirmationReceived() { this.syncConfirmationsReceived++; }

  hasActivity(): boolean {
    return this.messageCount > 0 || this.nonKeyhiveCount > 0;
  }

  logReport(label: string) {
    if (!this.hasActivity()) return;
    const countsStr = Object.entries(this.msgTypeCounts)
      .map(([type, count]) => `${type}=${count}`)
      .join(", ");
    console.debug(
      `[${label}] ${this.messageCount} keyhive messages from ${this.uniqueSenders.size} peers at ${new Date().toLocaleTimeString("en-GB")}. ` +
      `${this.droppedSyncRequests} duplicate sync requests dropped. ` +
      `${this.nonKeyhiveCount} non-keyhive messages. ` +
      `Breakdown: ${countsStr}. Total payload: ${this.totalPayloadBytes} bytes. ` +
      `Processing: ${this.totalProcessingTimeMs}ms.`
    );
    const perTypeStr = Object.entries(this.processingTimeByType)
      .map(([type, ms]) => `${type}=${ms}ms`)
      .join(", ");
    console.debug(
      `[${label}+] Per-type: ${perTypeStr}. ` +
      `Lookups: hash=${this.hashLookupTimeMs}ms, event=${this.eventLookupTimeMs}ms. ` +
      `Cache: ${this.cacheHits}/${this.cacheMisses} hit/miss. ` +
      `Queue wait: ${this.totalQueueWaitMs}ms. ` +
      `Ingestion: ${this.ingestCount}x, ${this.eventsIngested} events, ${this.pendingAfterIngest} pending, ${this.storageRetries} retries. ` +
      `Ops: ${this.opsSent} sent, ${this.opsRequested} requested. ` +
      `Sync checks: ${this.syncChecksSent} sent, ${this.syncChecksReceived} rcvd, ${this.syncChecksShortCircuited} short-circuited, ${this.syncChecksFallback} fallback. ` +
      `Confirmations: ${this.syncConfirmationsSent} sent, ${this.syncConfirmationsReceived} rcvd. ` +
      `Total ops: ${this.totalOps}`
    );
  }

}
