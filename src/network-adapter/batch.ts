import { log } from "../logging.js";
import type { Message } from "@automerge/automerge-repo/slim";
import type { Keyhive } from "@keyhive/keyhive/slim";
import type { KeyhiveMessageData, KeyhiveMessage } from "./messages.js";
import { Metrics } from "./metrics.js";

export class MessageBatch {
  readonly messages: KeyhiveMessage[] = [];
  readonly metrics = new Metrics();
  private readonly syncRequestSenders = new Set<string>();

  add(msg: Message, data: KeyhiveMessageData) {
    if (msg.type === "keyhive-sync-request" && msg.senderId) {
      if (this.syncRequestSenders.has(msg.senderId)) {
        this.metrics.recordDroppedSyncRequest();
        return;
      }
      this.syncRequestSenders.add(msg.senderId);
    }
    this.metrics.recordMessage(msg.type, msg.senderId, data.signed.payload?.byteLength ?? 0);
    this.messages.push({ msg, data });
  }

  countNonKeyhive() {
    this.metrics.recordNonKeyhive();
  }

  get isEmpty(): boolean {
    return !this.metrics.hasActivity();
  }
}

export class BatchProcessor {
  private timeoutId?: ReturnType<typeof setTimeout>;

  constructor(
    private readonly batchInterval: number,
    private readonly keyhive: Keyhive,
    private readonly handleMessage: (msg: Message, data: KeyhiveMessageData, metrics: Metrics) => Promise<void>,
    private readonly swapBatch: () => MessageBatch,
  ) {}

  start() {
    this.scheduleNext();
  }

  stop() {
    if (this.timeoutId !== undefined) {
      clearTimeout(this.timeoutId);
      this.timeoutId = undefined;
    }
  }

  private scheduleNext() {
    this.timeoutId = setTimeout(() => { void this.processAndReschedule() }, this.batchInterval);
  }

  private async processAndReschedule() {
    const batch = this.swapBatch();
    if (!batch.isEmpty) {
      await this.processBatch(batch);
    }
    this.scheduleNext();
  }

  private async processBatch(batch: MessageBatch) {
    const startTime = Date.now();
    for (const { msg, data } of batch.messages) {
      try {
        const msgStart = Date.now();
        await this.handleMessage(msg, data, batch.metrics);
        batch.metrics.recordProcessingTimeByType(msg.type ?? "unknown", Date.now() - msgStart);
      } catch (error) {
        log.error(`[AMRepoKeyhive] Error processing batch message (type=${msg.type}, from=${msg.senderId}):`, error);
      }
    }
    batch.metrics.recordProcessingTime(Date.now() - startTime);
    const stats = await this.keyhive.stats();
    batch.metrics.recordTotalOps(stats.totalOps);
    batch.metrics.logReport("Batch");
  }
}
