export class Pending {
  private nextSeqNumber = 1;
  private lastCompleted = 0;
  private readonly pending: Record<number, () => void> = {};

  register(): number {
    return this.nextSeqNumber++;
  }

  fire(seqNumber: number, fn: () => void): void {
    if (seqNumber === this.lastCompleted + 1) {
      fn();
      this.lastCompleted++;
      this.processQueue();
    } else {
      this.pending[seqNumber] = fn;
    }
  }

  cancel(seqNumber: number): void {
    if (seqNumber === this.lastCompleted + 1) {
      this.lastCompleted++;
      this.processQueue();
      if (seqNumber in this.pending) {
        delete this.pending[seqNumber];
      }
    } else {
      this.pending[seqNumber] = () => {};
    }
  }

  seqIds(): string[] {
    return Object.keys(this.pending)
  }

  private processQueue(): void {
    let seqNumber = this.lastCompleted + 1;

    while (seqNumber in this.pending) {
      const fn = this.pending[seqNumber];
      delete this.pending[seqNumber];

      fn();
      this.lastCompleted++;
      seqNumber++;
    }
  }
}

export class PromiseQueue {
  private queue: Promise<void> = Promise.resolve();
  private _enqueued = 0;
  private _completed = 0;

  run<T>(fn: () => Promise<T>): Promise<T> {
    this._enqueued++;
    const result = this.queue.then(fn);
    this.queue = result.then(
      () => { this._completed++; },
      () => { this._completed++; }
    );
    return result;
  }

  get depth(): number { return this._enqueued - this._completed; }
  get enqueued(): number { return this._enqueued; }
  get completed(): number { return this._completed; }
}
