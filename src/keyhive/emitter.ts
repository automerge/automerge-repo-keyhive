import { Event } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

export interface KeyhiveEmitterEvents {
  /** A keyhive event was applied (local mutation or remote ingestion). */
  update: (event: Event) => void;
  /** Document data was encrypted (blob interceptor activity). */
  encrypt: () => void;
}

export class KeyhiveEventEmitter extends EventEmitter<KeyhiveEmitterEvents> {
  handleKeyhiveEvent = (event: Event) => {
    this.emit("update", event);
  };
}
