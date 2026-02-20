import { Event } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

export class KeyhiveEventEmitter extends EventEmitter {
  isRemote = false

  constructor() {
    super();
  }

  handleKeyhiveEvent = (event: Event) => {
    ;(event as any).isRemote = this.isRemote
    this.emit("update", event);
  };
}
