import { Event } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

export class KeyhiveEventEmitter extends EventEmitter {
  constructor() {
    super();
  }

  handleKeyhiveEvent = (event: Event) => {
    const variant = event.variant;
    this.emit("update", { type: variant.toLowerCase(), event });
    // this.emit(variant.toLowerCase(), event);
  };
}
