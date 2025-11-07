import { Event } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

export class KeyhiveEventEmitter extends EventEmitter {
  constructor() {
    super();
  }

  handleKeyhiveEvent = (event: Event) => {
    console.log("!@ update fired for ", event.variant);
    this.emit("update", event);
  };
}
