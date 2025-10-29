import { Event } from "@keyhive/keyhive/slim";
import { EventEmitter } from "eventemitter3";

export class KeyhiveEventEmitter extends EventEmitter {
  constructor() {
    super();
  }

  handleKeyhiveEvent = (event: Event) => {
    this.emit("update", event);
  };
}
