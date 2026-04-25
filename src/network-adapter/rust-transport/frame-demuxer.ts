// WebSocket binary-frame demultiplexer.
//
// Each subduction WebSocket frame begins with a 4-byte schema header:
//   "SHK\0" — handshake
//   "SUM\0" — sync (sedimentree)
//   "SUE\0" — ephemeral
//   "SUK\0" — keyhive
//
// The Rust subduction server's `subduction_websocket` transport delivers
// each frame's bytes directly; this demultiplexer mirrors that on the
// TS side. SUK frames are routed to the keyhive consumer; everything else
// (or any frame the keyhive consumer hasn't claimed) is routed to a
// `Transport`-shaped channel that subduction-WASM can use via
// `subduction.connectTransport(transport, serviceName)`.
//
// We use the typed `WebSocket` from `isomorphic-ws` so this works in
// both Node.js and the browser, mirroring `WebSocketTransport` in
// automerge-repo's subduction module.

import WebSocket from "isomorphic-ws";
import type { Transport } from "@automerge/automerge-subduction/slim";
import { isSukFrame } from "./codec.js";

type SubductionFrameHandler = (bytes: Uint8Array) => void;

/**
 * Multiplexes a single WebSocket between subduction-WASM and the
 * keyhive layer.
 *
 * - `subductionTransport` — pass to `subduction.connectTransport()`.
 *   It receives every non-SUK frame (the SHK handshake, then SUM/SUE).
 * - `setSukHandler(cb)` — register a callback for inbound SUK frames.
 * - `sendSuk(bytes)` — send an outbound SUK-framed payload.
 * - `closed()` — resolves when the WebSocket closes.
 */
export class FrameDemuxer {
  readonly subductionTransport: Transport;

  #ws: WebSocket;
  #subductionQueue: Uint8Array[] = [];
  #subductionWaiters: Array<(bytes: Uint8Array) => void> = [];
  #subductionErrorWaiters: Array<(err: Error) => void> = [];
  #sukHandler: SubductionFrameHandler | null = null;
  #closed = false;
  #closedPromise: Promise<void>;
  #closedResolve!: () => void;
  #disconnectCallback: (() => void) | null = null;

  constructor(ws: WebSocket) {
    this.#ws = ws;
    this.#closedPromise = new Promise<void>((r) => {
      this.#closedResolve = r;
    });
    ws.binaryType = "arraybuffer";

    ws.addEventListener("message", (event: WebSocket.MessageEvent) => {
      const raw = event.data as ArrayBuffer | Uint8Array;
      const bytes =
        raw instanceof ArrayBuffer
          ? new Uint8Array(raw)
          : new Uint8Array(
              (raw as Uint8Array).buffer,
              (raw as Uint8Array).byteOffset,
              (raw as Uint8Array).byteLength,
            );

      if (isSukFrame(bytes)) {
        if (this.#sukHandler) {
          try {
            this.#sukHandler(bytes);
          } catch (err) {
            console.error("[FrameDemuxer] SUK handler threw:", err);
          }
        } else {
          // No handler yet — drop. Keyhive frames before the keyhive
          // layer is ready are unexpected from a well-behaved peer.
          console.warn(
            "[FrameDemuxer] dropping SUK frame: no handler registered"
          );
        }
        return;
      }

      const waiter = this.#subductionWaiters.shift();
      if (waiter) {
        this.#subductionErrorWaiters.shift();
        waiter(bytes);
      } else {
        this.#subductionQueue.push(bytes);
      }
    });

    ws.addEventListener("close", () => {
      this.#markClosed(new Error("WebSocket closed"));
    });

    ws.addEventListener("error", (event: WebSocket.ErrorEvent) => {
      const err =
        "error" in event && event.error instanceof Error
          ? event.error
          : new Error("WebSocket error");
      this.#markClosed(err);
    });

    this.subductionTransport = {
      sendBytes: async (bytes: Uint8Array): Promise<void> => {
        if (this.#closed) throw new Error("WebSocket closed");
        this.#ws.send(bytes.slice());
      },
      recvBytes: (): Promise<Uint8Array> => {
        const queued = this.#subductionQueue.shift();
        if (queued) return Promise.resolve(queued);
        if (this.#closed) return Promise.reject(new Error("WebSocket closed"));
        return new Promise<Uint8Array>((resolve, reject) => {
          this.#subductionWaiters.push(resolve);
          this.#subductionErrorWaiters.push(reject);
        });
      },
      disconnect: async (): Promise<void> => {
        this.#teardown({ fireDisconnectCallback: false });
      },
      onDisconnect: (callback: () => void): void => {
        this.#disconnectCallback = callback;
      },
    };
  }

  /**
   * Open a WebSocket and return a demuxer wrapping it. Resolves once the
   * socket is `open`.
   */
  static connect(url: string): Promise<FrameDemuxer> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      const onOpen = () => {
        ws.removeEventListener("open", onOpen);
        ws.removeEventListener("error", onError);
        resolve(new FrameDemuxer(ws));
      };
      const onError = (event: WebSocket.ErrorEvent) => {
        ws.removeEventListener("open", onOpen);
        ws.removeEventListener("error", onError);
        const err =
          "error" in event && event.error instanceof Error
            ? event.error
            : new Error("WebSocket connection failed");
        reject(err);
      };
      ws.addEventListener("open", onOpen);
      ws.addEventListener("error", onError);
    });
  }

  setSukHandler(handler: SubductionFrameHandler | null): void {
    this.#sukHandler = handler;
  }

  /**
   * Send a SUK-framed payload (already wrapped with SUK\0 + length).
   */
  sendSuk(bytes: Uint8Array): void {
    if (this.#closed) {
      throw new Error("FrameDemuxer: WebSocket closed");
    }
    this.#ws.send(bytes.slice());
  }

  /** Resolves when the underlying WebSocket closes (for any reason). */
  closed(): Promise<void> {
    return this.#closedPromise;
  }

  isClosed(): boolean {
    return this.#closed;
  }

  disconnect(): void {
    this.#teardown({ fireDisconnectCallback: false });
  }

  #markClosed(err: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closedResolve();
    for (const ew of this.#subductionErrorWaiters) ew(err);
    this.#subductionErrorWaiters = [];
    this.#subductionWaiters = [];
  }

  #teardown({
    fireDisconnectCallback,
  }: { fireDisconnectCallback?: boolean } = {}): void {
    this.#closed = true;
    this.#closedResolve();
    try {
      this.#ws.close();
    } catch {
      // ignore
    }
    if (fireDisconnectCallback && this.#disconnectCallback) {
      this.#disconnectCallback();
    }
  }
}
