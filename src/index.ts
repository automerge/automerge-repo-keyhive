export { initKeyhiveWasm, isWasmInitialized } from "./wasm.js";
export { setKeyhiveLogLevel, getKeyhiveLogLevel } from "./logging.js";
export type { KeyhiveLogLevel } from "./logging.js";
export type { Active } from "./keyhive/active.js";
export { KeyhiveEventEmitter } from "./keyhive/emitter.js";
export {
  LegacyAutomergeRepoKeyhive,
  AutomergeRepoKeyhive,
  AutomergeRepoKeyhiveBase,
} from "./keyhive/automerge-repo-keyhive.js";
export type {
  DocMember,
  WrapNetworkAdapterOptions,
  CreateKeyhiveNetworkAdapter,
} from "./keyhive/automerge-repo-keyhive.js";
export type { KeyhiveEmitterEvents } from "./keyhive/emitter.js";
export { KeyhiveBlobInterceptor } from "./keyhive/blob-interceptor.js";
export {
  initializeLegacyAutomergeRepoKeyhive,
  initializeAutomergeRepoKeyhive,
  KeyhiveStorage,
  docIdFromAutomergeUrl,
} from "./keyhive/keyhive.js";
export type {
  SyncServerIdentity,
  SyncServerSelection,
  KeyhiveIdentityOptions,
  KeyhiveSyncOptions,
  LegacyHiveOptions,
  SubductionHiveOptions,
  InitRepoLinkOptions,
  InitializeLegacyAutomergeRepoKeyhiveOptions,
  InitializeAutomergeRepoKeyhiveOptions,
} from "./keyhive/keyhive.js";
export { KeyhiveNetworkAdapter } from "./network-adapter/network-adapter.js";
export {
  KeyhiveSubductionAdapter,
  encodeSukFrame,
  decodeSukFrame,
  isSukFrame,
  SUK_SCHEMA,
  encodeSignedMessage,
  decodeSignedMessage,
  encodeSubductionKeyhiveMessage,
  decodeSubductionKeyhiveMessage,
  peerIdFromSubduction,
  peerIdToSubduction,
  SukFrameError,
} from "./network-adapter/subduction-transport/index.js";
export type {
  KeyhiveSubductionAdapterOptions,
  KeyhiveMessageType,
  SubductionEncodeInput,
  SubductionDecodeOutput,
  SubductionPeerId,
  SubductionSignedMessage,
} from "./network-adapter/subduction-transport/index.js";
export type { SyncServer } from "./sync-server.js";
export {
  isUnprotectedDoc,
  UnprotectedDocError,
  peerIdFromSigner,
  uint8ArrayToHex,
  verifyingKeyPeerIdWithoutSuffix,
} from "./utilities.js";

// Re-export all keyhive types.
export * from "@keyhive/keyhive/slim";
