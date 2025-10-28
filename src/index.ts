import packageJson from "../package.json" with { type: "json" };

console.log(
  `automerge-repo-keyhive version: ${packageJson.version}`
);

export { Active } from "./keyhive/active.js";
export { addMemberToDoc, revokeMemberFromDoc } from "./keyhive/doc.js";
export { KeyhiveEventEmitter } from "./keyhive/emitter.js";
export { AutomergeRepoKeyhive, docIdFromAutomergeUrl, initializeAutomergeRepoKeyhive } from "./keyhive/keyhive.js";
export { KeyhiveNetworkAdapter } from "./network-adapter/network-adapter.js";
export { SyncServer } from "./sync-server.js";
export { peerIdFromSigner, uint8ArrayToHex, verifyingKeyPeerIdWithoutSuffix } from "./utilities.js";
