import packageJson from "../package.json" with { type: "json" };

console.log(
  `automerge-repo-keyhive version: ${packageJson.version}`
);

export * from "./keyhive/active.js";
export * from "./keyhive/doc.js";
export * from "./keyhive/emitter.js";
export * from "./keyhive/keyhive.js";
export * from "./network-adapter/messages.js";
export * from "./network-adapter/network-adapter.js";
export * from "./sync-server.js";
export * from "./utilities.js";
