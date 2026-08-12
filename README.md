# Automerge Repo Keyhive (ARK)

`@automerge/automerge-repo-keyhive` adds end-to-end access control and
encryption to [automerge-repo](https://github.com/automerge/automerge-repo)
using the [keyhive](https://github.com/inkandswitch/keyhive) protocol.
Documents have member lists with ordered access levels (relay, read, edit,
admin), membership changes sync over a dedicated protocol, and document data
is encrypted so sync servers relay ciphertext they cannot read.

## Install

```
pnpm add @automerge/automerge-repo-keyhive
```

## Quickstart

Initialize a hive and its repo, connected to a subduction sync server:

```ts
import {
  Access,
  initializeAutomergeRepoKeyhive,
} from "@automerge/automerge-repo-keyhive";
import { Repo } from "@automerge/automerge-repo";
import { IndexedDBStorageAdapter } from "@automerge/automerge-repo-storage-indexeddb";

const { hive, repo } = await initializeAutomergeRepoKeyhive({
  createRepo: (config) => new Repo(config),
  storage: new IndexedDBStorageAdapter("my-app-keyhive"),
  peerIdSuffix: "my-app",
  syncServer: "keyhive", // the identity of keyhive.sync.automerge.org
  repo: {
    storage: new IndexedDBStorageAdapter(),
    subductionWebsocketEndpoints: ["wss://keyhive.sync.automerge.org"],
  },
});

// Documents created with `create2` use the repo's id factory, so they get a
// keyhive document id and are keyhive-protected. `repo.create` bypasses the
// id factory and produces an unprotected document.
const handle = await repo.create2({ title: "hello" });

// Let the sync server relay the (encrypted) document, then grant a
// collaborator access using the contact card they shared with you.
await hive.addSyncServerRelayToDoc(handle.url);
await hive.addMemberToDoc(handle.url, collaboratorContactCard, Access.edit());
```

## Documentation

- [API guide](docs/automerge-repo-keyhive-api-guide.md): the full public
  API, including initialization on both transports, identity and contact
  cards, membership and access, sync servers, bundler setup, and the
  storage layout.
- [Example app](https://github.com/inkandswitch/keyhive-todo-app-demo): a
  complete todo application built on ARK, including a reference Vite
  configuration.

## Status

Alpha. Published for early adopters. The public API will change without
notice between releases. Pin an exact version.

The TypeScript implementation of the keyhive sync protocol will be removed in the near future in favor of a WASM API for the Rust implementation.
