# Automerge Repo Keyhive (ARK) Integration Guide

How to wire `automerge-repo-keyhive` (ARK) into an application. ARK adds access control
to Automerge documents via the keyhive protocol.

## Install

```
pnpm add @automerge/automerge-repo-keyhive
```

## Initialize WASM

Call once at startup, before any keyhive operations:

```ts
import { initKeyhiveWasm } from "@automerge/automerge-repo-keyhive";

initKeyhiveWasm();
```

## Initialize keyhive

There are two initialization paths depending on your sync transport.

### Legacy (automerge-repo network adapters)

Use `initializeAutomergeRepoKeyhive` when syncing through automerge-repo's built-in network adapters (e.g., `MessageChannelNetworkAdapter`, `BrowserWebSocketClientAdapter`,
etc.):

```ts
import {
  initializeAutomergeRepoKeyhive,
} from "@automerge/automerge-repo-keyhive";
import { IndexedDBStorageAdapter } from "@automerge/automerge-repo-storage-indexeddb";

const hive = await initializeAutomergeRepoKeyhive({
  storage: new IndexedDBStorageAdapter("my-app-keyhive"),
  peerIdSuffix: "my-app" + Math.random().toString(36).slice(2),
  networkAdapter: myNetworkAdapter, // e.g., MessageChannelNetworkAdapter
  automaticArchiveIngestion: true,
  cachingMode: "periodic",
});
```

Peer IDs have your keyhive ID as a prefix and the provided `PeerIdSuffix` as a suffix.
This allows, for example, multiple tabs to share the same keyhive identity while still
operating as distinct peers.

Then create the Repo using hive-provided values:

```ts
import { Repo } from "@automerge/automerge-repo";

const repo = new Repo({
  storage: new IndexedDBStorageAdapter(),
  enableRemoteHeadsGossiping: true,
  network: [hive.networkAdapter],
  peerId: hive.peerId,
  idFactory: hive.idFactory,
});

hive.linkRepo(repo);
```

NOTE: `hive.linkRepo(repo)` is currently necessary but will be removed soon.

### Rust subduction sync

Use `initializeAutomergeRepoKeyhiveRust` when syncing through subduction.

There is a circular dependency to manage: keyhive initialization needs a `Subduction` instance to send and receive SUK frames, but `Subduction` is created internally by `Repo`. A deferred promise breaks the cycle. Pass it to keyhive during init, then resolve it once the Repo is constructed.

```ts
import {
  initKeyhiveWasm,
  initializeAutomergeRepoKeyhiveRust,
} from "@automerge/automerge-repo-keyhive";
import { Repo } from "@automerge/automerge-repo";
import { IndexedDBStorageAdapter } from "@automerge/automerge-repo-storage-indexeddb";
import type { Subduction } from "@automerge/automerge-subduction/slim";

initKeyhiveWasm();

// 1. Create a deferred promise for the Subduction instance.
//    Keyhive will buffer outbound SUK frames until this resolves.
let resolveSubduction!: (s: Subduction) => void;
const subductionPromise = new Promise<Subduction>((resolve) => {
  resolveSubduction = resolve;
});

// 2. Initialize keyhive with the unresolved promise.
const hive = await initializeAutomergeRepoKeyhiveRust({
  storage: new IndexedDBStorageAdapter("my-app-keyhive"),
  peerIdSuffix: "my-app-worker" + Math.random().toString(36).slice(2),
  subduction: subductionPromise,
  automaticArchiveIngestion: true,
  cachingMode: "periodic",
});

// 3. Build a signer from keyhive's key pair so subduction and keyhive
//    sign as the same peer.
const signer = await hive.constructSubductionSigner();

// 4. Create the Repo. This internally creates the Subduction instance.
const repo = new Repo({
  storage: new IndexedDBStorageAdapter(),
  signer,
  subductionWebsocketEndpoints: ["wss://your-sync-server.example.com"],
  peerId: hive.peerId,
  enableRemoteHeadsGossiping: true,
  idFactory: hive.idFactory,
});

// 5. Resolve the deferred promise with the Repo's Subduction instance.
//    repo.subduction is a Promise<Subduction> that resolves once the
//    Repo's subduction subsystem is ready. At this point, keyhive will
//    be able to send and receive SUK frames.
repo.subduction.then(resolveSubduction);

// 6. Link so keyhive membership updates notify the Repo.
hive.linkRepo(repo);
```

## Wrapping additional network adapters

When fanning out to additional peers (e.g., tabs connecting to a service worker), wrap their adapters:

```ts
const keyhiveAdapter = hive.createKeyhiveNetworkAdapter(
  rawNetworkAdapter,          // e.g., MessageChannelNetworkAdapter
  false,                      // onlyShareWithHardcodedServerPeerId
  false,                      // periodicallyRequestKeyhiveSync
  2000,                       // syncRequestInterval (ms)
);

repo.networkSubsystem.addNetworkAdapter(keyhiveAdapter);
```

## Contact cards

A contact card is a user's portable identity. Share it so others can grant you access to documents.

### Copy your contact card

```ts
const json = hive.active.contactCard.toJson();
navigator.clipboard.writeText(json);
```

### Receive a contact card

```ts
import { ContactCard } from "@automerge/automerge-repo-keyhive";

const contactCard = ContactCard.fromJson(pastedJson);
await hive.receiveContactCard(contactCard);
```

## Member management

### Add a member to a document

```ts
import { Access, ContactCard } from "@automerge/automerge-repo-keyhive";

const contactCard = ContactCard.fromJson(memberContactCardJson);
const access = Access.tryFromString("edit"); // "relay" | "read" | "edit" | "admin"

await hive.addMemberToDoc(docUrl, contactCard, access);
```

### Revoke a member

```ts
// hexId is the hex-encoded Identifier bytes of the member to revoke
await hive.revokeMemberFromDoc(docUrl, memberHexId);
```

### Add sync server relay access

Grant the sync server relay-level access so it can sync the document without reading its contents:

```ts
await hive.addSyncServerRelayToDoc(docUrl);
```

## Public access

Public access is implemented as a special "public" member. The same add/revoke flow applies.

### Make a document public

```ts
import { Access } from "@automerge/automerge-repo-keyhive";

const access = Access.tryFromString("edit");
await hive.setPublicAccess(docUrl, access);
```

### Revoke public access

```ts
import { Identifier } from "@automerge/automerge-repo-keyhive";
import { uint8ArrayToHex } from "@automerge/automerge-repo-keyhive";

const publicHexId = uint8ArrayToHex(Identifier.publicId().toBytes());
await hive.revokeMemberFromDoc(docUrl, publicHexId);
```

### Check public access

```ts
const access = await hive.getPublicAccess(docUrl);
// access?.toString() => "Relay", "Read", "Edit", "Admin", or undefined
```

## Querying access

### Check a user's access to a document

```ts
import { docIdFromAutomergeUrl } from "@automerge/automerge-repo-keyhive";

const docId = docIdFromAutomergeUrl(docUrl);
const access = await hive.accessForDoc(identifier, docId);
// access?.toString() => "None" | "Relay" | "Read" | "Edit" | "Admin"
```

### Best access (direct or public, whichever is higher)

```ts
const access = await hive.bestAccessForDoc(
  hive.active.individual.id,  // current user's Identifier
  docUrl
);
```

### List all members and their access

```ts
const docId = docIdFromAutomergeUrl(docUrl);
const memberships = await hive.docMemberCapabilities(docId);
```

## Triggering keyhive sync

After the repo is ready, trigger an initial keyhive sync:

```ts
await repo.networkSubsystem.whenReady();
hive.networkAdapter.syncKeyhive();
```

For the Rust path, `syncKeyhive` is called automatically during initialization.
