# Automerge Repo Keyhive

Utilities for working with [keyhive](https://github.com/inkandswitch/keyhive) with [automerge-repo](https://github.com/automerge/automerge-repo).

## Initializing Keyhive

You can call `initializeKeyhive` to create an `AutomergeRepoKeyhive`, which provides you with a `Keyhive`, a `KeyhiveNetworkAdapter`, and a `KeyhiveEventEmitter`, among other fields.

## Keyhive Network Adapter

`KeyhiveNetworkAdapter` wraps a lower-level `automerge-repo` `NetworkAdapter` (e.g. for WebSocket
connections) and uses a Keyhive `Signer` to sign data before sending messages
and verify signatures upon receiving messages.
