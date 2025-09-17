# Keyhive Network Adapter

Contains two `automerge-repo` `NetworkAdapter` implementations:

- `KeyhiveServerAdapter`
- `KeyhiveClientAdapter`

Each of these wraps a lower-level `NetworkAdapter` (e.g. for WebSocket
connections) and uses a Keyhive `Signer` to sign data before sending messages
and verify signatures upon receiving messages.
