# Keyhive Network Adapter

Provides a `NetworkAdapter` implementation called `KeyhiveNetworkAdapter`. This wraps a lower-level `NetworkAdapter` (e.g. for WebSocket
connections) and uses a Keyhive `Signer` to sign data before sending messages
and verify signatures upon receiving messages.
