# Benchmarks

Vitest benchmarks for automerge-repo-keyhive event processing, sync cycles, and encoding.

## Setup

Benchmarks require extracted data from a synced peer's keyhive storage. The data is
gitignored and must be generated locally.

1. Have a peer that has synced with a keyhive sync server with sufficient ops for testing.

2. Extract benchmark data:
   ```
   pnpm bench:extract --peer-data-dir <path-to-peer-keyhive-storage>
   ```

   This creates `bench/data/` with `archive.bin`, `events.bin`, `hashes.bin`, and `keypair.json`.

3. Run benchmarks:
   ```
   pnpm bench
   ```
