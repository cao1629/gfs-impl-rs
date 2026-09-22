# gfs-impl-rs

A Rust implementation of the Google File System as described in Ghemawat, Gobioff and Leung (SOSP 2003): a single master holding all metadata in memory with an operation log and checkpoints, chunkservers storing 64 MB chunks as plain files with per-block checksums, and a client library that talks to both. Leases, chunk versions, the pipelined data push chain, record append, copy-on-write snapshots and lazy garbage collection are all implemented; re-replication, rebalancing, permissions and master replication are deliberately left out.

It shares its wire protocol and on-disk formats with [gfs-impl-cpp](https://github.com/cao1629/gfs-impl-cpp): the two implementations use the same `.proto` files, the same crc32, the same log and checkpoint framing and the same `.meta` layout, so a master, chunkserver or client from either side can stand in for the other.

## Layout

```
proto/            wire protocol (gfs.proto) and on-disk records (master_state.proto)
src/common/       config, framing, path rules, distance function, logging, networking
src/master/       namespace, lock table, operation log, checkpoints, leases, gc
src/chunkserver/  chunk store, data buffer, mutation handling, heartbeat loop
src/client/       the client library
src/bin/          gfs_master, gfs_chunkserver, and the gfs command-line tool
tests/            gRPC-level tests against fakes, and real multi-process cluster tests
scripts/          local_cluster.sh
```

The master, chunkserver and client are all async on tokio; gRPC goes through tonic. Unit tests live next to the code they test, gRPC-level and multi-process tests live under `tests/`.

## Build

Needs a Rust toolchain (edition 2024) and `protoc` on the path (`brew install protobuf` on macOS).

```
cargo build --release
cargo test
```

## Run

```
scripts/local_cluster.sh
target/release/gfs --master_address=127.0.0.1:7000 create /hello
echo "hi" | target/release/gfs --master_address=127.0.0.1:7000 append /hello
target/release/gfs --master_address=127.0.0.1:7000 read /hello
```

Every parameter is a `--key=value` flag; run a binary with a bad flag to see the list. Durations take `ms`, `s`, `m`, `h`, `d`; sizes take `K`, `M`, `G`.
