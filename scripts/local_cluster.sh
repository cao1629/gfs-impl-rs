#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}"
ROOT="${ROOT:-/tmp/gfs-local}"
MASTER=127.0.0.1:7000
COMMON=(--chunk_size=1M --lease_duration=3s --lease_clock_skew_margin=200ms --heartbeat_interval=200ms
        --chunkserver_dead_timeout=1s --deleted_file_retention=2s --gc_interval=500ms)

rm -rf "$ROOT"; mkdir -p "$ROOT"
"$BIN/gfs_master" --listen=$MASTER --data_dir="$ROOT/master" "${COMMON[@]}" &
for i in 1 2 3; do
  "$BIN/gfs_chunkserver" --listen=127.0.0.1:$((7000 + i)) --master_address=$MASTER \
    --data_dir="$ROOT/cs$i" --rack="rack$((i % 2))" "${COMMON[@]}" &
done
echo "master on $MASTER, chunkservers on 7001-7003, data under $ROOT"
echo "try: $BIN/gfs --master_address=$MASTER create /hello"
wait
