# Distributed Event Log — Upgrade Log

## Priority Legend
🔴 P1 — Fixes real gaps, should ship next  
🟡 P2 — Meaningful capability jump  
🟢 P3 — Performance / polish

---

## 🔴 P1-A · Consumer Lag Metrics

**What:** Expose a `consumer_group_lag` Prometheus gauge — (latest offset − committed offset) per group per partition. Visible in Grafana immediately.

**Why first:** Logs grow forever and you can't tell if consumers are keeping up. This is the most obvious missing observable.

**ChatGPT prompt:**
```
I have a distributed event log in Go with this stack:
- hashicorp/raft 3-node cluster
- Custom append-only commit log: .log + .index file pairs, BigEndian binary encoding
- Message format: offset(8B) | timestamp(8B) | key_size(4B) | value_size(4B) | key | value
- gRPC + protobuf for produce/consume
- BoltDB per broker for consumer group offsets (FetchOffset/CommitOffset RPCs exist)
- Prometheus already wired up

Task: Add a consumer_group_lag Prometheus gauge.
- Gauge labels: group_id, topic, partition
- Value: latestOffset (last written offset in partition) − committedOffset (from BoltDB for that group)
- Update the gauge on every CommitOffset RPC call AND on a background ticker every 15s
- Register the gauge in the existing Prometheus registry (do not create a new one)
- Show only the changed files with full file content

Go 1.27, Windows, Docker Desktop environment.
```

**Audit focus:** gauge registration doesn't collide with existing metrics, BoltDB read in background goroutine is safe (BoltDB is not concurrent-write safe — read-only tx is fine), ticker is stopped on shutdown via context.

---

## 🔴 P1-B · Topic Retention Policy (size + time)

**What:** Per-topic config for max retention size (bytes) and max retention time (hours). Background goroutine deletes oldest segments that breach either limit.

**Why:** Logs currently grow unbounded. On a laptop running Docker this will fill disk.

**ChatGPT prompt:**
```
I have a distributed event log in Go with this stack:
- Custom append-only commit log with rolling segments: .log + .index file pairs
- Segment rolling triggers at maxSegmentSize (configurable)
- Segments are named by base offset
- BigEndian binary encoding, message format: offset(8B) | timestamp(8B) | key_size(4B) | value_size(4B) | key | value
- hashicorp/raft 3-node cluster; only the Raft leader should delete segments

Task: Add per-topic retention enforcement.
- Add two optional fields to topic config: RetentionBytes int64, RetentionMs int64
- A background goroutine per topic runs every 60s
- It deletes the oldest CLOSED segments (not the active/head segment) that push total size over RetentionBytes OR whose newest message timestamp is older than RetentionMs
- Deleting a segment means removing both the .log and .index files
- Only the Raft leader node runs the deletion goroutine (check raft.State() == raft.Leader)
- Show only changed files with full file content

Go 1.27, Windows, Docker Desktop.
```

**Audit focus:** active segment is never deleted (must guard on `segment == activeSegment`), both .log and .index deleted atomically (or at least .log first then .index — partial delete is recoverable), leader check is re-evaluated each tick (not cached), goroutine exits cleanly on shutdown.

---

## 🟡 P2-A · Batch Produce / Consume

**What:** Single gRPC RPC sends/receives N messages. Current design is one message per RPC — huge overhead.

**ChatGPT prompt:**
```
I have a distributed event log in Go with:
- gRPC + protobuf for produce/consume
- Current ProduceRequest: { topic, partition, key bytes, value bytes }
- Current ConsumeRequest: { topic, partition, offset int64 }
- Custom commit log append: writes one message at a time

Task: Add batch produce and batch consume RPCs alongside the existing single-message ones (do not remove them).
- ProduceBatchRequest: { topic, partition, repeated Record { key bytes, value bytes } }
- ProduceBatchResponse: { repeated int64 offsets, error string }
- ConsumeBatchRequest: { topic, partition, offset int64, max_count int32 }
- ConsumeBatchResponse: { repeated ConsumedRecord { offset int64, key bytes, value bytes, timestamp int64 } }
- Append records to the commit log in a single loop under one mutex lock acquisition
- Update the .proto file, regenerate, implement handler in broker

Show updated .proto file and changed Go files with full content.
Go 1.27, Windows, Docker Desktop, protoc available.
```

**Audit focus:** single mutex hold for the full batch (not per-message re-lock), proto field numbers don't conflict with existing messages, consume batch respects segment boundaries (reads across segment roll correctly), max_count is bounded server-side (cap at e.g. 1000).

---

## 🟡 P2-B · Admin REST API

**What:** Read-only HTTP endpoints for cluster inspection. No auth needed (it's local), but useful for debugging and the README.

**Endpoints:**
- `GET /admin/topics` — list topics, partition count, latest offset per partition
- `GET /admin/groups` — list consumer groups, committed offset per group/partition, lag
- `GET /admin/cluster` — Raft state, leader address, peer list

**ChatGPT prompt:**
```
I have a distributed event log in Go with:
- hashicorp/raft 3-node cluster
- Custom commit log with partitions per topic
- BoltDB for consumer group offsets
- Existing HTTP server (the join/health endpoints are already on it)

Task: Add read-only admin endpoints to the existing HTTP mux (do not start a new server).
GET /admin/topics  — JSON: [ { topic, partitions: [ { id, latestOffset } ] } ]
GET /admin/groups  — JSON: [ { groupId, offsets: [ { topic, partition, committed, lag } ] } ]
GET /admin/cluster — JSON: { state, leader, peers: [addr] }

- All handlers are read-only, no locks needed beyond what BoltDB and raft.Stats() already provide
- lag = latestOffset − committed (reuse the lag calculation from consumer group lag metrics if added)
- Return application/json, status 200, pretty-printed

Show only changed files with full content. Go 1.27.
```

**Audit focus:** handlers registered on existing mux (not a new `http.DefaultServeMux`), BoltDB access is read-only tx, raft.Stats() is safe to call from any goroutine, JSON marshalling handles empty slices as `[]` not `null`.

---

## 🟡 P2-C · Topic Compaction (log compaction per key)

**What:** For compacted topics, only the latest value per key is kept. Background compactor rewrites segments, dropping superseded entries.

**ChatGPT prompt:**
```
I have a distributed event log in Go with:
- Custom append-only commit log: .log + .index file pairs, rolling segments
- Message format: offset(8B) | timestamp(8B) | key_size(4B) | value_size(4B) | key | value
- BigEndian binary encoding throughout
- Per-topic config struct already exists

Task: Add optional log compaction for topics where Compacted: true in config.
- A background goroutine runs compaction every 5 minutes on closed segments only
- Build a map[string]int64 of key → latestOffset by scanning all closed segments newest-first
- Rewrite each closed segment, skipping any record whose offset is NOT the latest for that key
- Write the rewritten segment to a temp file, then atomically rename over the original
- Rebuild the .index file for the rewritten segment after rename
- Only the Raft leader runs compaction (check raft.State() == raft.Leader each cycle)
- Active/head segment is never compacted

Show only changed files with full content. Go 1.27, Windows (atomic rename works on Windows with os.Rename if temp file is same drive).
```

**Audit focus:** temp file is on the same drive/volume as the segment (os.Rename is atomic only same-volume on Windows), index rebuild uses the same BigEndian format as existing index writer, active segment guard is solid, key with empty value (tombstone) is preserved (not deleted) during compaction.

---

## 🟢 P3-A · Snappy Compression

**What:** Optional per-topic compression. Segment .log files written with Snappy framing. Transparent on read.

**ChatGPT prompt:**
```
I have a distributed event log in Go with:
- Custom append-only commit log writing raw bytes to .log files
- Message format: offset(8B) | timestamp(8B) | key_size(4B) | value_size(4B) | key | value
- BigEndian encoding

Task: Add optional Snappy compression per topic (Compressed: true in topic config).
- Use github.com/golang/snappy (snappy.Encode / snappy.Decode)
- When Compressed: true, compress only the key+value payload bytes before writing; the fixed header (offset, timestamp, key_size, value_size) stays uncompressed so the index still works
- On read, check topic config and decompress payload if Compressed: true
- Existing uncompressed topics/segments are unaffected
- Add a Prometheus counter: compressed_bytes_saved_total per topic

Show only changed files with full content. Go 1.27.
```

**Audit focus:** key_size and value_size in the header reflect the COMPRESSED sizes (so reads use the right byte counts), decompression error is returned not silently swallowed, existing segments without compression are still readable (no flag byte added to format — config drives it, not a per-record flag).

---

## 🟢 P3-B · Benchmark Suite

**What:** `go test -bench` suite + `ghz` gRPC load numbers. For the README and GitHub credibility.

**ChatGPT prompt:**
```
I have a distributed event log in Go with gRPC produce/consume RPCs and a custom commit log.

Task: Write a Go benchmark file internal/bench/bench_test.go with:
- BenchmarkProduceSingle: produces 1 message per op to an in-process broker (no network)
- BenchmarkProduceBatch100: produces a batch of 100 messages per op
- BenchmarkConsumeSingle: consumes 1 message per op sequentially
- BenchmarkConsumeSequential1000: consumes 1000 messages in one op from a pre-seeded log
- Use testing.B, each benchmark calls b.ResetTimer() after setup
- The in-process broker should use the real commit log on a temp dir (os.MkdirTemp), cleaned up with b.Cleanup()
- Also write a scripts/bench.ps1 that: runs go test -bench=. -benchtime=10s -benchmem ./internal/bench/..., then runs ghz against the running cluster at localhost:50051 for ProduceSingle (100k RPC calls, 10 concurrent)

Show bench_test.go and scripts/bench.ps1 in full. Go 1.27, Windows.
```

**Audit focus:** temp dirs are cleaned up (b.Cleanup not defer, so it runs even on benchmark failure), ghz call uses correct proto descriptor or reflection (cluster must have gRPC reflection enabled — flag this if not), benchmarks are independent (no shared state between Benchmark functions).

---

## Upgrade Order

| # | ID | Name | Priority |
|---|-----|------|----------|
| 1 | P1-A | Consumer Lag Metrics | 🔴 |
| 2 | P1-B | Topic Retention Policy | 🔴 |
| 3 | P2-A | Batch Produce/Consume | 🟡 |
| 4 | P2-B | Admin REST API | 🟡 |
| 5 | P2-C | Topic Compaction | 🟡 |
| 6 | P3-A | Snappy Compression | 🟢 |
| 7 | P3-B | Benchmark Suite | 🟢 |

---
