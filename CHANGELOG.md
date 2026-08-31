## [4.0.0] - 2026-08-31 (Upgrades & Performance Optimizations)

### Added

- Consumer group lag Prometheus gauge (`consumer_group_lag`) with labels
  `group_id`, `topic`, `partition`
- Lag updated immediately on every `CommitOffset` RPC and via 15-second
  background ticker
- Per-topic retention policy — `RetentionBytes` and `RetentionMs` fields
  on `TopicConfig`; background goroutine enforces limits every 60 seconds
  on the Raft leader only
- Segment deletion removes both `.log` and `.index` files atomically
- `NewestTimestamp()` on `Segment` for time-based retention decisions
- `EnforceRetention()` on `Log` — active/head segment never deleted
- Batch produce RPC (`ProduceBatch`) — entire batch replicated as one Raft
  operation, single mutex acquisition for all appends
- Batch consume RPC (`ConsumeBatch`) — reads up to `max_count` messages
  in one call, server-side cap of 1000
- `AppendBatch` and `ReadBatch` on `Log`
- `Record` type on `Log` for batch append input
- `BatchRecord` and `BatchCommand` types on Raft FSM
- Benchmarking suite in `internal/bench` and PowerShell runner `scripts/bench.ps1`

### Optimized

- **Zero-Allocation Binary Wire Format:** Replaced reflection-heavy `json.Marshal`/`json.Unmarshal` in Raft FSM apply loop with a custom binary framing protocol (`[type:1B][count:4B][payload...]`)
- **Contiguous Batch Disk I/O:** `Segment.AppendBatch` builds monolithic log and index byte slices in memory, reducing syscall overhead from $2 \times N$ writes to exactly two OS writes per batch
- **$\mathcal{O}(1)$ Index Lookup & Cached Bounds:** Replaced per-read `os.Stat()` disk syscalls with atomic in-memory entry tracking and constant-time index offset arithmetic
- **Sequential Segment Scans:** Optimized `Log.ReadBatch` to look up the segment descriptor once and read contiguous slice records sequentially

### Fixed

- FSM was storing Raft log index as message timestamp — corrected to
  `time.Now().UnixNano()`
- `handleCommitOffset` HTTP handler now updates the lag gauge after commit
  (previously only the gRPC path did)
- Dynamic leader resolution and per-run topic isolation in `test-cluster.ps1`

## [3.0.0] - 2026-08-27 (Phase 3)

### Added

- Multi-partition topics with 3 partitions per topic
- FNV-32a key-based partition selection
- Round-robin partition selection for keyless messages
- Independent commit log per topic partition
- Consumer groups with per-topic-partition committed offsets
- Persistent consumer-group offset storage with BoltDB
- CommitOffset and FetchOffset gRPC APIs
- HTTP consumer-group offset endpoints
- Consumer-group CLI with continuous follow mode
- Mutual TLS (mTLS) for gRPC
- TLS-protected Raft transport
- Certificate generation script using OpenSSL
- Local Docker TLS configuration for all brokers
- Automated cluster integration test script
- Certificate and runtime artifact exclusions in `.gitignore`
- PowerShell cluster startup script (`start.ps1`) — checks Docker,
  generates certs if missing, brings up Docker Compose, polls broker health
- PowerShell certificate generation script (`scripts/gen-certs.ps1`) —
  generates self-signed CA and per-broker mTLS certs using OpenSSL
- Graceful broker shutdown with Raft step-down

### Removed

- `scripts/gen-certs.sh` — replaced by `scripts/gen-certs.ps1`

## [2.0.0] - 2026-08-25 (Phase 2)

### Added

- Docker Compose: 3-broker cluster in isolated containers
- Real TCP Raft transport (replacing in-memory)
- gRPC + protobuf API alongside HTTP
- Prometheus metrics endpoint (/metrics) on all brokers
- Grafana dashboard with produce/consume rate and error panels

## [1.0.0] - 2026-08-20 (Phase 1)

### Added

- 3-node Raft cluster with in-memory transport
- Append-only commit log with binary index
- HTTP API and producer/consumer CLI
- Full test suite