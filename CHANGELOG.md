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
- PowerShell cluster startup script (`start.ps1`) — checks Docker, generates certs if missing, brings up Docker Compose, polls broker health
- PowerShell certificate generation script (`scripts/gen-certs.ps1`) — generates self-signed CA and per-broker mTLS certs using OpenSSL
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