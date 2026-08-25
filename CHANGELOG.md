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