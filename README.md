# distributed-event-log

![Go](https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat&logo=go) ![License](https://img.shields.io/badge/license-MIT-green?style=flat) ![Version](https://img.shields.io/badge/release-v4.0.0-brightgreen?style=flat)

Kafka is a marvel of distributed systems engineering. So I built one from scratch to understand why.

This is a fault-tolerant, high-throughput distributed event log written in Go - not a tutorial project, not a wrapper around someone else's library. The storage engine, binary wire format, consensus layer, contiguous batch I/O, partitioning, consumer groups, retention policies, transport security, and observability stack are all custom-built.

### What’s Included

- **Fault tolerance:** 3-broker Raft cluster with leader-based writes and failover testing
- **Storage:** Append-only segmented logs with binary indexes, batching, and retention
- **Topics:** Multi-partition topics with keyed hashing and round-robin selection
- **Consumer groups:** Persistent offsets and real-time lag metrics
- **Networking:** Protobuf/gRPC APIs with mTLS
- **Consensus security:** TLS-protected Raft transport
- **Observability:** Prometheus metrics and a provisioned Grafana dashboard


---

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                         │
│                                                              │
│  Producer CLI / Consumer CLI / Consumer Group CLI            │
└────────────────────────────┬─────────────────────────────────┘
                             │
                    gRPC + mTLS / HTTP
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                       BROKER CLUSTER                         │
│                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  │
│  │    Broker 1    │  │    Broker 2    │  │    Broker 3    │  │
│  │    (Leader)    │  │   (Follower)   │  │   (Follower)   │  │
│  │                │  │                │  │                │  │
│  │ HTTP + gRPC    │  │ HTTP + gRPC    │  │ HTTP + gRPC    │  │
│  │ TLS / mTLS     │  │ TLS / mTLS     │  │ TLS / mTLS     │  │
│  └────────┬───────┘  └────────┬───────┘  └────────┬───────┘  │
│           └───────────────────┼───────────────────┘          │
│                               │                              │
│                Raft Consensus (Binary FSM)                   │
│                        TLS protected                         │
└───────────────────────────────┬──────────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                       TOPIC / PARTITION LAYER                      │
│                                                                    │
│  Topic A                         Topic B                           │
│  ┌─────────┬─────────┬─────────┐ ┌─────────┬─────────┬──────────┐  │
│  │Partition│Partition│Partition│ │Partition│Partition│ Partition│  │
│  │    0    │    1    │    2    │ │    0    │    1    │     2    │  │
│  └────┬────┴────┬────┴────┬────┘ └────┬────┴─────┬───┴─────┬────┘  │
│       │         │         │           │          │         │       │
│       ▼         ▼         ▼           ▼          ▼         ▼       │
│    log+idx    log+idx   log+idx      log+idx   log+idx  log+idx    │
└────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                        │
│                                                              │
│  Append-only .log segments with contiguous batch I/O         │
│  Binary .index files with O(1) position lookup               │
│  Configurable segment retention (time & byte policies)       │
│  Consumer-group offsets & lag calculation                    │
│  └── consumer-groups.db                                      │
└──────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY LAYER                     │
│                                                              │
│  Prometheus → /metrics (Throughput, Errors, Group Lag)       │
│  Grafana → provisioned dashboard                             │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

* Docker Desktop
* PowerShell 7+
* OpenSSL on PATH (included with Git for Windows)

```powershell
.\start.ps1          # generate certs if needed, then bring up the cluster
.\test-cluster.ps1   # run the 7-stage integration test
```

`start.ps1` is idempotent — it is safe to re-run if the cluster is already up.

### README Guide

1. **Architecture** — System layout and component boundaries
2. **Storage Engine** — Commit log, wire formats, batching, indexing, and retention
3. **Consensus** — Raft replication and binary FSM framing
4. **Topics & Partitions** — Partition selection and on-disk layout
5. **Consumer Groups** — Offset persistence and lag monitoring
6. **APIs & Security** — gRPC, mTLS, and Raft TLS
7. **Observability** — Prometheus and Grafana
8. **CLI & Testing** — Produce, consume, integration tests, and benchmarks
9. **Project Structure** — Repository layout
10. **Release History** — Phase-by-phase progression


---

## Commit Log & Storage Engine

The commit log is the high-performance storage engine at the heart of the system.

Every message is appended sequentially to a `.log` file. A companion `.index` file maps each message offset to the exact byte position where that message begins, allowing constant-time and binary-search reads rather than scanning the entire log.

### Message Wire Format

```text
| offset (8B) | timestamp (8B) | key_size (4B) | value_size (4B) | key | value |
```

### Index Wire Format

```text
| offset (8B) | position (8B) |
```

Serialization uses `encoding/binary` with BigEndian byte order.

### Performance & Retention

* **Contiguous Batch Appends (`AppendBatch`):** Serializes an entire batch of messages in-memory before issuing writes, cutting OS disk system calls from `2 × N` to exactly two writes per batch.
* **`O(1)` Index Lookups:** In-memory entry counters eliminate `os.Stat()` calls on every read.
* **Segment Rolling & Retention:** Segments roll over when reaching the maximum configured size. Background workers enforce `RetentionBytes` and `RetentionMs` by atomically unlinking old `.log` and `.index` file pairs while keeping the active write head intact.

---

## Raft Consensus & State Machine

Writes are replicated through Raft before being committed to local partition logs.

The broker cluster uses:

* `hashicorp/raft`
* Persistent Raft log and stable stores using BoltDB
* Real TCP transport between isolated broker containers
* TLS-protected Raft streams
* Leader-based write routing with dynamic cluster joining

### Binary Wire Framing

The Raft FSM uses a custom binary protocol (`[type:1B][count:4B][payload...]`) instead of reflection-based JSON, eliminating garbage collection latency spikes during high throughput.

```text
Raft Command (Binary Frame)
            │
            ▼
        FSM Apply
            │
    ┌───────┴───────┐
    ▼               ▼
Single Append   Batch Append
    │               │
    └───────┬───────┘
            ▼
    Partition Commit Log

```

---

## Multi-Partition Topics

Each topic contains three independent partitions by default.

Partition selection works as follows:

```text
Message has key?
      │
   ┌──┴───┐
  yes     no
   │      │
   ▼      ▼
FNV-32a  round-robin
   │      │
   └──┬───┘
      ▼
partition 0 / 1 / 2
```

Every partition is its own isolated `log.Log` instance.

Partition data is organized on disk as:

```text
<dataDir>/
└── topics/
    └── <topic>/
        └── partitions/
            ├── 0/
            ├── 1/
            └── 2/
```

Topics are created automatically when first referenced.

---

## Consumer Groups & Lag Monitoring

Consumer groups track partition offsets independently:

* **Persistent Storage:** Offsets persist in `<dataDir>/consumer-groups.db` via BoltDB backed by an in-memory RWMutex cache.
* **Real-time Lag Calculation:** The broker updates Prometheus `consumer_group_lag` gauges on every commit and via background sweeps ($L = \text{Latest Log Offset} - \text{Committed Offset}$).

```text
Fetch committed offset
        │
        ▼
Read message at offset
        │
        ▼
Process message
        │
        ▼
Commit offset + 1
```

---

## gRPC & Batch API

Every broker exposes a typed gRPC API defined via Protobuf:

```protobuf
service BrokerService {
  rpc Produce (ProduceRequest) returns (ProduceResponse);
  rpc Consume (ConsumeRequest) returns (ConsumeResponse);
  rpc ProduceBatch (ProduceBatchRequest) returns (ProduceBatchResponse);
  rpc ConsumeBatch (ConsumeBatchRequest) returns (ConsumeBatchResponse);
  rpc CommitOffset (CommitOffsetRequest) returns (CommitOffsetResponse);
  rpc FetchOffset (FetchOffsetRequest) returns (FetchOffsetResponse);
}

```

Generated stubs live under:

```text
internal/proto/
```

---

## Security

* **gRPC mTLS:** The gRPC server enforces mutual TLS authentication using certificates signed by the root CA.
* **Raft TLS:** Consensus traffic is encrypted via a custom `raft.StreamLayer` with mutual certificate verification.
* **Automated PKI:** Generated automatically using `scripts/gen-certs.ps1` into `certs/` (gitignored).

---

## Observability

Prometheus scrapes `/metrics` across all brokers:

```text
event_log_messages_produced_total
event_log_messages_consumed_total
event_log_produce_errors_total
event_log_consume_errors_total
consumer_group_lag
```

* Prometheus: `http://localhost:9090`
* Grafana: `http://localhost:3000` (Default: `admin` / `admin`)

---

## CLI Usage

### Produce a Message
```powershell
go run ./cmd/cli produce `
  --addr localhost:9081 `
  --topic orders `
  --msg "hello world" `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

### Consume a Message
```powershell
go run ./cmd/cli consume `
  --addr localhost:9081 `
  --topic orders `
  --partition 0 `
  --offset 0 `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

### Consume with a Consumer Group
```powershell
go run ./cmd/cli consume-group `
  --addr localhost:9081 `
  --group orders-consumers `
  --topic orders `
  --partition 0 `
  --follow `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

---

## Testing & Benchmarks

### Cluster Integration Suite
```powershell
.\test-cluster.ps1
```

### Storage Engine Benchmarks
```powershell
.\scripts\bench.ps1

```

### Live Cluster Stress Test

```powershell
ghz `
  --cacert=".\certs\ca.crt" `
  --cert=".\certs\broker-1.crt" `
  --key=".\certs\broker-1.key" `
  --proto=".\internal\proto\broker.proto" `
  --call=broker.BrokerService.Produce `
  -d '{"topic":"stress-test","key":"c3RyZXNz","value":"c3RyZXNzLXBheWxvYWQtZGF0YQ=="}' `
  -n 50000 `
  -c 20 `
  localhost:9081

```

---

## Project Structure

```text
distributed-event-log/
│
├── cmd/
│   ├── broker/
│   │   └── main.go              # Broker entry point
│   └── cli/
│       └── main.go              # Producer/consumer CLI
│
├── internal/
│   ├── log/
│   │   ├── segment.go           # Contiguous batch I/O & index caching
│   │   └── log.go               # Multi-segment commit log & retention
│   │
│   ├── raft/
│   │   └── fsm.go               # Binary-framed Raft FSM
│   │
│   ├── broker/
│   │   ├── broker.go            # Broker, Raft, HTTP, gRPC
│   │   └── consumer_group.go    # Consumer-group offset store & lag
│   │
│   ├── bench/
│   │   └── log_bench_test.go    # Storage engine micro-benchmarks
│   │
│   ├── proto/
│   │   ├── broker.proto         # Protobuf definitions
│   │   ├── broker.pb.go         # Generated protobuf stubs
│   │   └── broker_grpc.pb.go    # Generated gRPC code
│   │
│   └── tls/
│       └── tls.go               # TLS/mTLS configuration helpers
│
├── scripts/
│   ├── gen-certs.ps1            # Local TLS certificate generation
│   └── bench.ps1                # Benchmark runner script
│
├── grafana/
│   ├── dashboard.json
│   ├── dashboards.yml
│   └── datasource.yml
│
├── certs/                       # Local mTLS certs (gitignored)
├── data/                        # Runtime broker volumes (gitignored)
│
├── docker-compose.yml
├── Dockerfile
├── prometheus.yml
├── start.ps1                    # Cluster startup & health check
├── test-cluster.ps1             # 7-stage failover integration suite
├── go.mod
├── go.sum
└── CHANGELOG.md

```

---

## Release History

| Release | Status | Description |
| --- | --- | --- |
| **v1.0.0 (Phase 1)** | ✅ Complete | Single-process Raft cluster, append-only log with binary index, CLI |
| **v2.0.0 (Phase 2)** | ✅ Complete | Multi-container Docker deployment, TCP Raft transport, gRPC API, Prometheus/Grafana |
| **v3.0.0 (Phase 3)** | ✅ Complete | Multi-partition topics, BoltDB consumer groups, gRPC mTLS, TLS-Raft, failover harness |
| **v4.0.0 (Core Engine)** | ✅ Complete | Zero-allocation binary Raft FSM, contiguous batch I/O, `O(1)` index caching, retention limits, lag metrics |

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.