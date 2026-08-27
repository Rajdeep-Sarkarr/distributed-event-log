# distributed-event-log

![Go](https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat\&logo=go) ![License](https://img.shields.io/badge/license-MIT-green?style=flat) ![Phase](https://img.shields.io/badge/phase-3%20complete-brightgreen?style=flat)

Kafka is a marvel of distributed systems engineering. So I built one from scratch to understand why.

This is a fault-tolerant, distributed event log written in Go - not a tutorial project, not a wrapper around someone else's library. The storage engine, binary wire format, cluster coordination, partitioning, consumer groups, transport security, and observability stack are all deliberately built as part of the system. Because reading about append-only logs, Raft consensus, and distributed messaging is one thing. Writing the code that makes them work is another thing entirely.

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
│                       Raft Consensus                         │
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
│  │    0    │    1    │    2    │ │    0    │    1    │     2    │  |
│  └────┬────┴────┬────┴────┬────┘ └────┬────┴─────┬───┴─────┬────┘  │
│       │         │         │           │          │         │       │
│       ▼         ▼         ▼           ▼          ▼         ▼       │
│    log+idx    log+idx   log+idx      log+idx   log+idx  log+idx    |
└────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                        │
│                                                              │
│  Append-only .log segments                                   │
│  Binary .index files                                         │
│  Offset → byte-position lookup                               │
│  Segment rolling                                             │
│                                                              │
│  Consumer-group offsets                                      │
│  └── consumer-groups.db                                      │
└──────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY LAYER                     │
│                                                              │
│  Prometheus → /metrics                                       │
│  Grafana → provisioned dashboard                             │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

**Prerequisites:**

* Docker Desktop
* PowerShell 7+
* OpenSSL on PATH (included with Git for Windows)

```powershell
.\start.ps1          # generate certs if needed, then bring up the cluster
.\test-cluster.ps1   # run the 7-stage integration test
```

`start.ps1` is idempotent — it is safe to re-run if the cluster is already up.

---

## Commit Log

The commit log is the storage engine at the heart of the system.

Every message is appended sequentially to a `.log` file. A companion `.index` file maps each message offset to the exact byte position where that message begins, allowing reads through binary search rather than scanning the entire log.

Message wire format:

```text
| offset (8B) | timestamp (8B) | key_size (4B) | value_size (4B) | key | value |
```

Index format:

```text
| offset (8B) | position (8B) |
```

Serialization uses `encoding/binary` with BigEndian encoding.

When a segment reaches the configured maximum size, the log rolls over to a new segment. Existing segments are discovered and recovered from disk on restart.

The commit-log implementation is intentionally independent of the broker and Raft layers.

## Raft Consensus

Writes are replicated through Raft before they are applied to the local commit log.

The broker cluster uses:

* `hashicorp/raft`
* persistent Raft log and stable stores using BoltDB
* TCP transport between broker containers
* TLS-protected Raft streams in Phase 3
* leader-based writes
* dynamic broker joining

A message is encoded as a Raft command and applied by the FSM on every participating broker.

The FSM routes the command to the correct topic partition:

```text
Raft Command
     │
     ▼
  FSM Apply
     │
     ├── Topic
     ├── Partition
     ├── Key
     └── Value
     │
     ▼
Partition Log
```

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

Every partition is its own `log.Log` instance.

Partition data is stored under:

```text
<dataDir>/
└── topics/
    └── <topic>/
        └── partitions/
            ├── 0/
            ├── 1/
            └── 2/
```

Topics are automatically created when they are first used.

If no topic is supplied, the broker uses the default topic:

```text
default
```

## Consumer Groups

Phase 3 adds consumer-group offset management.

Each consumer group is identified by a group ID and tracks committed offsets independently for each topic-partition.

Offsets are stored locally in:

```text
<dataDir>/consumer-groups.db
```

The implementation uses BoltDB with a `consumer-groups` bucket and maintains an in-memory cache protected by `sync.RWMutex`.

Consumer offset commits intentionally do **not** go through Raft in this phase. Each broker maintains its own consumer-group state.

The consumer-group flow is:

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

## gRPC API

Every broker exposes a typed gRPC API defined through protobuf.

Current RPCs:

```text
Produce
Consume
CommitOffset
FetchOffset
```

The API supports:

* topics
* partitions
* message keys and values
* consumer-group offsets

Generated protobuf stubs live under:

```text
internal/proto/
```

## Security

Phase 3 adds transport security with mutual TLS.

### gRPC

The broker's gRPC server uses mTLS when certificate configuration is provided.

Clients present a certificate and verify the broker certificate against the project CA.

### Raft

Raft peer-to-peer communication is protected with TLS through a custom `raft.StreamLayer`.

Both sides authenticate with certificates signed by the same CA.

Certificate generation is handled by:

```text
scripts/gen-certs.ps1
```

Generated certificates are stored locally under:

```text
certs/
```

and are intentionally excluded from Git.

For local Docker Compose deployment, the broker certificates contain SANs for:

```text
broker-1
broker-2
broker-3
localhost
127.0.0.1
```

## Observability

Prometheus scrapes `/metrics` from all three brokers.

Metrics include:

```text
event_log_messages_produced_total
event_log_messages_consumed_total
event_log_produce_errors_total
event_log_consume_errors_total
```

Grafana is provisioned automatically with a dashboard containing rate panels for production, consumption, and errors.

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Default Grafana credentials:

```text
admin / admin
```

## Project Structure

```text
distributed-event-log/
│
├── cmd/
│   ├── broker/
│   │   └── main.go              # Broker process entry point
│   └── cli/
│       └── main.go              # Producer/consumer CLI
│
├── internal/
│   ├── log/
│   │   ├── segment.go           # Single .log + .index segment
│   │   └── log.go               # Multi-segment commit log
│   │
│   ├── raft/
│   │   └── fsm.go               # Raft FSM
│   │
│   ├── broker/
│   │   ├── broker.go            # Broker, Raft, HTTP, gRPC
│   │   └── consumer_group.go    # Consumer-group offset store
│   │
│   ├── proto/
│   │   ├── broker.proto         # gRPC schema
│   │   ├── broker.pb.go         # Generated protobuf code
│   │   └── broker_grpc.pb.go    # Generated gRPC code
│   │
│   └── tls/
│       └── tls.go               # TLS configuration helpers
│
├── scripts/
│   └── gen-certs.ps1            # Local TLS certificate generation
│
├── grafana/
│   ├── dashboard.json
│   ├── dashboards.yml
│   └── datasource.yml
│
├── certs/                       # Local only, gitignored
├── data/                        # Runtime data, gitignored
│
├── docker-compose.yml
├── Dockerfile
├── prometheus.yml
├── start.ps1                    # Cluster startup script
├── test-cluster.ps1             # Cluster integration test
├── go.mod
├── go.sum
└── CHANGELOG.md
```

---

## Getting Started

### Requirements

* Go 1.27+
* Docker Desktop
* Docker Compose
* PowerShell 7+
* OpenSSL on PATH

### Generate local TLS certificates manually

The normal workflow is to use `start.ps1`, which generates certificates automatically when `certs\ca.crt` is missing.

To generate them manually:

```powershell
.\scripts\gen-certs.ps1
```

This creates the local CA and broker certificates under:

```text
certs/
```

### Start the full cluster

```powershell
.\start.ps1
```

This:

1. Checks that Docker Desktop is running.
2. Generates TLS certificates if they are missing.
3. Starts the Docker Compose cluster.
4. Polls the three broker `/metrics` endpoints.
5. Prints `Cluster ready.` when all three brokers are healthy.

### Produce a message

Using the TLS-enabled CLI:

```powershell
go run ./cmd/cli produce `
  --addr localhost:9081 `
  --topic orders `
  --msg "hello world" `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

The command returns the partition-local offset.

You can explicitly select a partition:

```powershell
go run ./cmd/cli produce `
  --addr localhost:9081 `
  --topic orders `
  --partition 1 `
  --msg "hello partition 1" `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

### Consume a message

```powershell
go run ./cmd/cli consume `
  --addr localhost:9081 `
  --topic orders `
  --partition 1 `
  --offset 0 `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

### Consume with a consumer group

```powershell
go run ./cmd/cli consume-group `
  --addr localhost:9081 `
  --group orders-consumers `
  --topic orders `
  --partition 1 `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

Use `--follow` for continuous polling:

```powershell
go run ./cmd/cli consume-group `
  --addr localhost:9081 `
  --group orders-consumers `
  --topic orders `
  --partition 1 `
  --follow `
  --cert certs/broker-1.crt `
  --key certs/broker-1.key `
  --ca certs/ca.crt
```

### Prometheus

```text
http://localhost:9090
```

Targets:

```text
http://localhost:9090/targets
```

### Grafana

```text
http://localhost:3000
```

Login:

```text
admin / admin
```

### Run the integration test

With the Docker Compose stack running:

```powershell
.\test-cluster.ps1
```

The integration test verifies:

* Docker broker startup
* Raft leader election
* gRPC mTLS
* Produce
* partitioned Consume
* Prometheus metrics
* consumer-group commit/fetch
* leader failure
* Raft re-election
* producing after failover
* failed broker restart

### Run Go tests

```powershell
go test ./...
```

### Stop the stack

```powershell
docker compose down
```

To remove persisted Docker volumes as well:

```powershell
docker compose down -v
```

---

## Phases

| Phase        | Status     | Description                                                                                |
| ------------ | ---------- | ------------------------------------------------------------------------------------------ |
| **Phase 1**  | ✅ Complete | 3-node Raft cluster in one process, in-memory transport, disk commit log, HTTP API and CLI |
| **Phase 2**  | ✅ Complete | Docker containers, real TCP Raft transport, gRPC + protobuf, Prometheus + Grafana          |
| **Phase 3A** | ✅ Complete | Multi-partition topics, FNV-32a key hashing, round-robin routing                           |
| **Phase 3B** | ✅ Complete | Consumer groups and persistent local offsets with BoltDB                                   |
| **Phase 3D** | ✅ Complete | mTLS for gRPC, TLS-protected Raft transport, certificate tooling and integration testing   |

---

## Key Design Decisions

### Why build the commit log from scratch?

Because that's where many of the interesting storage problems live.

The implementation owns:

* byte offsets
* segment rolling
* binary encoding
* index lookups
* on-disk recovery

Using an existing embedded database for the commit log would hide exactly the storage behavior this project is intended to explore.

### Why use hashicorp/raft instead of implementing Raft from scratch?

Raft appears deceptively simple on paper and is notoriously difficult to implement correctly.

This project focuses on the engineering around consensus:

* the FSM
* persistent state
* bootstrap and membership
* leader routing
* replicated partition writes
* broker lifecycle

`hashicorp/raft` provides the consensus implementation while leaving those integration boundaries under direct project control.

### Why use independent logs per partition?

A partition is the unit of ordered storage.

Giving each partition its own `log.Log` keeps:

* offsets independent
* segment rolling independent
* storage isolated
* topic partitioning explicit

This provides a clean foundation for future partition-aware consumer behavior.

### Why are consumer-group offsets local?

This is intentionally simplified for Phase 3.

Message data is replicated through Raft, while consumer-group offsets are maintained locally on each broker. This separation keeps the consumer-group implementation straightforward without introducing another replicated state machine for offsets.

### Why use mTLS?

The project has two security-sensitive network paths:

```text
Client → gRPC → Broker
Broker → Raft → Broker
```

Both require identity verification and encrypted transport.

mTLS allows every participating endpoint to authenticate against a shared project CA while keeping private keys local to each node.

### Why Go?

Goroutines map naturally to concurrent broker components. The standard library provides strong primitives for HTTP, binary encoding, cryptography, networking, and file I/O.

Go also makes the lower-level mechanics of the system visible rather than abstracting them away.

---

## References

* [The Log: What every software engineer should know about real-time data](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) - Jay Kreps, LinkedIn
* [Raft Consensus Algorithm](https://raft.github.io/) - Diego Ongaro
* [hashicorp/raft](https://github.com/hashicorp/raft) - Raft implementation
* [Apache Kafka Design Docs](https://kafka.apache.org/documentation/#design) - Storage and distributed-log reference

---

## Status

**Phase 3 complete.**

The project currently provides a networked, containerized distributed event log with:

* persistent partitioned storage
* Raft replication
* consumer groups
* gRPC + HTTP APIs
* mTLS
* Prometheus metrics
* Grafana dashboards
* Docker Compose deployment
* automated cluster integration testing

The next stage is focused on deeper reliability, failure testing, and further Kafka-style distributed behavior.
