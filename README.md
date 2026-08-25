# distributed-event-log

![Go](https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/license-MIT-green?style=flat)
![Phase](https://img.shields.io/badge/phase-2%20complete-brightgreen?style=flat)

Kafka is a marvel of distributed systems engineering. So I built one from scratch to understand why.

This is a fault-tolerant, distributed event log written in Go - not a tutorial project, not a wrapper around someone else's library. The storage engine, the binary wire format, the cluster coordination, the observability stack - all of it built from first principles. Because reading about append-only logs and Raft consensus is one thing. Writing the code that makes them work is another thing entirely.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    CLIENT LAYER                     │
│         Producer (CLI)       Consumer (CLI)         │
└────────────────┬─────────────────┬──────────────────┘
                 │                 │
                 ▼                 ▼
┌─────────────────────────────────────────────────────┐
│                   BROKER CLUSTER                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │  │
│  │  (Leader)   │  │ (Follower)  │  │ (Follower)  │  │
│  │  HTTP+gRPC  │  │  HTTP+gRPC  │  │  HTTP+gRPC  │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │
│         └────────────────┼────────────────┘         │
│                 Raft Consensus (TCP)                │
└─────────────────────────┬───────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│                   STORAGE LAYER                     │
│         Commit Log (append-only segments)           │
│         Index File (offset → byte position)         │
└─────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│                OBSERVABILITY LAYER                  │
│         Prometheus (metrics scraping)               │
│         Grafana (produce/consume/error dashboards)  │
└─────────────────────────────────────────────────────┘
```

### Commit Log
The heart of the system. Every message lands in an append-only `.log` file - no overwrites, no deletions, just an ever-growing record of truth. A companion `.index` file maps each message offset to its exact byte position in the log, making reads an O(log n) binary search instead of a full file scan.

Message wire format - hand-rolled binary, no bloat:
```
| offset (8B) | timestamp (8B) | key_size (4B) | value_size (4B) | key | value |
```

When a segment fills up, the log rolls over to a new one and keeps going. On restart, every segment is recovered from disk in order. Nothing is lost.

### Raft Consensus
Every write earns its place. Before a message touches the commit log, it clears Raft - replicated to a quorum of nodes, committed by the leader, then applied. Kill the leader mid-write and a new one is elected in ~150ms. The cluster doesn't flinch.

Built on `hashicorp/raft`, the same battle-hardened library running inside Consul and Nomad at scale across the industry. Real TCP transport between containers - not goroutines sharing memory.

### gRPC API
Every broker exposes a typed gRPC API alongside HTTP. Produce and Consume RPCs defined in protobuf, generated stubs, real binary transport. The HTTP API remains for human-readable access and health checks.

### Observability
Prometheus scrapes `/metrics` from all three brokers every 5 seconds. Grafana provisions the datasource and dashboard automatically on startup - no clicking required. Four panels: messages produced rate, messages consumed rate, produce errors, consume errors, per broker instance.

---

## Project Structure

```
distributed-event-log/
├── cmd/
│   ├── broker/         # Entry point: broker node (HTTP + gRPC)
│   └── cli/            # Producer/consumer CLI
├── internal/
│   ├── log/            # Commit log: segment.go, log.go
│   ├── broker/         # Broker node: Raft, HTTP, gRPC, metrics
│   ├── raft/           # Raft FSM: bridges hashicorp/raft and commit log
│   └── proto/          # Protobuf generated stubs
├── grafana/            # Provisioned datasource, dashboard, provider config
├── data/               # Runtime only, commit log files (gitignored)
├── docker-compose.yml
├── Dockerfile
├── prometheus.yml
└── Makefile
```

---

## Getting Started

**Requirements:** Docker + Docker Compose

**Start the full stack:**
```bash
docker compose up --build
```

This brings up 3 broker containers, Prometheus, and Grafana. Broker-1 bootstraps as leader, broker-2 and broker-3 join and begin replicating.

**Produce a message:**
```bash
curl -X POST http://localhost:8081/produce \
  -H "Content-Type: application/json" \
  -d '{"key":"order-1","value":"hello world"}'
# {"offset":0}
```

**Consume a message:**
```bash
curl http://localhost:8081/consume?offset=0
# {"offset":0,"key":"order-1","value":"hello world"}
```

**View metrics:**
- Prometheus targets: http://localhost:9090/targets
- Grafana dashboard: http://localhost:3000 (admin / admin → Dashboards → Distributed Event Log)

**Tear down:**
```bash
docker compose down -v
```

**Run tests (local):**
```bash
go test ./...
```

---

## Phases

| Phase | Status | Description |
|---|---|---|
| **Phase 1** | ✅ Complete | Single binary, 3 goroutine-brokers, in-memory Raft transport, disk commit log, HTTP + CLI |
| **Phase 2** | ✅ Complete | Docker containers, real TCP Raft, gRPC + protobuf, Prometheus + Grafana |
| **Phase 3** | 🔄 Planned | Cloud VMs (GCP), TLS, consumer groups, multi-partition topics, chaos engineering |

---

## Key Design Decisions

**Why build the commit log from scratch?**
Because that's where all the interesting problems live. Dropping in BoltDB or Badger would mean borrowing someone else's understanding of how storage actually works. The goal here was to own that understanding - byte offsets, segment rolling, binary index lookups and all.

**Why `hashicorp/raft` instead of implementing Raft from scratch?**
Raft looks deceptively simple on paper and is notoriously brutal to implement correctly. The interesting engineering in this project isn't reimplementing leader election timers, it's in designing the FSM, the bootstrap sequence, and the leader routing logic that makes the whole system coherent. `hashicorp/raft` handles the hard part so this project can focus on the harder part.

**Why Go?**
Goroutines map naturally to concurrent broker nodes. The standard library handles HTTP, binary encoding, and file I/O without pulling in a dependency tree. And frankly, Go rewards people who actually understand what's happening under the hood - which is exactly the point.

---

## References

- [The Log: What every software engineer should know about real-time data](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) - Jay Kreps, LinkedIn
- [Raft Consensus Algorithm](https://raft.github.io/) - Diego Ongaro
- [hashicorp/raft](https://github.com/hashicorp/raft) - Production Raft implementation
- [Apache Kafka Design Docs](https://kafka.apache.org/documentation/#design) - Storage internals reference

---

*Phase 3 is next. This project isn't finished... it's just getting started.*