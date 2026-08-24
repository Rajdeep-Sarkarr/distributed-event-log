# distributed-event-log

![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/license-MIT-green?style=flat)
![Phase](https://img.shields.io/badge/phase-1%20complete-brightgreen?style=flat)

Kafka is a marvel of distributed systems engineering. So I built one from scratch to understand why.

This is a fault-tolerant, distributed event log written in Go - not a tutorial project, not a wrapper around someone else's library. The storage engine, the binary wire format, the cluster coordination - all of it built from first principles. Because reading about append-only logs and Raft consensus is one thing. Writing the code that makes them work is another thing entirely.

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
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │
│         └────────────────┼────────────────┘         │
│                    Raft Consensus                   │
└─────────────────────────┬───────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│                   STORAGE LAYER                     │
│         Commit Log (append-only segments)           │
│         Index File (offset → byte position)         │
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

Built on `hashicorp/raft`, the same battle-hardened library running inside Consul and Nomad at scale across the industry.

---

## Project Structure

```
distributed-event-log/
├── cmd/
│   ├── broker/         # Entry point: starts the 3-node cluster+HTTP API
│   └── cli/            # Producer/consumer CLI
├── internal/
│   ├── log/            # Commit log: segment.go, log.go
│   ├── broker/         # Broker node: owns partitions, talks Raft
│   └── raft/           # Raft FSM: bridges hashicorp/raft and the commit log
├── data/               # Runtime only, commit log files (gitignored)
└── Makefile
```

---

## Getting Started

**Requirements:** Go 1.21+

**Build:**
```bash
make build
```

**Run the cluster:**
```bash
.\broker.exe
# broker cluster started on :8080
```

**Produce a message:**
```bash
.\cli.exe produce --topic orders --msg "hello world"
# 0
```

**Consume a message:**
```bash
.\cli.exe consume --offset 0
# key: msg
# value: hello world
```

**Run tests:**
```bash
make test
```

---

## Phases

| Phase | Status | Description |
|---|---|---|
| **Phase 1** | Complete | Single binary, 3 goroutine-brokers, in-memory Raft transport, disk commit log, HTTP+CLI |
| **Phase 2** | In progress | Docker containers, real TCP, gRPC+protobuf, Prometheus+Grafana |
| **Phase 3** | Planned | Cloud VMs, TLS, consumer groups, multi-partition topics, chaos engineering |

---

## Key Design Decisions

**Why build the commit log from scratch?**
Because that's where all the interesting problems live. Dropping in BoltDB or Badger would mean borrowing someone else's understanding of how storage actually works. The goal here was to own that understanding - byte offsets, segment rolling, binary index lookups and all.

**Why `hashicorp/raft` instead of implementing Raft from scratch?**
Raft looks deceptively simple on paper and is notoriously brutal to implement correctly. The interesting engineering in this project isn't reimplementing leader election timers, it's in designing the FSM, the bootstrap sequence, and the leader routing logic that makes the whole system coherent. `hashicorp/raft` handles the hard part so this project can focus on the harder part.

**Why Go?**
Goroutines map naturally to concurrent broker nodes. The standard library handles HTTP, binary encoding, and file I/O without pulling in a dependency tree. And frankly, Go rewards people who actually understand what's happening under the hood which is exactly the point.

---

## References

- [The Log: What every software engineer should know about real-time data](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) - Jay Kreps, LinkedIn
- [Raft Consensus Algorithm](https://raft.github.io/) - Diego Ongaro
- [hashicorp/raft](https://github.com/hashicorp/raft) - Production Raft implementation
- [Apache Kafka Design Docs](https://kafka.apache.org/documentation/#design) - Storage internals reference

---

*Phase 2 and Phase 3 are actively in development. This project isn't finished... It's just getting started.*
```