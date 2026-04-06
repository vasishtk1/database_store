# Distributed KV store (Raft)

A small **key-value store** in Go backed by the **Raft** consensus algorithm. Multiple nodes agree on a single ordered log of writes; clients can talk over **TCP** (text protocol) or **HTTP/JSON**.

## What’s included

- **Raft** — leader election, log replication, persistence, **snapshots**, and follower **catch-up** after restarts (`internal/raft`)
- **3-node cluster** — config-driven peers and addresses (`config/cluster.json`)
- **HTTP API + dashboard** — `GET/PUT/DELETE` on `/keys/{key}`, `/status`, `/metrics`, and a simple UI on `/` (per-node `http_addr` in config)
- **Cluster smoke test** — `scripts/cluster_test.sh` (build binaries, exercise leader failover and restarts)

## Quick start (cluster)

1. Build: `go build -o bin/kvstore .` and `go build -o bin/kvctl ./cmd/kvctl`
2. Start three processes (each in its own terminal), e.g.  
   `./bin/kvstore --id node1 --config config/cluster.json --data data`  
   (repeat for `node2`, `node3`)
3. Use **kvctl** against `kv_addr` ports from config, or open the **dashboard** at e.g. `http://localhost:9001/`
4. Optional: `bash scripts/cluster_test.sh`

## Single-node mode

```bash
go run . --single --data data
```

Uses the original WAL + in-memory store path (no Raft).

## Requirements

- Go 1.21+
