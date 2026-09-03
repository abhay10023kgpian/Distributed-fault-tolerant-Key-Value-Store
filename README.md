# Distributed Fault-Tolerant Key-Value Store

A high-performance, crash-resilient key-value store built from scratch in Go. Every write is persisted to a **Write-Ahead Log (WAL)** before it touches the in-memory map, ensuring zero data loss on unexpected shutdowns. A **group-commit** batching strategy amortizes the cost of `fsync` across multiple concurrent writers, achieving **~6× lower write latency** compared to a naive per-write sync approach.

---

## Highlights

| Feature | Details |
|---|---|
| **Durability** | WAL with CRC-32 integrity checks; survives crashes and truncated writes |
| **Concurrency** | `RWMutex`-guarded reads + ordered-apply writes; data-race free (`go test -race`) |
| **Group Commit** | Batches ~6 records per `fsync`, cutting write latency from **~830 µs → ~130 µs** |
| **HTTP API** | RESTful `GET` / `PUT` / `DELETE` on `/kv/{key}` |
| **Recovery** | Full WAL replay on startup — replays `PUT` and `DELETE` operations to rebuild state |

---

## Architecture

```
                    ┌─────────────────────┐
                    │     HTTP Server      │
                    │    (:8080 /kv/)      │
                    └────────┬────────────┘
                             │
                    ┌────────▼────────────┐
                    │       Store         │
                    │  (in-memory map)    │
                    └────────┬────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
          READ path                     WRITE path
              │                             │
          RWMutex.RLock                 WAL.Append()
              │                             │
           map[key]                   queue ← request
              │                             │
           ~36 ns                   commit goroutine
                                            │
                                    ├── Encode batch
                                    ├── Write batch
                                    └── ONE fsync
                                            │
                                    request.done ← nil
                                            │
                                    applyOrdered()
                                            │
                                    update in-memory map
```

### Write Path (Group Commit)

1. `Store.Set()` / `Store.Delete()` builds a WAL `Record` and calls `WAL.Append()`.
2. `Append()` assigns a monotonic sequence number, enqueues the record, and blocks on a per-request `done` channel.
3. A dedicated **commit goroutine** drains the queue, collecting all pending writes into a batch.
4. The entire batch is encoded, written, and flushed with a **single `fsync`**.
5. All callers in the batch are unblocked simultaneously.
6. `applyOrdered()` ensures records are applied to the in-memory map in strict sequence order, even if goroutines arrive out of order.

### Read Path

Reads acquire only an `RWMutex.RLock`, hitting the in-memory `map[string]string` directly — **zero disk I/O, ~36 ns latency**.

### Recovery

On startup, `WAL.Replay()` reads every record from the log file, validates CRC-32 checksums, and gracefully handles truncated tail records (partial writes from a crash). The store replays each `PUT`/`DELETE` to rebuild the map.

---

## WAL Binary Format

Each record on disk follows this layout:

```
┌──────────┬──────┬──────────┬────────────┬─────┬───────┬──────────┐
│ Length   │  Op  │ Key Len  │ Value Len  │ Key │ Value │ CRC-32   │
│ (4B u32) │ (1B) │ (4B u32) │ (4B u32)   │ var │ var   │ (4B u32) │
└──────────┴──────┴──────────┴────────────┴─────┴───────┴──────────┘
  ← header →  ← ─────────── body ──────────────────── → ← tail →

• Length   = size of everything after this field (body + CRC)
• CRC-32  = IEEE checksum over the body (Op + KeyLen + ValueLen + Key + Value)
```

All multi-byte integers are **big-endian**.

---

## Project Structure

```
kv-store/
├── cmd/
│   └── server/
│       └── main.go              # HTTP server entry point
├── internal/
│   ├── store/
│   │   ├── store.go             # In-memory store with WAL integration
│   │   ├── store_test.go        # Concurrency & persistence tests
│   │   ├── store_bench_test.go  # Set / Get / Mixed benchmarks
│   │   └── benchmark.txt        # Benchmark history & analysis
│   └── wal/
│       ├── wal.go               # WAL: encode, append, replay, group commit
│       ├── wal_test.go          # Unit tests (encode, append, replay, crash)
│       ├── wal_batch_test.go    # Group commit batch tests
│       ├── wal_benchmark_test.go
│       └── wal_perf_test.go
└── go.mod
```

---

## Getting Started

### Prerequisites

- **Go 1.26+**

### Run the Server

```bash
cd kv-store
go run ./cmd/server
```

The server starts on **`http://localhost:8080`**.

### API Usage

#### Set a key

```bash
curl -X PUT http://localhost:8080/kv/mykey -d 'myvalue'
# 204 No Content
```

#### Get a key

```bash
curl http://localhost:8080/kv/mykey
# myvalue
```

#### Delete a key

```bash
curl -X DELETE http://localhost:8080/kv/mykey
# 204 No Content
```

#### Error responses

| Status | Meaning |
|--------|---------|
| `400` | Missing key in URL |
| `404` | Key not found |
| `405` | HTTP method not supported |
| `500` | Internal error (WAL write failure) |

---

## Testing

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Verbose output
go test -v ./...
```

### Test Coverage

| Area | Tests |
|------|-------|
| **WAL** | Encode/decode, append, replay, truncated record recovery, sync failure, write failure, group commit |
| **Store** | Concurrent set, concurrent get, concurrent read-write, ordered apply, persistence across restarts |

---

## Benchmarks

```bash
# Store benchmarks
go test ./internal/store -bench=. -benchmem -benchtime=3s

# WAL group commit benchmark
go test ./internal/wal -bench=BenchmarkWALAppendGroupCommit -benchmem -benchtime=3s
```

### Results (AMD Ryzen 5 5500U)

| Benchmark | Latency | ops/sec | Allocs/op |
|-----------|---------|---------|-----------|
| `Store.Get` | **~36 ns** | ~27.9M | 0 |
| `Store.Set` (sequential) | ~741 µs | ~1.35K | 15 |
| `Store.Set` (concurrent) | **~130 µs** | ~7.66K | 16 |
| Mixed 80% Read / 20% Write | ~24.7 µs | ~40.5K | 2 |
| WAL Append (group commit) | ~116 µs | ~8.6K | 10 |

### Group Commit Impact

```
Concurrent Set latency
  Before batching:  830,407 ns/op
  After batching:   130,490 ns/op
  ─────────────────────────────────
  Speedup:          ~6.4×
  Latency reduction: 84.3%

Average batch size:  ~6 records/fsync
```

---

## Design Decisions

1. **WAL-first writes** — The record is durable on disk *before* the in-memory map is updated. On crash, the WAL is the source of truth.

2. **Group commit over fixed-size batching** — Instead of waiting for a fixed batch to fill (which adds latency under low load), the commit goroutine eagerly drains whatever is already queued. Under high concurrency this naturally collects ~6 records per fsync; under low load a single record goes through without artificial delay.

3. **Ordered apply via sequence numbers** — Each `Append()` assigns a monotonic sequence number. `applyOrdered()` buffers out-of-order arrivals and replays them in sequence, guaranteeing a consistent view of the map regardless of goroutine scheduling.

4. **CRC-32 checksums** — Every WAL record carries a CRC-32 over the body, catching bit-rot and partial writes.

5. **Truncated tail tolerance** — `Replay()` treats `io.ErrUnexpectedEOF` as end-of-log rather than a fatal error, so a crash mid-write doesn't prevent recovery of all prior records.

---

## Roadmap

- [ ] Log compaction / snapshotting
- [ ] TTL-based key expiration
- [ ] Raft consensus for multi-node replication
- [ ] gRPC transport layer
- [ ] Prometheus metrics export

---

## License

This project is open source. See [LICENSE](LICENSE) for details.
