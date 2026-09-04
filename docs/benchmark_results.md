# Benchmark Results & Performance Evolution

> **System**: AMD Ryzen 5 5500U (12 threads) · Windows · Go 1.26 · `amd64`
>
> All benchmarks run with `-benchmem` flag. Timing benchmarks use `-benchtime=3s` (or `10s` where noted).

---

## Table of Contents

- [Final Results Summary](#final-results-summary)
- [Phase 1 — Naive Sync-Per-Write](#phase-1--naive-sync-per-write)
- [Phase 2 — WAL Append (Single-Writer Queue)](#phase-2--wal-append-single-writer-queue)
- [Phase 3 — Group Commit (Batch Sync)](#phase-3--group-commit-batch-sync)
- [Phase Transition Analysis](#phase-transition-analysis)
- [How to Reproduce](#how-to-reproduce)

---

## Final Results Summary

These are the **latest** benchmark numbers after all optimizations (Phase 3 — Group Commit):

| Benchmark | Latency | ops/sec | B/op | Allocs/op |
|---|---|---|---|---|
| `Store.Get` | **35.93 ns** | ~27.8M | 0 | 0 |
| `Store.Set` (sequential) | 740,593 ns (~741 µs) | ~1.35K | 563 | 15 |
| `Store.Set` (concurrent, 12 goroutines) | **130,490 ns (~130 µs)** | ~7.66K | 645 | 16 |
| Mixed 80% Read / 20% Write (concurrent) | **24,667 ns (~24.7 µs)** | ~40.5K | 121 | 2 |
| WAL Append — Group Commit | **115,579 ns (~116 µs)** | ~8.7K | 587 | 10 |

### Group Commit Batching Stats

| Metric | Value |
|---|---|
| Total operations | 27,590 |
| Total batches | 4,584 |
| Average batch size | **~6.02 records/fsync** |

---

## Phase 1 — Naive Sync-Per-Write

### Architecture

Every `Store.Set()` call held a **global lock** for the entire operation: encode → file write → `fsync` → update in-memory map. Each operation paid the full cost of an `fsync` individually.

```
Store.Set()
    │
    ▼
  Mutex.Lock()
    │
    ├── Encode record
    ├── file.Write()
    ├── file.Sync()          ← fsync per write (bottleneck)
    └── map[key] = value
    │
    ▼
  Mutex.Unlock()
```

### Results

| Benchmark | Latency | ops/sec | B/op | Allocs/op |
|---|---|---|---|---|
| `Store.Set` | **846,032 ns (~846 µs)** | ~1.18K | 259 | 9 |
| `Store.Get` | 39.98 ns | ~25.0M | 0 | 0 |

### Bottleneck Analysis

The write path was completely dominated by `fsync`. To isolate this:

| Operation | Latency | What it measures |
|---|---|---|
| WAL Write + Sync | **657,936 ns (~658 µs)** | `encode → write → fsync` |
| WAL Write only (no Sync) | **5,601 ns (~5.6 µs)** | `encode → write` |

`fsync` alone adds **~652 µs** of overhead — that's **99.1%** of the WAL write latency. This made it the clear target for optimization.

---

## Phase 2 — WAL Append (Single-Writer Queue)

### What Changed

Instead of each goroutine writing directly to the file under a global lock, writes were **decoupled** via a channel-based queue:

1. `Store.Set()` creates a `pendingWrite` and pushes it onto a buffered channel.
2. A dedicated **commit goroutine** pops from the channel, encodes, writes, and syncs.
3. The caller blocks on a per-request `done` channel until the commit goroutine signals completion.

```
Store.Set()
    │
    ▼
WAL.Append()
    │
    ▼
queue ← request          ← non-blocking enqueue
    │
    ▼
commit goroutine         ← single writer
    │
    ├── Encode
    ├── Write
    └── Sync             ← still 1 fsync per record
    │
    ▼
request.done ← result   ← unblock caller
    │
    ▼
Store updates in-memory map
```

### Results

| Benchmark | Latency | ops/sec | B/op | Allocs/op |
|---|---|---|---|---|
| WAL Append (per-record sync) | **657,936 ns (~658 µs)** | ~1.52K | 240 | 8 |
| `Store.Set` (sequential) | ~846 µs | ~1.18K | 259 | 9 |

### Why This Wasn't Enough

The single-writer model serialized I/O correctly and removed lock contention on the file, but it still performed **one `fsync` per record**. Under concurrent load, requests queued up but were still flushed one at a time. The commit goroutine infrastructure was now in place to enable the real win: **batching**.

---

## Phase 3 — Group Commit (Batch Sync)

### What Changed

The commit goroutine was enhanced to **drain all pending requests** from the queue before syncing, amortizing the cost of a single `fsync` across an entire batch:

1. Pop the first item from the channel (blocking — the goroutine sleeps when idle).
2. Eagerly drain any remaining items via a non-blocking `select` loop.
3. Encode **all** records in the batch.
4. Write **all** records to the file.
5. Issue **ONE** `fsync` for the entire batch.
6. Unblock **all** callers simultaneously.

```
              GROUP COMMIT
                   │
                   ▼
          ┌─────────────────┐
          │ Batch collection│   ← drain queue (non-blocking)
          └────────┬────────┘
                   ▼
              Write batch
                   │
              ┌────┴────┐
              │         │
            error      success
              │         │
              ▼         ▼
           notify    ONE fsync     ← amortized across ~6 records
                       │
                   ┌───┴───┐
                   │       │
                 error   success
                   │       │
                   ▼       ▼
                reject   commit    ← unblock all callers
```

Additionally, **ordered apply** was introduced via monotonic sequence numbers. Each `Append()` assigns a `seq`, and `applyOrdered()` buffers out-of-order arrivals to guarantee the in-memory map is always updated in strict order, regardless of goroutine scheduling.

### Results

| Benchmark | Latency | ops/sec | B/op | Allocs/op |
|---|---|---|---|---|
| WAL Append — Group Commit | **115,579 ns (~116 µs)** | ~8.7K | 587 | 10 |
| `Store.Set` (sequential) | 740,593 ns (~741 µs) | ~1.35K | 563 | 15 |
| `Store.Set` (concurrent) | **130,490 ns (~130 µs)** | ~7.66K | 645 | 16 |
| `Store.Get` | 35.93 ns | ~27.8M | 0 | 0 |
| Mixed 80R / 20W | 24,667 ns (~24.7 µs) | ~40.5K | 121 | 2 |

> [!NOTE]
> Sequential `Set` shows limited improvement (~846 → ~741 µs) because there's only one writer — no batching opportunity. The group commit design intentionally avoids artificial delays (no "wait for batch to fill"), so single-writer latency stays close to a raw sync.

### Batching Behavior Under Benchmark Load

The benchmark automatically reports batch statistics as the test scales up:

| Records | Batches | Avg Batch Size |
|---|---|---|
| 1 | 1 | 1.00 |
| 100 | 17–18 | 5.56–5.88 |
| 10,000 | 1,650–1,662 | 6.02–6.06 |
| 22,825–28,922 | 3,785–4,812 | **~6.01–6.03** |

The batch size **naturally converges to ~6 records/fsync** under sustained concurrent load, without any fixed-size configuration. This is an emergent property of the balance between the `fsync` latency (~116 µs) and the rate at which goroutines enqueue new requests.

---

## Phase Transition Analysis

### Phase 1 → Phase 2: Decoupled I/O

| Metric | Before | After | Change |
|---|---|---|---|
| Architecture | Global lock, inline I/O | Channel queue, dedicated writer | Serialized I/O without lock contention |
| `Store.Set` latency | 846 µs | ~846 µs | **No change** (still 1 sync/write) |
| Concurrency model | Blocked | Non-blocking enqueue | Foundation for batching |

**Takeaway**: This phase was a structural refactor, not a performance win. It decoupled the hot path from file I/O and set up the commit goroutine that Phase 3 would exploit.

---

### Phase 2 → Phase 3: Group Commit

| Metric | Before (per-record sync) | After (group commit) | Change |
|---|---|---|---|
| Concurrent Set latency | **830,407 ns** | **130,490 ns** | **6.4× faster, 84.3% reduction** |
| WAL Append latency | 657,936 ns | 115,579 ns | **5.7× faster, 82.4% reduction** |
| fsyncs per record | 1.00 | ~0.17 (1 per ~6 records) | **~6× fewer disk syncs** |
| Batch size | 1 | ~6 | Naturally emergent |

**Takeaway**: Batching `fsync` was the single highest-impact optimization. By amortizing one of the most expensive syscalls across multiple records, concurrent write throughput jumped from ~1.2K to ~7.7K ops/sec.

---

### Full Journey: Phase 1 → Phase 3

```
Concurrent Set Latency

Phase 1 (naive):     ████████████████████████████████████████  846 µs
Phase 2 (queue):     ████████████████████████████████████████  830 µs
Phase 3 (batch):     ██████                                   130 µs
                     └──────────────────────────────────────┘
                     0                                     900 µs
```

| Metric | Phase 1 | Phase 3 | Improvement |
|---|---|---|---|
| Concurrent Set latency | 846 µs | 130 µs | **6.5×** |
| Concurrent Set throughput | ~1.18K ops/s | ~7.66K ops/s | **6.5×** |
| Get latency | ~40 ns | ~36 ns | Stable |
| Write path fsync count | 1 per write | 1 per ~6 writes | **~6×** fewer |

---

### Cost of Durability

Comparing write-without-sync to the final group-commit write shows the residual cost of durability:

| Path | Latency | Notes |
|---|---|---|
| WAL Write (no sync) | 5.6 µs | OS page cache only — not durable |
| WAL Write (group commit) | 116 µs | Durable, amortized across ~6 records |
| WAL Write (per-record sync) | 658 µs | Durable, full fsync cost per record |

Group commit brings the durable-write cost from **117× the cached write** down to **~21×** — a significant improvement while maintaining crash safety.

---

## How to Reproduce

All benchmarks can be run from the `kv-store/` directory:

```bash
# Store benchmarks (Get, Set, ConcurrentSet, MixedReadWrite)
go test ./internal/store -bench=. -benchmem -benchtime=3s -v

# Individual store benchmarks
go test ./internal/store -bench=BenchmarkSet -benchmem -benchtime=3s
go test ./internal/store -bench=BenchmarkGet -benchmem -benchtime=3s
go test ./internal/store -bench=BenchmarkConcurrentSet -benchmem -benchtime=3s
go test ./internal/store -bench=BenchmarkMixedReadWrite -benchmem -benchtime=3s

# WAL group commit benchmark
go test ./internal/wal -bench=BenchmarkWALAppendGroupCommit -benchmem -benchtime=3s -v

# WAL raw append (per-record sync baseline)
go test ./internal/wal -bench=BenchmarkWALAppend -benchmem -benchtime=10s

# WAL write-only (no fsync — measures encode + write overhead)
go test ./internal/wal -bench=BenchmarkWriteOnly -benchmem -benchtime=10s

# Race detector (all tests)
go test -race ./...
```

> [!TIP]
> The `-v` flag on concurrent benchmarks will print batch statistics (records, batches, average batch size) which are useful for verifying that group commit is working correctly.
