# Raft High-Level Overview

This document provides a high-level overview of the Raft consensus protocol as implemented in this repository.

## Components and Data Flow

The distributed KV store is built in layered abstractions:

1. **HTTP Handler** (`internal/httpapi/handler.go`): Exposes REST API endpoints and routes KV operations to the appropriate Raft node.
2. **Cluster & EventBus** (`internal/events`): Manages the cluster map and broadcasts non-blocking events for observability (e.g. SSE streaming).
3. **KVNode Layer** (`internal/raft/kv_node.go`): The distributed state machine wrapper that forwards logical `SET`/`DELETE` commands to the core Raft node.
4. **Raft Core** (`internal/raft/node.go`, `vote.go`, `append_entries.go`): The consensus algorithm managing Leader Election, Term Management, and Log Replication.
5. **WAL & Store** (`internal/wal`, `internal/store`): The persistence layer. Commands committed by Raft are applied to the key-value Store, which is backed by a Write-Ahead Log for durability.

## Workflow: Client `SET` Operation

> [!NOTE]
> All write operations (`SET`, `DELETE`) must go through the Leader. Followers automatically redirect or error back so the client can find the correct leader.

1. **Client Request**: The client sends a `PUT /kv/foo` request with the payload `{"value": "bar"}`.
2. **API Routing**: The handler determines if the receiving node is the Leader. If not, it returns a redirect/error indicating the current Leader's ID.
3. **KVNode Propose**: The `KVNode` encodes the command `SET foo bar` and passes it to `RaftNode.Start()`.
4. **Log Appended**: The Leader appends the command to its local in-memory log.
5. **Replication**: The Leader sends `AppendEntries` RPCs to all Followers concurrently.
6. **Commitment**: Once a majority (Quorum) of followers successfully replicate the log entry, the Leader marks the index as *Committed*.
7. **Application**: The `KVNode` listens to a commit channel, pulls the committed entry, parses it, and applies it to the `Store` (which writes it to disk via WAL).
8. **Response**: The API returns a success response to the client.

## Safe Restarts

> [!TIP]
> Nodes can be killed and restarted via the API or Dashboard.
>
> A `Kill` cleanly stops the heartbeat and election timer goroutines without dropping the in-memory log. A `Restart` resets the node to a `FOLLOWER` state and re-launches the timers. This simulates a safe network partition recovery.
