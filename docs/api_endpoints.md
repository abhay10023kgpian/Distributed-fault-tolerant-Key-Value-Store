# API Endpoints Reference

The backend exposes a REST API on port `8080` for interacting with the Key-Value store and managing the cluster.

## Key-Value Operations

### `GET /kv/{key}`
Retrieves the value for a given key.
- **Success (200)**: `{"key": "foo", "value": "bar"}`
- **Not Found (404)**: `{"error": "key not found"}`

### `PUT /kv/{key}`
Sets the value for a given key. This operation must be handled by the Leader.
- **Request Body**: `{"value": "new_value"}`
- **Success (200)**: `{"accepted": true, "key": "foo", "value": "new_value", "leader_id": 0}`
- **Redirect/Error (500)**: `{"error": "not leader", "leader_id": 1}`

### `DELETE /kv/{key}`
Deletes a key from the store. This operation must be handled by the Leader.
- **Success (200)**: `{"deleted": true, "key": "foo", "leader_id": 0}`
- **Redirect/Error (500)**: `{"error": "not leader", "leader_id": 1}`

---

## Cluster Management

### `GET /cluster`
Returns the current status and Raft state of all nodes in the cluster.
- **Success (200)**:
```json
{
  "nodes": [
    {
      "id": 0,
      "state": "LEADER",
      "term": 2,
      "leader_id": 0,
      "last_log_index": 5,
      "commit_index": 5,
      "last_applied": 5,
      "alive": true
    },
    ...
  ]
}
```

### `GET /nodes/{id}`
Returns the status of a specific node.
- **Success (200)**: Same node object as seen in `/cluster`.

### `POST /nodes/{id}/kill`
Stops the node cleanly. The node stops sending heartbeats and participating in elections, but retains its WAL and in-memory state.
- **Success (200)**: `{"node_id": 1, "action": "killed", "success": true}`

### `POST /nodes/{id}/restart`
Restarts a killed node. The node boots up as a FOLLOWER, retaining its term and log, and begins processing heartbeats and elections again.
- **Success (200)**: `{"node_id": 1, "action": "restarted", "success": true}`

---

## Event Stream (SSE)

### `GET /events`
Opens a Server-Sent Events (SSE) connection that streams live cluster events (e.g. state changes, heartbeats, log commitments).

- **Format**: `text/event-stream`
- **Payload Example**:
```json
data: {"timestamp":"2023-10-25T10:00:00Z", "node_id":0, "type":"became_leader", "term":2, "message":"Node became leader"}
```
