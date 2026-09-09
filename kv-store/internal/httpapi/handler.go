package httpapi

import (
	"encoding/json"
	"fmt"
	"kv-store/internal/events"
	"kv-store/internal/raft"
	"net/http"
	"strconv"
	"strings"
)

// Cluster holds the KVNodes, RaftNodes, and event bus.
type Cluster struct {
	KVNodes   []*raft.KVNode
	RaftNodes []*raft.RaftNode
	EventBus  *events.Bus
}

// NewHandler returns an http.Handler with all routes registered.
func NewHandler(cluster *Cluster) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", kvHandler(cluster))
	mux.HandleFunc("/cluster", clusterHandler(cluster))
	mux.HandleFunc("/nodes/", nodesHandler(cluster))
	mux.HandleFunc("/events", sseHandler(cluster))

	return corsMiddleware(mux)
}

// corsMiddleware adds CORS headers for frontend dev server.
func corsMiddleware(next http.Handler) http.Handler {
	return CorsWrap(next)
}

// CorsWrap adds CORS headers. Exported for use by main when combining handlers.
func CorsWrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// --- KV Handlers ---

func kvHandler(cluster *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			httpError(w, http.StatusBadRequest, "missing key")
			return
		}

		// Find the leader's KVNode (or any node to attempt through).
		// We use node 0 as the entry point; Raft handles redirection.
		kvNode := findLeaderKVNode(cluster)
		if kvNode == nil {
			// No leader — use any alive node so the follower redirect works.
			kvNode = findAnyAliveKVNode(cluster)
			if kvNode == nil {
				httpError(w, http.StatusServiceUnavailable, "no alive nodes")
				return
			}
		}

		switch r.Method {
		case http.MethodGet:
			handleGet(w, cluster, key)

		case http.MethodPut:
			handlePut(w, kvNode, cluster, key, r)

		case http.MethodDelete:
			handleDelete(w, kvNode, cluster, key)

		default:
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGet(w http.ResponseWriter, cluster *Cluster, key string) {
	// GET can be served from any node (linearizable reads not required for this demo).
	for _, kv := range cluster.KVNodes {
		if !kv.Raft.IsStopped() {
			value, ok := kv.Get(key)
			if !ok {
				httpError(w, http.StatusNotFound, "key not found")
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{
				"key":   key,
				"value": value,
			})
			return
		}
	}
	httpError(w, http.StatusServiceUnavailable, "no alive nodes")
}

func handlePut(w http.ResponseWriter, kvNode *raft.KVNode, cluster *Cluster, key string, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}

	ok, leaderID, index := kvNode.Set(key, body.Value)
	if !ok {
		// Follower: redirect to leader.
		writeJSON(w, http.StatusTemporaryRedirect, map[string]interface{}{
			"error":     "not leader",
			"leader_id": leaderID,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":       key,
		"value":     body.Value,
		"accepted":  true,
		"leader_id": leaderID,
		"log_index": index,
	})
}

func handleDelete(w http.ResponseWriter, kvNode *raft.KVNode, cluster *Cluster, key string) {
	ok, leaderID, index := kvNode.Delete(key)
	if !ok {
		writeJSON(w, http.StatusTemporaryRedirect, map[string]interface{}{
			"error":     "not leader",
			"leader_id": leaderID,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":       key,
		"deleted":   true,
		"leader_id": leaderID,
		"log_index": index,
	})
}

// --- Cluster Status ---

func clusterHandler(cluster *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		nodes := make([]raft.NodeStatus, len(cluster.RaftNodes))
		for i, node := range cluster.RaftNodes {
			nodes[i] = node.Status()
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"nodes": nodes,
		})
	}
}

// --- Node Control ---

func nodesHandler(cluster *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse /nodes/{id} or /nodes/{id}/kill
		path := strings.TrimPrefix(r.URL.Path, "/nodes/")
		parts := strings.Split(path, "/")

		if len(parts) == 0 || parts[0] == "" {
			httpError(w, http.StatusBadRequest, "missing node id")
			return
		}

		id, err := strconv.Atoi(parts[0])
		if err != nil || id < 0 || id >= len(cluster.RaftNodes) {
			httpError(w, http.StatusBadRequest, "invalid node id")
			return
		}

		if len(parts) == 1 {
			// GET /nodes/{id} — return node status.
			if r.Method != http.MethodGet {
				httpError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			writeJSON(w, http.StatusOK, cluster.RaftNodes[id].Status())
			return
		}

		if len(parts) == 2 {
			if parts[1] == "kill" && r.Method == http.MethodPost {
				node := cluster.RaftNodes[id]
				if node.IsStopped() {
					writeJSON(w, http.StatusOK, map[string]interface{}{
						"node_id": id,
						"action":  "already_stopped",
						"success": false,
					})
					return
				}

				node.Stop()

				writeJSON(w, http.StatusOK, map[string]interface{}{
					"node_id": id,
					"action":  "killed",
					"success": true,
				})
				return
			}

			if parts[1] == "restart" && r.Method == http.MethodPost {
				node := cluster.RaftNodes[id]
				if !node.IsStopped() {
					writeJSON(w, http.StatusOK, map[string]interface{}{
						"node_id": id,
						"action":  "already_running",
						"success": false,
					})
					return
				}
				node.Restart()
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"node_id": id,
					"action":  "restarted",
					"success": true,
				})
				return
			}
		}

		httpError(w, http.StatusNotFound, "not found")
	}
}

// --- SSE Handler ---

func sseHandler(cluster *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			httpError(w, http.StatusInternalServerError, "streaming not supported")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		sub := cluster.EventBus.Subscribe()
		defer cluster.EventBus.Unsubscribe(sub)

		nodeFilterStr := r.URL.Query().Get("node")
		categoryFilterStr := r.URL.Query().Get("category")

		// Send recent events for catch-up.
		for _, e := range cluster.EventBus.Recent() {
			if !matchesFilter(e, nodeFilterStr, categoryFilterStr) {
				continue
			}
			data, _ := json.Marshal(e)
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-sub.Ch:
				if !ok {
					return
				}
				if !matchesFilter(event, nodeFilterStr, categoryFilterStr) {
					continue
				}
				data, _ := json.Marshal(event)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}

func matchesFilter(e events.Event, nodeFilter string, category string) bool {
	if nodeFilter != "" && nodeFilter != "all" {
		if fmt.Sprintf("%d", e.NodeID) != nodeFilter && fmt.Sprintf("%d", e.PeerID) != nodeFilter {
			return false
		}
	}

	if category == "heartbeats" {
		switch e.Type {
		case events.HeartbeatSent, events.HeartbeatReceived, events.LogReplicated, events.AppendEntriesSent, events.AppendEntriesSuccess, events.AppendEntriesFailed:
			return true
		default:
			return false
		}
	} else if category == "elections" {
		switch e.Type {
		case events.ElectionStarted, events.VoteGranted, events.VoteRejected, events.BecameCandidate, events.BecameLeader, events.BecameFollower, events.LeaderChanged, events.NodeStopped, events.NodeRestarted:
			return true
		default:
			return false
		}
	}

	return true
}

// --- Helpers ---

func findLeaderKVNode(cluster *Cluster) *raft.KVNode {
	for _, kv := range cluster.KVNodes {
		if !kv.Raft.IsStopped() && kv.Raft.GetState() == raft.Leader {
			return kv
		}
	}
	return nil
}

func findAnyAliveKVNode(cluster *Cluster) *raft.KVNode {
	for _, kv := range cluster.KVNodes {
		if !kv.Raft.IsStopped() {
			return kv
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
