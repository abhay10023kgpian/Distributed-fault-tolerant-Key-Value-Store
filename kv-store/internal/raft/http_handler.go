package raft

import (
	"encoding/json"
	"net/http"
)

func RegisterRaftHandlers(mux *http.ServeMux, node *RaftNode) {
	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if node.IsStopped() {
			http.Error(w, "node stopped", http.StatusServiceUnavailable)
			return
		}

		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reply := node.RequestVote(args)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if node.IsStopped() {
			http.Error(w, "node stopped", http.StatusServiceUnavailable)
			return
		}

		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reply := node.AppendEntries(args)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
}
