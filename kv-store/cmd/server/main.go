package main

import (
	"fmt"
	"kv-store/internal/events"
	"kv-store/internal/httpapi"
	"kv-store/internal/raft"
	"kv-store/internal/store"
	"kv-store/internal/wal"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	const numNodes = 3

	// Create data directory for WALs.
	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Println("Error creating data directory:", err)
		return
	}

	// Create event bus.
	eventBus := events.NewBus()

	// Create WALs, stores, and Raft nodes.
	var wals [numNodes]*wal.WAL
	var stores [numNodes]*store.Store
	var raftNodes [numNodes]*raft.RaftNode
	var kvNodes [numNodes]*raft.KVNode

	for i := 0; i < numNodes; i++ {
		walPath := filepath.Join(dataDir, fmt.Sprintf("node-%d.wal", i))

		w, err := wal.Open(walPath)
		if err != nil {
			fmt.Printf("Error opening WAL for node %d: %v\n", i, err)
			return
		}
		wals[i] = w

		s, err := store.New(w)
		if err != nil {
			fmt.Printf("Error creating store for node %d: %v\n", i, err)
			return
		}
		stores[i] = s

		raftNodes[i] = raft.NewRaftNode(i)
		raftNodes[i].SetEventBus(eventBus)
	}

	// Connect all nodes as peers.
	peers := make([]*raft.RaftNode, numNodes)
	for i := range raftNodes {
		peers[i] = raftNodes[i]
	}
	for _, node := range raftNodes {
		node.SetPeers(peers)
	}

	// Create KVNodes.
	for i := range raftNodes {
		kvNodes[i] = raft.NewKVNode(raftNodes[i], stores[i])
	}

	// Start election timers.
	for _, node := range raftNodes {
		go node.RunElectionTimer()
	}

	// Build HTTP handler.
	cluster := &httpapi.Cluster{
		KVNodes:   kvNodes[:],
		RaftNodes: peers,
		EventBus:  eventBus,
	}

	handler := httpapi.NewHandler(cluster)

	// Serve static frontend files if they exist.
	frontendDir := "frontend/dist"
	if _, err := os.Stat(frontendDir); err == nil {
		// Serve frontend static files.
		fs := http.FileServer(http.Dir(frontendDir))
		mux := http.NewServeMux()
		mux.Handle("/kv/", handler)
		mux.Handle("/cluster", handler)
		mux.Handle("/nodes/", handler)
		mux.Handle("/events", handler)
		mux.Handle("/", fs)
		handler = httpapi.CorsWrap(mux)
	}

	addr := ":8080"
	fmt.Printf("Starting distributed KV store on %s\n", addr)
	fmt.Printf("  %d Raft nodes started\n", numNodes)
	fmt.Printf("  API: http://localhost%s\n", addr)
	fmt.Printf("  Dashboard: http://localhost%s\n", addr)

	// Graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		fmt.Println("\nShutting down...")

		for i, node := range raftNodes {
			node.Stop()
			fmt.Printf("  Node %d stopped\n", i)
		}

		for i, w := range wals {
			if err := w.Close(); err != nil {
				fmt.Printf("  Error closing WAL %d: %v\n", i, err)
			}
		}

		os.Exit(0)
	}()

	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Println("Error starting server:", err)
	}
}