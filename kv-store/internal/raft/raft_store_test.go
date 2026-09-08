package raft

import (
	"fmt"
	"os"
	"testing"
	"time"

	"kv-store/internal/store"
	"kv-store/internal/wal"
)

func TestRaftStoreReplication(t *testing.T) {
	// Create a temporary directory manually.
	// We do this instead of t.TempDir() because the WAL files
	// must be explicitly closed before Windows allows deletion.
	dir, err := os.MkdirTemp("", "raft-store-test")
	if err != nil {
		t.Fatal(err)
	}

	var stores [3]*store.Store
	var wals [3]*wal.WAL

	// Create one WAL + Store for each Raft node.
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/wal-%d.log", dir, i)

		w, err := wal.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		wals[i] = w

		s, err := store.New(w)
		if err != nil {
			t.Fatal(err)
		}

		stores[i] = s
	}

	// Close WALs and remove temporary files when the test finishes.
	defer func() {
		for _, w := range wals {
			if w != nil {
				if err := w.Close(); err != nil {
					t.Errorf("failed to close WAL: %v", err)
				}
			}
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()

	// Create 3 Raft nodes.
	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

	// Every node knows about every other node.
	for _, node := range nodes {
		node.peers = nodes
	}

	// Connect each Raft node's committed commands
	// to its own Store.
	for i, node := range nodes {
		s := stores[i]

		node.SetApplyFunc(func(cmd Command) {
			switch cmd.Type {
			case CommandSet:
				s.Apply(wal.Record{
					Op:    wal.OpPut,
					Key:   []byte(cmd.Key),
					Value: []byte(cmd.Value),
				})

			case CommandDelete:
				s.Apply(wal.Record{
					Op:  wal.OpDelete,
					Key: []byte(cmd.Key),
				})
			}
		})
	}

	// Start election timers.
	for _, node := range nodes {
		go node.runElectionTimer()
	}

	// Wait for a leader.
	var leader *RaftNode

	deadline := time.After(2 * time.Second)

	for leader == nil {
		select {
		case <-deadline:
			t.Fatal("no leader elected")

		default:
			for _, node := range nodes {
				node.mu.Lock()

				if node.state == Leader {
					leader = node
				}

				node.mu.Unlock()
			}

			if leader == nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// Submit a command through the leader.
	_, _, ok := leader.Start(Command{
		Type:  CommandSet,
		Key:   "foo",
		Value: "bar",
	})

	if !ok {
		t.Fatal("leader rejected command")
	}

	// Wait until the command is applied to all Stores.
	deadline = time.After(2 * time.Second)

	for {
		allApplied := true

		for _, s := range stores {
			value, exists := s.Get("foo")

			if !exists || value != "bar" {
				allApplied = false
				break
			}
		}

		if allApplied {
			break
		}

		select {
		case <-deadline:
			t.Fatal("command was not applied to all stores")

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Final verification.
	for i, s := range stores {
		value, exists := s.Get("foo")

		if !exists {
			t.Fatalf("node %d: key not found", i)
		}

		if value != "bar" {
			t.Fatalf(
				"node %d: expected value %q, got %q",
				i,
				"bar",
				value,
			)
		}
	}
}