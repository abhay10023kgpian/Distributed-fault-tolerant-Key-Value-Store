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
	// --------------------------------------------------
	// 1. Create temporary directory for the 3 WALs
	// --------------------------------------------------

	dir, err := os.MkdirTemp("", "raft-store-test")
	if err != nil {
		t.Fatal(err)
	}

	var stores [3]*store.Store
	var wals [3]*wal.WAL

	// --------------------------------------------------
	// 2. Create one WAL + Store for each Raft node
	// --------------------------------------------------

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

	// --------------------------------------------------
	// 3. Create 3 Raft nodes
	// --------------------------------------------------

	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

	// --------------------------------------------------
	// 4. Connect all Raft nodes
	// --------------------------------------------------

	peersMap := make(map[int]string)
	nodesMap := make(map[string]*RaftNode)
	for i, node := range nodes {
		address := fmt.Sprintf("local:%d", i)
		peersMap[i] = address
		nodesMap[address] = node
	}
	transport := NewLocalTransport(nodesMap)

	for _, node := range nodes {
		node.SetPeers(peersMap)
		node.SetTransport(transport)
	}

	// --------------------------------------------------
	// 5. Create KVNode for every Raft + Store pair
	// --------------------------------------------------

	kvNodes := make([]*KVNode, 3)

	for i := range nodes {
		kvNodes[i] = NewKVNode(nodes[i], stores[i])
	}

	// --------------------------------------------------
	// 6. Start election timers
	// --------------------------------------------------

	for _, node := range nodes {
		go node.runElectionTimer()
	}

	// --------------------------------------------------
	// 7. Wait for a leader
	// --------------------------------------------------

	var leader *RaftNode

	deadline := time.After(5 * time.Second)

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

	// --------------------------------------------------
	// 8. Find the KVNode belonging to the leader
	// --------------------------------------------------

	var leaderKV *KVNode

	for i, node := range nodes {
		if node == leader {
			leaderKV = kvNodes[i]
			break
		}
	}

	if leaderKV == nil {
		t.Fatal("could not find leader KV node")
	}

	// --------------------------------------------------
	// 9. Client sends SET through KVNode
	// --------------------------------------------------

	ok, _, _ := leaderKV.Set("foo", "bar")

	if !ok {
		t.Fatal("leader rejected SET")
	}

	// --------------------------------------------------
	// 10. Wait until all Stores receive the command
	// --------------------------------------------------

	deadline = time.After(5 * time.Second)

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

	// --------------------------------------------------
	// 11. Final verification
	// --------------------------------------------------

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

	// --------------------------------------------------
	// 12. Stop Raft nodes
	// --------------------------------------------------

	for _, node := range nodes {
		node.Stop()
	}

	// --------------------------------------------------
	// 13. Close WALs
	// --------------------------------------------------

	for _, w := range wals {
		if w != nil {
			if err := w.Close(); err != nil {
				t.Errorf("failed to close WAL: %v", err)
			}
		}
	}

	// --------------------------------------------------
	// 14. Remove temporary directory
	// --------------------------------------------------

	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("failed to remove test directory: %v", err)
	}
}



func TestFollowerRedirect(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-redirect-test")
	if err != nil {
		t.Fatal(err)
	}

	var stores [3]*store.Store
	var wals [3]*wal.WAL

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

	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

	peersMap2 := make(map[int]string)
	nodesMap2 := make(map[string]*RaftNode)
	for i, node := range nodes {
		address := fmt.Sprintf("local:%d", i)
		peersMap2[i] = address
		nodesMap2[address] = node
	}
	transport2 := NewLocalTransport(nodesMap2)

	for _, node := range nodes {
		node.SetPeers(peersMap2)
		node.SetTransport(transport2)
	}

	kvNodes := make([]*KVNode, 3)

	for i := range nodes {
		kvNodes[i] = NewKVNode(nodes[i], stores[i])
	}

	for _, node := range nodes {
		go node.runElectionTimer()
	}

	// Wait for leader.
	var leader *RaftNode

	deadline := time.After(5 * time.Second)

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

	// Find leader and follower.
	leaderIndex := -1
	followerIndex := -1

	for i, node := range nodes {
		if node == leader {
			leaderIndex = i
		} else if followerIndex == -1 {
			followerIndex = i
		}
	}

	if leaderIndex == -1 || followerIndex == -1 {
		t.Fatal("could not identify leader/follower")
	}

	// Give the followers time to learn the leader ID.
	time.Sleep(500 * time.Millisecond)

	// Try writing through a follower.
	ok, reportedLeader, _ := kvNodes[followerIndex].Set("foo", "bar")

	if ok {
		t.Fatal("follower accepted client write")
	}

	if reportedLeader != leader.id {
		t.Fatalf(
			"expected leader %d, follower reported %d",
			leader.id,
			reportedLeader,
		)
	}

	// Now send the same request to the actual leader.
	ok, reportedLeader, _ = kvNodes[leaderIndex].Set("foo", "bar")

	if !ok {
		t.Fatal("leader rejected client write")
	}

	if reportedLeader != leader.id {
		t.Fatalf(
			"expected leader %d, got %d",
			leader.id,
			reportedLeader,
		)
	}

	// Wait for replication.
	deadline = time.After(5 * time.Second)

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
			t.Fatal("command was not replicated to all stores")

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Stop Raft before closing WALs.
	for _, node := range nodes {
		node.Stop()
	}

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
}


func TestRaftSetAndDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-delete-test")
	if err != nil {
		t.Fatal(err)
	}

	var stores [3]*store.Store
	var wals [3]*wal.WAL

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

	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

	peersMap3 := make(map[int]string)
	nodesMap3 := make(map[string]*RaftNode)
	for i, node := range nodes {
		address := fmt.Sprintf("local:%d", i)
		peersMap3[i] = address
		nodesMap3[address] = node
	}
	transport3 := NewLocalTransport(nodesMap3)

	for _, node := range nodes {
		node.SetPeers(peersMap3)
		node.SetTransport(transport3)
	}

	kvNodes := make([]*KVNode, 3)

	for i := range nodes {
		kvNodes[i] = NewKVNode(nodes[i], stores[i])
	}

	for _, node := range nodes {
		go node.runElectionTimer()
	}

	// Wait for leader.
	var leader *RaftNode

	deadline := time.After(5 * time.Second)

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

	leaderIndex := -1

	for i, node := range nodes {
		if node == leader {
			leaderIndex = i
			break
		}
	}

	if leaderIndex == -1 {
		t.Fatal("could not find leader")
	}

	leaderKV := kvNodes[leaderIndex]

	// -----------------------------
	// SET
	// -----------------------------

	ok, _, _ := leaderKV.Set("foo", "bar")

	if !ok {
		t.Fatal("leader rejected SET")
	}

	// Wait for SET to reach all stores.
	deadline = time.After(5 * time.Second)

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
			t.Fatal("SET was not replicated to all stores")

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// -----------------------------
	// DELETE
	// -----------------------------

	ok, _, _ = leaderKV.Delete("foo")

	if !ok {
		t.Fatal("leader rejected DELETE")
	}

	// Wait for DELETE to reach all stores.
	deadline = time.After(5 * time.Second)

	for {
		allDeleted := true

		for _, s := range stores {
			_, exists := s.Get("foo")

			if exists {
				allDeleted = false
				break
			}
		}

		if allDeleted {
			break
		}

		select {
		case <-deadline:
			t.Fatal("DELETE was not replicated to all stores")

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Final verification.
	for i, s := range stores {
		if _, exists := s.Get("foo"); exists {
			t.Fatalf("node %d: key still exists after DELETE", i)
		}
	}

	// Cleanup.
	for _, node := range nodes {
		node.Stop()
	}

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
}