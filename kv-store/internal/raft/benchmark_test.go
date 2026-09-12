package raft

import (
	"fmt"
	"testing"
	"time"
)

// setupCluster creates a 3-node cluster with LocalTransport, waits for
// leader election, and returns (nodes, leader, transport, cleanup).
func setupCluster(b *testing.B) ([]*RaftNode, *RaftNode, *LocalTransport) {
	b.Helper()

	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

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

	for _, node := range nodes {
		go node.runElectionTimer()
	}

	// Wait for leader.
	var leader *RaftNode
	deadline := time.After(10 * time.Second)

	for leader == nil {
		select {
		case <-deadline:
			b.Fatal("no leader elected")
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

	return nodes, leader, transport
}

// BenchmarkLeaderElection measures time to elect a leader in a 3-node cluster.
func BenchmarkLeaderElection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nodes := []*RaftNode{
			NewRaftNode(0),
			NewRaftNode(1),
			NewRaftNode(2),
		}

		peersMap := make(map[int]string)
		nodesMap := make(map[string]*RaftNode)
		for j, node := range nodes {
			address := fmt.Sprintf("local:%d", j)
			peersMap[j] = address
			nodesMap[address] = node
		}

		transport := NewLocalTransport(nodesMap)
		for _, node := range nodes {
			node.SetPeers(peersMap)
			node.SetTransport(transport)
		}

		for _, node := range nodes {
			go node.runElectionTimer()
		}

		// Wait for leader.
		elected := false
		deadline := time.After(10 * time.Second)

		for !elected {
			select {
			case <-deadline:
				b.Fatal("no leader elected")
			default:
				for _, node := range nodes {
					node.mu.Lock()
					if node.state == Leader {
						elected = true
					}
					node.mu.Unlock()
				}
				if !elected {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}

		for _, node := range nodes {
			node.Stop()
		}
	}
}

// BenchmarkLogAppend measures throughput of appending commands via
// the leader's Start() method (Raft log append only, no replication wait).
func BenchmarkLogAppend(b *testing.B) {
	nodes, leader, _ := setupCluster(b)
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cmd := Command{
			Type:  CommandSet,
			Key:   fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("val-%d", i),
		}
		_, _, ok := leader.Start(cmd)
		if !ok {
			b.Fatal("leader rejected command")
		}
	}

	b.StopTimer()
}

// BenchmarkReplication measures end-to-end replication latency:
// appending a command on the leader and waiting until all followers
// have committed it.
func BenchmarkReplication(b *testing.B) {
	nodes, leader, _ := setupCluster(b)
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	// Let cluster stabilize.
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cmd := Command{
			Type:  CommandSet,
			Key:   fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("val-%d", i),
		}

		index, _, ok := leader.Start(cmd)
		if !ok {
			b.Fatal("leader rejected command")
		}

		// Wait for all nodes to commit this entry.
		deadline := time.After(5 * time.Second)
		allCommitted := false

		for !allCommitted {
			select {
			case <-deadline:
				b.Fatalf("timed out waiting for commit of index %d", index)
			default:
				allCommitted = true
				for _, node := range nodes {
					node.mu.Lock()
					if node.commitIndex < index {
						allCommitted = false
					}
					node.mu.Unlock()
				}
				if !allCommitted {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}
	}

	b.StopTimer()
}

// BenchmarkRequestVote measures the raw RPC cost of a RequestVote call
// through the LocalTransport.
func BenchmarkRequestVote(b *testing.B) {
	nodes, _, transport := setupCluster(b)
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	args := RequestVoteArgs{
		Term:         100,
		CandidateID:  99,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transport.RequestVote("local:1", args)
	}
}

// BenchmarkAppendEntries measures the raw RPC cost of an AppendEntries call
// (empty heartbeat) through the LocalTransport.
func BenchmarkAppendEntries(b *testing.B) {
	nodes, _, transport := setupCluster(b)
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	args := AppendEntriesArgs{
		Term:         100,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transport.AppendEntries("local:1", args)
	}
}

// BenchmarkBurstReplication measures throughput of burst-writing N commands
// and then waiting for all to replicate.
func BenchmarkBurstReplication(b *testing.B) {
	nodes, leader, _ := setupCluster(b)
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()

	var lastIndex uint64
	for i := 0; i < b.N; i++ {
		cmd := Command{
			Type:  CommandSet,
			Key:   fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("val-%d", i),
		}
		idx, _, ok := leader.Start(cmd)
		if !ok {
			b.Fatal("leader rejected command")
		}
		lastIndex = idx
	}

	// Wait for all nodes to commit the last entry.
	deadline := time.After(10 * time.Second)
	allCommitted := false
	for !allCommitted {
		select {
		case <-deadline:
			b.Fatal("timed out waiting for full replication")
		default:
			allCommitted = true
			for _, node := range nodes {
				node.mu.Lock()
				if node.commitIndex < lastIndex {
					allCommitted = false
				}
				node.mu.Unlock()
			}
			if !allCommitted {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	b.StopTimer()
}
