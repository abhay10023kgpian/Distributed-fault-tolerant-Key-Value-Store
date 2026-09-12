package raft

import "fmt"

type LocalTransport struct {
	nodes map[string]*RaftNode
}

func NewLocalTransport(nodes map[string]*RaftNode) *LocalTransport {
	return &LocalTransport{
		nodes: nodes,
	}
}

func (t *LocalTransport) RequestVote(peerAddress string, args RequestVoteArgs) (RequestVoteReply, error) {
	node, ok := t.nodes[peerAddress]
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("node unreachable")
	}

	if node.IsStopped() {
		return RequestVoteReply{}, fmt.Errorf("node stopped")
	}

	return node.RequestVote(args), nil
}

func (t *LocalTransport) AppendEntries(peerAddress string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	node, ok := t.nodes[peerAddress]
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("node unreachable")
	}

	if node.IsStopped() {
		return AppendEntriesReply{}, fmt.Errorf("node stopped")
	}

	return node.AppendEntries(args), nil
}
