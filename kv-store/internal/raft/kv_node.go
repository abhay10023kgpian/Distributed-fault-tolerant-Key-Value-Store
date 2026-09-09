package raft

import "kv-store/internal/store"

type KVNode struct {
	Raft  *RaftNode
	Store *store.Store
}

func NewKVNode(raftNode *RaftNode, s *store.Store) *KVNode {
	raftNode.SetApplyFunc(func(cmd Command) {
		switch cmd.Type {
		case CommandSet:
			s.ApplyPut(cmd.Key, cmd.Value)

		case CommandDelete:
			s.ApplyDelete(cmd.Key)
		}
	})

	return &KVNode{
		Raft:  raftNode,
		Store: s,
	}
}

func (n *KVNode) Set(key, value string) (bool, int, uint64) {
	index, _, ok := n.Raft.Start(Command{
		Type:  CommandSet,
		Key:   key,
		Value: value,
	})

	if !ok {
		return false, n.Raft.LeaderID(), 0
	}

	return true, n.Raft.LeaderID(), index
}

func (n *KVNode) Get(key string) (string, bool) {
	return n.Store.Get(key)
}

func (n *KVNode) Delete(key string) (bool, int, uint64) {
	index, _, ok := n.Raft.Start(Command{
		Type: CommandDelete,
		Key:  key,
	})
	if !ok {
		return false, n.Raft.LeaderID(), 0
	}

	return true, n.Raft.LeaderID(), index
}