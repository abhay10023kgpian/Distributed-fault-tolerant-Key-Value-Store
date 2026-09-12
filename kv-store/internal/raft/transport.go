package raft

type Transport interface {
	RequestVote(peerAddress string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peerAddress string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
