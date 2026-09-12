package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HTTPTransport struct {
	client *http.Client
}

func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (t *HTTPTransport) RequestVote(peerAddress string, args RequestVoteArgs) (RequestVoteReply, error) {
	url := peerAddress + "/raft/request-vote"
	body, err := json.Marshal(args)
	if err != nil {
		return RequestVoteReply{}, err
	}

	resp, err := t.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return RequestVoteReply{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return RequestVoteReply{}, fmt.Errorf("bad status: %d", resp.StatusCode)
	}

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return RequestVoteReply{}, err
	}

	return reply, nil
}

func (t *HTTPTransport) AppendEntries(peerAddress string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	url := peerAddress + "/raft/append-entries"
	body, err := json.Marshal(args)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	resp, err := t.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return AppendEntriesReply{}, fmt.Errorf("bad status: %d", resp.StatusCode)
	}

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return AppendEntriesReply{}, err
	}

	return reply, nil
}
