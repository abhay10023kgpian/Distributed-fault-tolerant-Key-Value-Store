# Raft Election & Decision Tree

This document illustrates the Raft election process and voting logic using a Mermaid decision tree.

## Election Timeout & Candidate State

A follower transitions to a Candidate when its election timer expires without receiving a heartbeat from a Leader or granting a vote to another candidate.

```mermaid
stateDiagram-v2
    [*] --> Follower
    
    Follower --> Candidate: Election Timeout (150ms-300ms)
    Candidate --> Candidate: Election Timeout (Split Vote)
    Candidate --> Leader: Receives votes from majority
    Candidate --> Follower: Discovers higher term or new Leader
    Leader --> Follower: Discovers higher term
    
    Leader --> [*]: Node Killed
    Candidate --> [*]: Node Killed
    Follower --> [*]: Node Killed
    
    [*] --> Follower: Node Restarted
```

## Voting Decision Logic

When a Candidate sends a `RequestVote` RPC, the receiving node evaluates the request based on the Candidate's term and log completeness.

```mermaid
flowchart TD
    Start[Receive RequestVote RPC] --> CheckTerm{Candidate Term < Current Term?}
    CheckTerm -- Yes --> Reject[Reject Vote: Term too old]
    CheckTerm -- No --> UpdateTerm{Candidate Term > Current Term?}
    
    UpdateTerm -- Yes --> StepDown[Update CurrentTerm & Step Down to Follower]
    UpdateTerm -- No --> CheckVotedFor
    StepDown --> CheckVotedFor
    
    CheckVotedFor{Already voted in this term?}
    CheckVotedFor -- Yes, for someone else --> Reject2[Reject Vote: Already Voted]
    CheckVotedFor -- No, or voted for Candidate --> CheckLog
    
    CheckLog{Candidate Log is up-to-date?}
    CheckLog -- "Candidate LastLogTerm < Local LastLogTerm" --> Reject3[Reject Vote: Log out of date]
    CheckLog -- "Terms equal, but Candidate LastLogIndex < Local LastLogIndex" --> Reject3
    CheckLog -- "Candidate Log is >= Local Log" --> Accept[Grant Vote & Reset Election Timer]
```

### Log Completeness Rule
Raft dictates that a node cannot win an election unless its log is *at least as up-to-date* as the majority of the cluster. This is evaluated by:
1. Comparing the term of the last entry in the log.
2. If the terms are equal, comparing the length (index) of the logs.
