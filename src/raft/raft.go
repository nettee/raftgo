package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"log"
	"math/rand"
	"time"
	"fmt"
)

// import "bytes"
// import "encoding/gob"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Dead
)

func (role Role) String() string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "UnknownRole"
	}
}


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index int // The position of this entry in log
	Term int // The term of leader when the command is received
	Command interface{} // The command from client
}

func (le *LogEntry) String() string {
	return fmt.Sprintf("<I.%d,T%d,%v>", le.Index, le.Term, le.Command)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role // Follower, Candidate or Leader
	dead chan bool

	// Persistent states on all servers:
	currentTerm int // latest Term server has seen
	votedFor int // CandidateId that received vote in current term (or -1 if none)
	log []LogEntry // Log entries (first index is 1)

	// Volatile states during leader election
	votes         int       // Number of votes received, only for Candidate
	majorityVotes chan bool // Signals that the candidate receives majority votes
	winsElection bool
	receivedHeartbeat chan bool // Signals that the follower receives a heartbeat
	grantingVote chan bool // Signals that the follower is granting votes to candidate

	// Volatile states on leaders
	nextIndex []int // nextIndex[i] - index of the next log entry to send to server[i]

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.role == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int // candidate's Term
	CandidateId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term int // Leader's term
	LeaderId int // Leader's ID
	PrevLogIndex int // Index of log entry immediately preceding new ones
	PrevLogTerm int // Term of PrevLogIndex entry
	Entries []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var voteGranted bool

	// (S5.1-P3) If one server’s current term is smaller than the other’s, then
	// it updates its current term to the larger value.
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		log.Printf("[%d] updates its term = %d according to [%d]", rf.me, rf.currentTerm, args.CandidateId)
		rf.votedFor = args.CandidateId
		log.Printf("[%d] votes for [%d]", rf.me, args.CandidateId)
		voteGranted = true

		// If a server receives a request with a stale term number,
		// it rejects the request.
	} else if rf.currentTerm > args.Term {
		log.Printf("[%d] rejected [%d]", rf.me, args.CandidateId)
		voteGranted = false

	} else if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		log.Printf("[%d] votes for [%d]", rf.me, args.CandidateId)
		voteGranted = true
	} else {
		log.Printf("[%d] already voted another server", rf.me)
		voteGranted = false
	}

	if voteGranted {
		rf.grantingVote <- true
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Note: currentTerm will be modified later.
	reply.Term = rf.currentTerm

	// If a server receives a request with a stale term number,
	// it rejects the request.
	if rf.currentTerm > args.Term {
		log.Printf("[%d] rejected [%d]", rf.me, args.LeaderId)
		return
	}

	// (S5.1-P3) If one server’s current term is smaller than the other’s, then
	// it updates its current term to the larger value.
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		log.Printf("[%d] updates its term = %d according to [%d]", rf.me, rf.currentTerm, args.LeaderId)
	}

	rf.receivedHeartbeat <- true

	// TODO when args.PrevLogIndex > rf.getLastLogEntry().Index

	// TODO check log term matching

	rf.log = rf.log[:args.PrevLogIndex+1] // Truncate rf.log to log[0, prevLogIndex]
	rf.log = append(rf.log, args.Entries...) // `...' means appending all entries
	if len(args.Entries) > 0 {
		log.Printf("[%d] append %d log entries, last log (I.%d)", rf.me, len(args.Entries), rf.getLastLogEntry().Index)
	}
	reply.Success = true
	reply.NextIndex = rf.getLastLogEntry().Index + 1

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	log.Printf("[%d]->[%d] SEND RequestVote RPC (T%d)", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		log.Printf("[%d]<-[%d] RECEIVE RequestVote RPC Reply, voteGranted = %v", rf.me, server, reply.VoteGranted)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	isEmptyHeartbeat := args.Entries == nil || len(args.Entries) == 0
	if isEmptyHeartbeat {
		log.Printf("[%d]->[%d] SEND heartbeat (T%d)", rf.me, server, args.Term)
	} else {
		log.Printf("[%d]->[%d] SEND nonempty heartbeat, prevLog = (I.%d,T%d), entries = (I.%d)~(I.%d)",
			rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if !isEmptyHeartbeat {
			log.Printf("[%d]<-[%d] RECEIVE nonempty heartbeat reply, success = %v", rf.me, server, reply.Success)
			if reply.Success {
				lastNextIndex := rf.nextIndex[server]
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				log.Printf("[%d].nextIndex[%d]: %d->%d", rf.me, server, lastNextIndex, rf.nextIndex[server])
			}
		}
	}
	return ok
}

func (rf *Raft) getLastLogEntry() LogEntry {
	i := len(rf.log) - 1
	return rf.log[i]
}

func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	return rf.log[logIndex]
}

func (rf *Raft) getLogEntriesToSend(nextIndex int) []LogEntry {
	return rf.log[nextIndex:]
}

func (rf *Raft) electionTimeout() time.Duration {
	rand.Seed(int64(rf.me + time.Now().Nanosecond()))
	// Election timeout: 500~800ms
	timeout := 500 + rand.Intn(300)
	//log.Printf("[%d]'s election timeout = %d ms", rf.me, timeout)
	return time.Duration(timeout) * time.Millisecond
}

func (rf *Raft) heartbeatTimeout() time.Duration {
	return time.Duration(150) * time.Millisecond
}

func (rf *Raft) increaseTerm() {

	oldTerm := rf.currentTerm
	rf.currentTerm++
	log.Printf("[%d] increase term: %d->%d", rf.me, oldTerm, rf.currentTerm)
}

func (rf *Raft) roleTransition(newRole Role) {

	oldRole := rf.role
	rf.role = newRole
	log.Printf("[%d] %s->%s", rf.me, oldRole, newRole)

	if oldRole == Candidate {
		rf.winsElection = false // Reset variable
	}
}

func (rf *Raft) runAsFollower() {

	// (Figure2) Followers:
	// If election timeout elapses without receiving AppendEntries RPC from
	// current leader or granting vote to candidate: convert to candidate.

	timeout := time.After(rf.electionTimeout())

	if rf.role != Follower {
		return
	}

	select {
	case <- rf.receivedHeartbeat:
		//log.Printf("Follower [%d] received heartbeat, remains follower", rf.me)
	case <- rf.grantingVote:
		log.Printf("Follower [%d] is granting vote, remains follower", rf.me)
	case <- timeout:
		log.Printf("Follower [%d] election timeout expired", rf.me)
		// (S5.2-P1) If a follower receives no communication over election timeout,
		// then is begins an election to choose a new leader.
		// (S5.2-P2) To begin an election, a follower increments its current term
		// and transitions to candidate state.
		rf.roleTransition(Candidate)
		return
	case <- rf.dead:
		return
	}
}

func (rf *Raft) runAsCandidate() {

	// (Figure2) Candidates:
	// On conversion to candidate, start election:
	//   1. Increment currentTerm
	//   2. Vote for self
	//   3. Reset election timer
	//   4. Send RequestVote RPCs to all other servers

	// 1. Increment the current term
	rf.increaseTerm()

	// 2. Vote for itself
	rf.votedFor = rf.me
	rf.votes = 1

	// 3. Reset election timer
	timeout := time.After(rf.electionTimeout())

	// 4. Send RequestVote RPCs to all other servers
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, args, &reply)
			if reply.VoteGranted {
				rf.mu.Lock()
				rf.votes++
				if rf.role == Candidate && !rf.winsElection && rf.votes > len(rf.peers)/2 {
					// This candidate has received votes from majority of servers
					// Send signal that it wins an election
					rf.winsElection = true
					rf.majorityVotes <- true
				}
				rf.mu.Unlock()
			}
		} (serverId)
	}

	if rf.role != Candidate {
		return
	}

	// (Figure2) Candidates:
	// If votes received from majority of servers: become leader
	// If AppendEntries RPC (heartbeat) received from new leader: convert to follower
	// If election timeout elapses: start new election

	select {
	case <- rf.majorityVotes:
		log.Printf("Candidate [%d] wins an election", rf.me)
		rf.roleTransition(Leader)
		// Initialize states for leaders
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
		}
		return
	case <- rf.receivedHeartbeat:
		log.Printf("Candidate [%d] received heartbeat from new leader", rf.me)
		rf.roleTransition(Follower)
		return
	case <- timeout:
		log.Printf("Candidate [%d] election timeout elapses", rf.me)
		// Start new election
		rf.roleTransition(Candidate)
		return
	case <- rf.dead:
		return
	}
}

func (rf *Raft) runAsLeader() {

	rf.mu.Lock()
	rf.mu.Unlock()

	timeout := time.After(rf.heartbeatTimeout())

	// Repeat sending initial empty AppendEntries RPCs (heartbeats) to each server
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
			args.Entries = make([]LogEntry, rf.getLastLogEntry().Index - args.PrevLogIndex)
			copy(args.Entries, rf.getLogEntriesToSend(rf.nextIndex[i]))

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(i, args, &reply)
		} (serverId)
	}

	if rf.role != Leader {
		return
	}

	select {
	case <- rf.receivedHeartbeat:
		log.Printf("Leader [%d] received heartbeat from new leader", rf.me)
		rf.roleTransition(Follower)
	case <- timeout:
		return
	case <- rf.dead:
		return
	}
}

func (rf *Raft) run() {

	for {
		switch rf.role {
		case Follower:
			rf.runAsFollower()
		case Candidate:
			rf.runAsCandidate()
		case Leader:
			rf.runAsLeader()
		case Dead:
			// do nothing
		default:
			log.Fatalf("Invalid rf.role %v", rf.role)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here.

	// (Figure2-State) initialized to 0 on first boot
	rf.currentTerm = 0

	// Vote for nobody
	rf.votedFor = -1

	// Add a dummy log entry to ensure the index of real log entries start from 1
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil})


	// (S5.2-P1) When servers start up, they begin as followers.
	rf.role = Follower
	log.Printf("[%d] :%s, term = %d", rf.me, rf.role, rf.currentTerm)
	rf.dead = make(chan bool, len(rf.peers))

	rf.receivedHeartbeat = make(chan bool, len(rf.peers))
	rf.grantingVote = make(chan bool, len(rf.peers))
	rf.majorityVotes = make(chan bool, len(rf.peers)) // The buffer size should be large enough
	rf.winsElection = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	term = rf.currentTerm
	isLeader = rf.role == Leader

	if isLeader {

		// (Figure2) Leaders:
		// If command received from client: append entry to log,
		// respond after entry applied to state machine.

		log.Printf("================== COMMAND received from client: {%d}", command)

		index = rf.getLastLogEntry().Index + 1

		logEntry := LogEntry{Index: index, Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, logEntry)

		log.Printf("append new log entry: %s", logEntry.String())
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	log.Printf("Kill [%d]", rf.me)
	rf.role = Dead
	rf.dead <- true
}
