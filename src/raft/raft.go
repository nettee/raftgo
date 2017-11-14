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
)

// import "bytes"
// import "encoding/gob"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (role Role) String() string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	currentTerm int // latest Term server has seen
	votedFor int // CandidateId that received vote in current Term (or -1 if none)
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// (S5.1-P3) If one server’s current term is smaller than the other’s, then
	// it updates its current term to the larger value.
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		log.Printf("[%d] updates its term = %d according to [%d]", rf.me, rf.currentTerm, args.CandidateId)
		rf.votedFor = args.CandidateId
		log.Printf("[%d] votes for [%d]", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	// If a server receives a request with a stale term number,
	// it rejects the request.
	} else if rf.currentTerm > args.Term {
		log.Printf("[%d] rejected [%d]", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		log.Printf("[%d] votes for [%d]", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		log.Printf("[%d] already voted another server", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	log.Printf("Kill Raft[%d]", rf.me)
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

	// Your initialization code here.

	// (Figure2-State) initialized to 0 on first boot
	rf.currentTerm = 0

	rf.votedFor = -1

	// (S5.2-P1) When servers start up, they begin as followers.
	rf.role = Follower
	log.Printf("[%d] :%s", rf.me, rf.role)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go run(rf)

	return rf
}

func run(rf *Raft) {

	for {
		switch rf.role {
		case Follower:
			runAsFollower(rf)
		case Candidate:
			runAsCandidate(rf)
		case Leader:
			runAsLeader(rf)
		default:
			log.Fatalf("Invalid rf.role %v", rf.role)
		}
	}
}

func runAsFollower(rf *Raft) {
	rand.Seed(int64(rf.me + time.Now().Nanosecond()))
	// Election timeout: 500~800 ms
	electionTimeout := 500 + rand.Intn(300)
	log.Printf("[%d]'s election timeout = %d ms", rf.me, electionTimeout)
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	log.Printf("[%d] is election timeout", rf.me)

	if rf.votedFor != -1 {
		log.Printf("[%d] already voted for [%d], quit", rf.me, rf.votedFor)
		roleTransition(rf, Follower)
		return
	}

	// (S5.2-P1) If a follower receives no communication over run timeout,
	// then is begins an run to choose a new leader.

	// (S5.2-P2) To begin an run, a follower increments its current Term
	// and transitions to candidate state.
	rf.currentTerm++
	log.Printf("[%d] increase term = %d", rf.me, rf.currentTerm)
	roleTransition(rf, Candidate)
	return
}

func runAsCandidate(rf *Raft) {

	voteCount := 0
	maxCount := len(rf.peers)

	rf.votedFor = rf.me // Vote for itself
	voteCount++

	for serverId := 0; serverId < maxCount; serverId++ {
		if serverId == rf.me {
			continue
		}
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
		reply := RequestVoteReply{}
		log.Printf("[%d]->[%d] SEND RequestVote RPC, term = %d", rf.me, serverId, rf.currentTerm)
		rf.sendRequestVote(serverId, args, &reply)
		log.Printf("[%d]<-[%d] RECEIVE RequestVote RPC Reply, voteGranted = %v", rf.me, serverId, reply.VoteGranted)
		if reply.VoteGranted {
			voteCount++
		}
	}

	majority := voteCount > maxCount / 2
	log.Printf("[%d] collects %d/%d votes, majority = %v", rf.me, voteCount, maxCount, majority)

	if majority {
		roleTransition(rf, Leader)
		return
	}
}

func runAsLeader(rf *Raft) {
	//
}

func roleTransition(rf *Raft, newRole Role) {
	oldRole := rf.role
	rf.role = newRole
	log.Printf("[%d] %s->%s", rf.me, oldRole, newRole)
}
