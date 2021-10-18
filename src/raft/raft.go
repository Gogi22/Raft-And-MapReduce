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

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	ElectionTime  = 1300
	HeartbeatTime = 130
	Spread        = 700
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	state       State
	votedFor    int
	timerStart  time.Time
	waitTime    time.Duration
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

func GenerateSeed() *rand.Rand {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1
}

func (rf *Raft) BecomeFollower(term int) {
	r1 := GenerateSeed()
	rf.state = Follower
	rf.currentTerm = term
	rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
	rf.timerStart = time.Now()
	rf.votedFor = -1
}

func (rf *Raft) BecomeCandidate() {
	r1 := GenerateSeed()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
	rf.timerStart = time.Now()

}

func (rf *Raft) BecomeLeader() {
	DPrintf("[%d] server became the Leader", rf.me)
	rf.state = Leader
	rf.timerStart = time.Now()
	// start heartbeats
	go rf.HeartbeatTicker()
}

func (rf *Raft) HeartbeatTicker() {
	for {
		rf.mu.Lock()
		if rf.state != Leader && rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.SendHeartbeats()
		time.Sleep(HeartbeatTime * time.Millisecond)

	}
}

func (rf *Raft) SendHeartbeats() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			DPrintf("[%d] sending AE to %d with Term %d", rf.me, server, term)
			rf.CallAppendEntry(server, term, rf.me)

		}(server)
	}
}

func (rf *Raft) CallAppendEntry(server int, term int, leader int) {
	args := AppendEntryArgs{
		Term:     term,
		LeaderId: leader,
	}
	var reply AppendEntryReply

	ok := rf.sendAppendEntry(server, &args, &reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] Recieved Append Entry from (%d)", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Success = true
	rf.BecomeFollower(args.Term)
}

func (rf *Raft) ElectionTicker() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.timerStart) > rf.waitTime {
			go rf.AttempElection()
			r1 := GenerateSeed()
			rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
			rf.timerStart = time.Now()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] recieved RequestVote from (%d) with the Term of -- %d and my Term %d, and I'have voted for %d", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	if args.Term < rf.currentTerm {
		reply.Granted = false
		reply.Term = rf.currentTerm
		return
	}

	// Not Sure about third comparison
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		reply.Granted = true
		reply.Term = args.Term
	}

}

func (rf *Raft) AttempElection() {
	rf.mu.Lock()
	rf.BecomeCandidate()
	term := rf.currentTerm
	count := 1
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			DPrintf("[%d] requests vote from - %d with the Term of -- %d", rf.me, server, term)
			voteGranted := rf.CallRequestVote(server, term)
			if !voteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("[%d] recieved vote from - %d", rf.me, server)
			count += 1
			if rf.state != Candidate {
				return
			}
			if count >= len(rf.peers)/2+1 {
				rf.BecomeLeader()
			}

		}(server)
	}

}

func (rf *Raft) CallRequestVote(server int, term int) bool {
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok {
		return false
	}

	if reply.Granted {
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
	}

	return false
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("About to Send a RPC Call for RequestVote to server %d from server %d", server, args.CandidateId)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Sent a RPC Call for RequestVote to server %d from server %d and recieved %v", server, args.CandidateId, ok)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	DPrintf("Created Peer %d", me)
	r1 := GenerateSeed()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
	rf.timerStart = time.Now()
	rf.state = Candidate
	// Your initialization code here (2A, 2B, 2C).
	go rf.ElectionTicker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
