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
	ElectionTime    = 550
	HeartbeatTime   = 125
	Spread          = 400
	IdleForElection = 30
	IdleForHearbeat = 60
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
	mu        sync.Mutex          // Lock to protect shared access to this
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	state       State
	votedFor    int
	timerStart  time.Time
	waitTime    time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh    chan ApplyMsg
	sendAE     bool
	lastSentAE time.Time
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term    int
	Granted bool
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

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
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

func GenerateSeed() *rand.Rand {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1
}

func (rf *Raft) BecomeFollower(term, votedFor int, resetTime bool) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	if resetTime {
		r1 := GenerateSeed()
		rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
		rf.timerStart = time.Now()
	}
	// DPrintf("TIME for (%d), waitTime - %v, timerStart - %v", rf.me, rf.waitTime, rf.timerStart)
}

func (rf *Raft) BecomeCandidate() {
	r1 := GenerateSeed()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
	rf.timerStart = time.Now()
}

func (rf *Raft) BecomeLeader() {
	DPrintf("[%d] server became the Leader", rf.me)
	rf.state = Leader

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.timerStart = time.Now()
	rf.sendAE = true
	rf.lastSentAE = time.Now()
	// start heartbeats
	go rf.HeartbeatTicker()
}

func (rf *Raft) HeartbeatTicker() {
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.sendAE || time.Since(rf.lastSentAE) > HeartbeatTime {
			rf.sendAE = false
			rf.lastSentAE = time.Now()
			go rf.SendHeartbeats()
		}
		rf.mu.Unlock()
		time.Sleep(IdleForHearbeat * time.Millisecond)
	}
}

func (rf *Raft) GetPrevLog(server, term int) AppendEntryArgs {
	entries := make([]LogEntry, 0)
	if rf.matchIndex[server]+1 == rf.nextIndex[server] && rf.nextIndex[server] <= rf.log[len(rf.log)-1].Index {
		entries = append(entries, rf.log[rf.nextIndex[server]:]...)
	}

	args := AppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[rf.nextIndex[server]-1].Index,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	return args
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
			rf.CallAppendEntry(server, term)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.commitIndex == rf.log[len(rf.log)-1].Index {
				return
			}

			idx := 0
			for i := range rf.matchIndex {
				cnt := 0
				if i == rf.me {
					continue
				}
				for j := range rf.matchIndex {
					if rf.me != j && rf.matchIndex[i] == rf.matchIndex[j] {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					idx = rf.matchIndex[i]
					break
				} else if cnt == len(rf.peers)/2 && rf.matchIndex[i] > idx {
					idx = rf.matchIndex[i]
				}
			}

			commited := false
			if idx > rf.commitIndex {
				rf.commitIndex = idx
				commited = true
			}

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.lastApplied,
					Command:      rf.log[rf.lastApplied].Command,
				}
				// DPrintf("COMMITED: sending %+v msg to tester", applyMsg)
				rf.applyCh <- applyMsg
			}
			if commited {
				rf.sendAE = true
			}

		}(server)
	}
}

func (rf *Raft) CallAppendEntry(server, term int) {
	var reply AppendEntryReply
	rf.mu.Lock()
	args := rf.GetPrevLog(server, term)
	if len(args.Entries) != 0 {
		DPrintf("[%d] sending AE to %d, with args = %+v", rf.me, server, args)
		DPrintf("me - %d, log - %v, commitIndex - %d, lastApplied - %d, nextIndex %+v, matchIndex %+v", rf.me, rf.log, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	} else {
		DPrintf("[%d] sending AE to %d with Term %d", rf.me, server, term)
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntry(server, &args, &reply)

	if !ok {
		rf.matchIndex[server] = 0
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term, -1, false)
	} else if reply.Term > term {
		return
	} else if !reply.Success {
		if reply.XTerm == -1 && reply.XIndex != -1 {
			rf.nextIndex[server] = reply.XIndex + 1
		} else if reply.XTerm != -1 {
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = rf.log[i].Index
					break
				} else if rf.log[i].Term < reply.XTerm {
					break
				}
			}
			rf.nextIndex[server] = reply.XIndex + 1
		} else {
			DPrintf("{%d} in this clause with term - %d, with %d, XTerm - %d, XIndex - %d, Success - %v", rf.me, term, server, reply.XTerm, reply.XIndex, reply.Success)
			rf.nextIndex[server] -= 1
		}
	} else {
		// DPrintf("[%d] my command %v was recorded by %d", rf.me, args.Entry.Command, server)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(%d) Recieved Append Entry from [%d]", rf.me, args.LeaderId)
	// DPrintf("[%d] Recieved Append Entry from (%d), args is %v", rf.me, args.LeaderId, args)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	if args.Term < rf.currentTerm {
		return
	}

	// Figure2 2
	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex {
		rf.BecomeFollower(args.Term, rf.votedFor, true)
		reply.XIndex = rf.log[len(rf.log)-1].Index
		return
	}

	// Figure2 3
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("(%d) rf.log[args.PrevLogIndex].Term != args.PrevLogTerm, PrevLogIndex  = %d", rf.me, args.PrevLogIndex)
		i := len(rf.log) - 1
		for rf.log[i].Term == rf.log[i-1].Term {
			i -= 1
		}
		reply.XTerm = rf.log[i].Term
		reply.XIndex = rf.log[i].Index

		rf.BecomeFollower(args.Term, rf.votedFor, true)
		return
	}

	j := 0
	if len(args.Entries) != 0 && args.Entries[0].Index > 0 {
		for i := args.Entries[0].Index; i < len(rf.log); i++ {
			//Packet delay
			if rf.log[i].Term != args.Entries[j].Term {
				rf.log = rf.log[:i]
				break
			}
			rf.log[i] = args.Entries[j]
			j += 1
		}
	}

	if len(args.Entries[j:]) != 0 {
		rf.log = append(rf.log, args.Entries[j:]...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex)
	}

	for rf.commitIndex > rf.lastApplied {
		DPrintf("[%d] about to apply, prevLogIndex - %d, prevLogTerm - %d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}

	DPrintf("(%d) recieved AE from [%d] and now my log is %v and commitIndex is %d ", rf.me, args.LeaderId, rf.log, rf.commitIndex)
	reply.Success = true
	rf.BecomeFollower(args.Term, rf.votedFor, true)
}

func (rf *Raft) ElectionTicker() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.timerStart) > rf.waitTime {
			// DPrintf("START ELECTION for %d, time now is %v", rf.me, time.Now())
			go rf.AttempElection()
			r1 := GenerateSeed()
			rf.waitTime = time.Duration(ElectionTime+r1.Intn(Spread)) * time.Millisecond
			rf.timerStart = time.Now()
		}
		rf.mu.Unlock()
		time.Sleep(IdleForElection * time.Millisecond)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(%d) recieved RequestVote from [%d] with the Term of -- %d and my Term %d, and I'have voted for %d", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	reply.Granted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) || args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm {
			// DPrintf("[%d] i voted for %d", rf.me, args.CandidateId)
			rf.BecomeFollower(args.Term, args.CandidateId, true)
			reply.Granted = true
			reply.Term = args.Term
			return
		}
	}
	if args.Term > rf.currentTerm {
		DPrintf("[%d] i didn't vote for %d - second if statement", rf.me, args.CandidateId)
		reply.Term = args.Term
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		return
	}
	// DPrintf("[%d] i didn't vote for %d", rf.me, args.CandidateId)

}

func (rf *Raft) AttempElection() {
	rf.mu.Lock()
	rf.BecomeCandidate()
	count := 1
	term := rf.currentTerm
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			DPrintf("[%d] requests vote from - %d with the Term of -- %d", rf.me, server, term)
			// DPrintf("[%d] requests vote from - %d", rf.me, server)
			voteGranted := rf.CallRequestVote(server)
			if !voteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Candidate {
				return
			}
			DPrintf("[%d] recieved vote from - %d", rf.me, server)
			count++
			if count >= len(rf.peers)/2+1 {
				rf.BecomeLeader()
			}

		}(server)
	}

}

func (rf *Raft) CallRequestVote(server int) bool {
	var reply RequestVoteReply
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

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
		rf.BecomeFollower(reply.Term, -1, false)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}

	DPrintf("START - [%d] recieved command %+v", rf.me, command)
	term = rf.currentTerm
	index = rf.log[len(rf.log)-1].Index + 1

	logEntry := LogEntry{
		Index:   index,
		Command: command,
		Term:    term,
	}

	rf.log = append(rf.log, logEntry)
	rf.sendAE = true
	// DPrintf("start return values %d, %d, %v", index, term, isLeader)
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

	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.sendAE = false

	// Your initialization code here (2A, 2B, 2C).
	rf.BecomeCandidate()
	go rf.ElectionTicker()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
