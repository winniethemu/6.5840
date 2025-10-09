package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State string

const (
	Candidate State = "Candidate"
	Follower  State = "Follower"
	Leader    State = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan raftapi.ApplyMsg

	// Persistent
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile
	commitIndex   int
	lastApplied   int
	currentState  State
	electionReset time.Time

	// For leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Command any
	Term    int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.currentState == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("becoming follower: peer=%d, term=%d\n", rf.me, term)
	rf.currentState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionReset = time.Now()
	rf.persist()

	go rf.ticker()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.logs) > 0 {
		lastLogIndex = len(rf.logs) - 1
		lastLogTerm = rf.logs[lastLogIndex].Term
	}

	if args.Term < rf.currentTerm {
		DPrintf("stale RequestVote: receiver=%d, term=%d, requester=%d, term=%d\n",
			rf.me,
			rf.currentTerm,
			args.CandidateID,
			args.Term,
		)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("newer RequestVote: receiver=%d, term=%d, requester=%d, term=%d\n",
			rf.me,
			rf.currentTerm,
			args.CandidateID,
			args.Term,
		)
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm ||
			args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		return
	}

	reply.Term = args.Term
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if rf.currentTerm < args.Term {
		DPrintf(
			"term out of date in AppendEntries, becoming follower: peer=%d\n",
			rf.me,
		)
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Candidate finds out that another peer has won the election for this term
	if rf.currentState != Follower {
		rf.becomeFollower(args.Term)
	}
	rf.electionReset = time.Now()

	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	cachedTerm := rf.currentTerm
	prevLogIndex := len(rf.logs) - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func() {
			args := AppendEntriesArgs{
				Term:         cachedTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{},
				LeaderCommit: leaderCommit,
			}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(idx, &args, &reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if cachedTerm < reply.Term {
				DPrintf(
					"term out of date in AppendEntries, becoming follower: peer=%d\n",
					rf.me,
				)
				rf.becomeFollower(reply.Term)
			}
		}()
	}
}

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
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.currentState == Leader

	// Your code here (3B).
	if !isLeader {
		return -1, -1, false
	}

	rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()

			ni := rf.nextIndex[peer]
			prevLogIndex := ni - 1
			prevLogTerm := rf.logs[prevLogIndex].Term
			entries := rf.logs[ni:]

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			DPrintf(
				"sending AppendEntries for agreement: leader=%d, peer=%d, req=%+v",
				rf.me,
				peer,
				args,
			)
			if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// stale response
				if rf.currentState == Leader && reply.Term != args.Term {
					return
				}

				if reply.Success {
					rf.nextIndex[peer] = ni + len(entries)
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1

					// update leader commitIndex if quorum is reached
					for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
						if rf.logs[i].Term != rf.currentTerm {
							break
						}
						matchCount := 1
						for j := range rf.peers {
							if rf.matchIndex[j] >= i {
								matchCount++
							}
						}
						if matchCount*2 > len(rf.peers)+1 {
							rf.commitIndex = i
						}
					}
				} else {
					rf.nextIndex[peer] = ni - 1
				}
			}
		}(idx)
	}

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startLeader() {
	DPrintf("becoming leader: peer=%d, term=%d\n", rf.me, rf.currentTerm)
	rf.currentState = Leader
	rf.votedFor = -1
	rf.persist()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			rf.sendHeartbeat()
			<-ticker.C
			rf.mu.Lock()
			if rf.currentState != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

// expects rf.mu to be locked
func (rf *Raft) startElection() {
	rf.currentState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionReset = time.Now()
	cachedTerm := rf.currentTerm
	DPrintf("becoming candidate: peer=%d, term=%d\n", rf.me, cachedTerm)

	votesReceived := 1

	for idx := range rf.peers {
		go func(peerID int) {
			if idx != rf.me {
				rf.mu.Lock()
				lastLogIndex := -1
				lastLogTerm := -1
				if len(rf.logs) > 0 {
					lastLogIndex = len(rf.logs) - 1
					lastLogTerm = rf.logs[lastLogIndex].Term
				}
				rf.mu.Unlock()
				args := RequestVoteArgs{
					Term:         cachedTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(idx, &args, &reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentState != Candidate {
					DPrintf(
						"candidate state change while waiting for reply: peer=%d, currentState=%v\n",
						rf.me,
						rf.currentState,
					)
					return
				}
				if rf.currentTerm < reply.Term {
					DPrintf("term out of date in RequestVoteReply, becoming follower: peer=%d\n", rf.me)
					rf.becomeFollower(reply.Term)
				} else if rf.currentTerm == reply.Term {
					if reply.VoteGranted {
						votesReceived++
						if votesReceived > len(rf.peers)/2 {
							DPrintf(
								"winning election: peer=%d, term=%d, votesReceived=%d\n",
								rf.me,
								rf.currentTerm,
								votesReceived,
							)
							rf.startLeader()
							return
						}
					}
				}
			}
		}(idx)
	}

	// Run another election timer in case this election was unsuccessful
	go rf.ticker()
}

func (rf *Raft) ticker() {
	// Generate random election timeout between 200-500ms
	ms := 200 + (rand.Int63() % 300)
	electionTimeout := time.Duration(ms) * time.Millisecond

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.currentState == Leader {
			DPrintf("bailing out of election timer, currentState=%v\n", rf.currentState)
			rf.mu.Unlock()
			return
		}

		// Sleep for a short interval before checking again
		time.Sleep(10 * time.Millisecond)

		// Check if election timeout has occurred
		if elapsed := time.Since(rf.electionReset); elapsed >= electionTimeout {
			DPrintf("election timeout occurred: peer=%d, elapsed=%v, timeout=%v\n",
				rf.me, elapsed, electionTimeout)
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.logs) {
			rf.lastApplied++
			entry := rf.logs[rf.lastApplied]
			message := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- message
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 1
	rf.currentState = Follower
	rf.votedFor = -1
	rf.electionReset = time.Now()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// Raft log is 1-indexed, but we view it as 0-indexed. This allows the
	// very first AppendEntries to contain 0 as `PrevLogIndex`, and be a
	// valid index into the log.
	rf.logs = []LogEntry{
		{Command: nil, Term: 0},
	}

	// for each server, index of the next log entry to send to that server
	// initialized to leader last log index + 1
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// for each server, index of highest log entry known to be replicated
	// on server (initialized to 0)
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
