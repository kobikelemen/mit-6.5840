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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	heartbeat bool
	term 	  int
	state 	  int // 0: follower
			  	  // 1: candidate
			  	  // 2: leader
	votedFor 			 int
	electionTimeoutMin   int64
	electionTimeoutRange int64
	heartbeatTime 		 int64

	commitIndex 		 int
	lastApplied 		 int
	nextIndex 			 []int
	matchIndex 			 []int
	log 				 []LogEntry

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	command interface{}
	term 	int
	index 	int
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	isLeader := false
	if rf.state == 2 {
		isLeader = true
	}
	return rf.term, isLeader
}

func (rf *Raft) GetMyState() int {
	return rf.state
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		int
	CandidateId int

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int 
	VoteGranted bool
}


// - reject if my term is greater than term in args, and reply with my term
// - can only vote once 
// - if my term is less than (or equal?) to term in args, step down as leader
// 		(if applicable), and update term, and vote for server who requested vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	// reject request if my term is ahead 
	DPrintf("S%v, vote request from: %v, for term: %v", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.term {
		DPrintf("S%v, vote request from: %v REJECTED, lower term", rf.me, args.CandidateId)
		return
	}
	// i have already won the election
	if args.Term == rf.term && rf.state == 2 {
		DPrintf("S%v, vote request from: %v REJECTED, I won already", rf.me, args.CandidateId)
		return
	}

	// become follower and update term if my term is behind
	if args.Term > rf.term {
		rf.state = 0
		rf.updateTerm(args.Term)
		DPrintf("S%v, T->%v, becoming follower", rf.me, args.Term)
	}


	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // and candidates log at least up to date with mine
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// rf.heartbeat = true // reset timeout if grant vote
		// 					// no lock needed since locked above
		DPrintf("S%v, vote request from: %v GRANTED", rf.me, args.CandidateId)
		return
	}
	DPrintf("S%v, vote request from: %v REJECTED, already voted", rf.me, args.CandidateId)
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
// look at the comments in ../labrpc/labrpc.gofor more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("S%v, VOTE REQUEST COUDLN'T REACH: %v", rf.me, args.CandidateId)
	}
	return ok
}




type AppendEntriesArgs struct {
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries 	 []LogEntry
}


type AppendEntriesReply struct {
	Term 	int
	Success bool
}


// - reject heartbeat if term in args is lower than my term
// - if their term > my term, update my term, convert to follower
// - if i am candidate, and recieve heartbeat with term equal to 
//	  mine, become a follower (election lost)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("S%v, RECIEVED HEARTBEAT from: %v", rf.me, args.LeaderId)
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		DPrintf("S %v, REJECTED HEARTBEAT from lower T:%v", rf.me, args.Term)
		return
	}

	// TODO: reply false if args.PrevLogIndex exceeds length of rf.log
	// log doesn't contain entry at prevLogIndex with matching term
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.term
		return
	} 
	if len(args.Entries) != 0 {
		// TODO: num 3 from Figure 2
		rf.appendNewEntries(args.Entries)
	}
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex = min(leaderCommit, index of last new entry)
		lastEntryIdx := args.Entries[len(args.Entries) - 1].index
		rf.commitIndex = lastEntryIdx
		if args.LeaderCommit < lastEntryIdx {
			rf.commitIndex = args.LeaderCommit
		}
	}
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.state = 0
		DPrintf("S%v, T->%v from HEARTBEAT", rf.me, args.Term)
	}
	// election lost if i am candidate and recieve heartbeat 
	// with same term so become follower
	if rf.state == 1 && args.Term == rf.term {
		rf.state = 0
		DPrintf("S%v, ELECTION LOST from HEARTBEAT", rf.me)
	}
	rf.heartbeat = true
	reply.Term = rf.term
	reply.Success = true
	DPrintf("S%v, SUCCESS HEARTBEAT", rf.me)
}


// - send heartbeat to all peers
// - if reply contains a rejection because peers term number
//    is greater than my tern num, step down as leader
func (rf *Raft) sendAppendEntries(me, iPeer int, targetPeer *labrpc.ClientEnd, 
								args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("S%v, SENDING HEARTBEAT to: %v", me, iPeer)
	ok := targetPeer.Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("S%v, HEARTBEAT COUDLN'T REACH: %v", me, iPeer)
	}
	return ok
}


func (rf *Raft) appendNewEntries(newEntries []LogEntry) {
	for i := 0; i < len(newEntries); i ++ {
		if !rf.checkInLog(newEntries[i]) {
			rf.log = append(rf.log, newEntries[i])
		}
	}
}


func (rf *Raft) checkInLog(entry LogEntry) bool {
	// TODO: currently assumes all new entries are not 
	//		 not in my log (not generally true)
	return false
}


func (rf *Raft) getAppendEntriesArgs(iPeer int) AppendEntriesArgs {
	entries := make([]LogEntry, 0)
	if len(rf.log) - 1 >= rf.nextIndex[iPeer] {
		copy(entries, rf.log[rf.nextIndex[iPeer]:])
	}
	args := AppendEntriesArgs{
		Term: rf.term, 
		LeaderId: rf.me,
		PrevLogIndex : 	rf.nextIndex[iPeer] - 1,
		PrevLogTerm : rf.log[rf.commitIndex].term,
		Entries : entries,
		LeaderCommit : rf.commitIndex,
	}
	return args
}


func (rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i ++ {
		if i != rf.me && rf.getStateCopy() == 2 {
			go func(rf *Raft, i int) {
				logInconsistency := true
				for logInconsistency {
					logInconsistency = false
					rf.mu.Lock()
					args := rf.getAppendEntriesArgs(i)
					reply := AppendEntriesReply{}
					me := rf.me
					targetPeer := rf.peers[i]
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(me, i, targetPeer, &args, &reply)
					// ignore response if peer un-reachable
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// step down as leader if peer has higher term
					if !reply.Success && reply.Term > rf.term{
						rf.updateTerm(reply.Term)
						rf.state = 0
						DPrintf("S%v, HEARTBEAT to: %v REJECTED, becoming follower ", rf.me, i)
						return
					} else if !reply.Success { // log incosistency 
						rf.nextIndex[i] --
						logInconsistency = true
					} else if reply.Success {
						rf.nextIndex[i] = len(rf.log) 
						rf.matchIndex[i] = len(rf.log)
					}
				}
			} (rf, i)
		}
	}
}


func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majority := int(len(rf.peers) / 2) + 1
	N := rf.commitIndex + 1
	// TODO: check if N exceeds log length
	maxN := rf.commitIndex
	for N < len(rf.log) {
		count := 0
		for iPeer := 0; iPeer < len(rf.peers); iPeer ++ {
			if rf.matchIndex[iPeer] >= N {
				count ++
			}
		}
		if count >= majority && rf.log[N].term == rf.term {
			maxN = N
			N ++
		} else {
			break
		}
	}
	rf.commitIndex = maxN
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	isLeader := rf.state == 2
	if isLeader {
		index = len(rf.log) - 1
		rf.log = append(rf.log, LogEntry{command : command, 
										 term : rf.term, 
										 index : index})
	}
	return index, rf.term, isLeader
}


func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.term ++
	rf.votedFor = rf.me
	rf.state = 1
	votes := 1 // vote for myself
	DPrintf("S%v, started ELECTION, for term: %v", rf.me, rf.term)
	vreqArgs := RequestVoteArgs{
		Term: rf.term, 
		CandidateId: rf.me}	
	rf.mu.Unlock()

	var waitCount int32 = 0
	var won int32 = 0

	for vreq := 0; vreq < len(rf.peers); vreq ++ {
		if vreq != rf.me {
			atomic.AddInt32(&waitCount, 1)

			go func(rf *Raft, waitCount *int32, votes *int, vreq int, 
					vreqArgs *RequestVoteArgs, won *int32) {
				vreqReply := RequestVoteReply{}
				ok := rf.sendRequestVote(vreq, vreqArgs, &vreqReply)
				// only consider response if peer is reachable
				if ok {
					rf.mu.Lock()
					// update term and become follower if peer 
					// has higher term
					if vreqReply.Term > rf.term {
						rf.updateTerm(vreqReply.Term)
						rf.state = 0
					} else if vreqReply.VoteGranted {
						(*votes) ++
						if *votes > len(rf.peers) / 2 {
							atomic.StoreInt32(won, 1)
						}
						DPrintf("S%v, recieved vote from: %v, total: %v", rf.me, vreq, *votes)
					}
					rf.mu.Unlock()
				}
				atomic.AddInt32(waitCount, -1)

			} (rf, &waitCount, &votes, vreq, &vreqArgs, &won)	
		}
	}
	// wait for election to finish because either:
	// 1. recieved replies from all peers
	// 2. won the election by recieving over half the votes
	// 3. lost the election by recieving heartbeat from peer
	//	  with greater than or equal term
	for atomic.LoadInt32(&waitCount) > 0 && atomic.LoadInt32(&won) == 0 {
		if rf.getStateCopy() == 0 {  // election lost
			return
		}
	}
	DPrintf("S%v, voting COMPLETE", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if votes > len(rf.peers) / 2 {
		DPrintf("S%v, WON ELECTION", rf.me)
		rf.state = 2
		return			
	}
	DPrintf("S%v, didn't recieve enough votes, ELECTION LOST", rf.me)
}


func (rf *Raft) leaderLoop() {
	for rf.killed() == false && rf.getStateCopy() == 2 {
		rf.broadcastAppendEntries()
		rf.updateCommitIndex()
		time.Sleep(time.Duration(rf.heartbeatTime) * time.Millisecond)
	}
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

func (rf *Raft) checkHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	res := rf.heartbeat
	return res
}

func (rf *Raft) setHeartbeat(new bool) {
	rf.mu.Lock()
	rf.heartbeat = new
	rf.mu.Unlock()
}

func (rf *Raft) getStateCopy() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	res := rf.state
	return res
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.setHeartbeat(false)
		// pause for a random amount of time between...
		ms := rf.electionTimeoutMin + (rand.Int63() % rf.electionTimeoutRange)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// if heartbeat seen is still false then start an election
		if !rf.checkHeartbeat() {
			rf.startElection()
			if rf.getStateCopy() == 2 {
				rf.initIndexState()
				rf.leaderLoop()
			}
		}
	}
}

func (rf *Raft) updateTerm(newTerm int) {
	rf.term = newTerm
	rf.votedFor = -1
}


func (rf *Raft) initIndexState() {
	rf.matchIndex = nil
	rf.nextIndex = nil
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeat = false
	rf.state = 0
	rf.updateTerm(0)
	rf.electionTimeoutMin = 600
	rf.electionTimeoutRange = 300
	rf.heartbeatTime = 125
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.log = make([]LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
