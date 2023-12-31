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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
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
	applyCh   chan ApplyMsg
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

	snapshotLogLen 		 int
	snapshotTerm		 int
	snapshotIndex		 int

}


type LogEntry struct {
	Command interface{}
	Term 	int
	Index 	int
}


// IMPORTANT: first index is 1
func (rf *Raft) accessLog(index int) *LogEntry {
	DPrintf("S%v, acessLog(), snapshotLogLen:%v, index:%v, logLen:%v", 
			rf.me, rf.snapshotLogLen, index, rf.lenLog())
	 if index == 0 || index > rf.lenLog() || rf.snapshotLogLen > index -  1 {
		DPrintf("S%v, accessLog returning nil", rf.me)
		return nil
	 }
	 return &rf.log[index - 1 - rf.snapshotLogLen]
}


func (rf *Raft) accessLogTerm(index int) int {
	DPrintf("S%v, accessLogTerm()", rf.me)
	if index == 0 || index > rf.lenLog() {
		return -1
	}
	if index == rf.snapshotLogLen {
		return rf.snapshotTerm
	}
	return rf.accessLog(index).Term
}


func (rf *Raft) printLog() {
	DPrintf("S%v, LOG:", rf.me)
	fmt.Printf("			")
	for i := rf.snapshotLogLen + 1; i <= rf.lenLog(); i ++ {
		logEntry := rf.accessLog(i)
		fmt.Printf("IDX:%v T:%v CMD:%v   ", 
				logEntry.Index, logEntry.Term, logEntry.Command)
	}
	fmt.Printf("\n")
}


func (rf *Raft) lenLog() int {
	return len(rf.log) + rf.snapshotLogLen
}


func (rf *Raft) popLog() {
	if len(rf.log) == 0 {
		return
	}
	rf.log = rf.log[:len(rf.log)-1]
	rf.persist(rf.persister.ReadSnapshot())
}


func (rf *Raft) appendLog(logEntry LogEntry) {
	rf.log = append(rf.log, logEntry)
	rf.persist(rf.persister.ReadSnapshot())
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
func (rf *Raft) persist(snapshot []byte) {
	DPrintf("S%v, PERSISTING STATE, T:%v votedFor:%v", 
				rf.me, rf.term, rf.votedFor)
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(rf.term)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("S%v, ATTEMPING RESTORING STATE, data len:%v", rf.me, len(data))
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry = make([]LogEntry, 0)
	if d.Decode(&term) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
		DPrintf("S%v, FAILED TO RESTORE PREVIOUS STATE", rf.me)
		return
	} else {
		rf.term = term
		rf.votedFor = votedFor
		for i := 0; i < len(log); i ++ {
			rf.appendLog(log[i])
		}
		DPrintf("S%v, SUCCESFULLY RESTORED STATE, T:%v votedFor:%v, log len:%v",
					rf.me, rf.term, rf.votedFor, len(log))
		// rf.printLog()
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	DPrintf("S%v, Snapshot(), index:%v", rf.me, index)
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.accessLogTerm(index)
	reduceBy := index - rf.snapshotLogLen
	rf.snapshotLogLen += index
	rf.log = rf.log[reduceBy:]
	rf.snapshotLogLen = index
	rf.persist(snapshot)
	rf.mu.Unlock()
}



// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term 		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}


// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term 		int 
	VoteGranted bool
}


// - reject if my term is greater than term in args, and reply with my term
// - can only vote once 
// - if my term is less than (or equal?) to term in args, step down as leader
// 		(if applicable), and update term, and vote for server who requested vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	// reject request if my term is ahead 
	DPrintf("S%v, vote request from: %v, for term: %v", 
			rf.me, args.CandidateId, args.Term)
	if args.Term < rf.term {
		DPrintf("S%v, vote request from: %v REJECTED, lower term", 
				rf.me, args.CandidateId)
		return
	}
	// i have already won the election
	if args.Term == rf.term && rf.state == 2 {
		DPrintf("S%v, vote request from: %v REJECTED, I won already", 
				rf.me, args.CandidateId)
		return
	}
	
	// become follower and update term if my term is behind
	if args.Term > rf.term {
		rf.state = 0
		rf.updateTerm(args.Term)
		DPrintf("S%v, T->%v, becoming follower", rf.me, args.Term)
	}

	if !rf.isCandUpToDate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("S%v, vote request from:%v REJECTED, candidate NOT up-to-date", 
				rf.me, args.CandidateId)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist(rf.persister.ReadSnapshot())
		DPrintf("S%v, vote request from: %v GRANTED", 
				rf.me, args.CandidateId)
		return
	}
	DPrintf("S%v, vote request from: %v REJECTED, already voted", 
			rf.me, args.CandidateId)
}


func (rf *Raft) isCandUpToDate(candLastLogIndex, candLastLogTerm int) bool {
	lastLogTerm := 0
	if rf.lenLog() > 0 {
		lastLogTerm = rf.accessLogTerm(rf.lenLog())
	}
	DPrintf("S%v, isCandUpToDate(), cand T:%v cand I:%v my T:%v my I:%v", 
			rf.me, candLastLogTerm, candLastLogIndex, lastLogTerm, rf.lenLog())
	if candLastLogTerm != lastLogTerm {
		return candLastLogTerm >= lastLogTerm
	}
	return candLastLogIndex >= rf.lenLog()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, 
								reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("S%v, VOTE REQUEST COUDLN'T REACH: %v", rf.me, args.CandidateId)
	}
	return ok
}


type InstallSnapshotArgs struct {
	Term 			  int
	LeaderId 		  int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data 			  []byte
}


type InstallSnapshotReply struct {
	Term int
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, 
							reply *InstallSnapshotReply) {
	DPrintf("S%v, RECV INSTALL SNAPSHOT", rf.me)
	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}
	DPrintf("S%v, recv data len:%v", rf.me, len(args.Data))
	rf.snapshotTerm = args.LastIncludedTerm 
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotLogLen = args.LastIncludedIndex
	rf.persist(args.Data)
	matchIndex := rf.matchingLogIndex(args.LastIncludedIndex, 
										args.LastIncludedTerm)
	if matchIndex != -1 {
		DPrintf("S%v, discarding up to IDX:%v", rf.me, matchIndex)
		rf.log = rf.log[matchIndex:]
	} else {
		DPrintf("S%v, discarding whole log", rf.me)
		rf.log = nil
	}
	rf.sendApplyMsgSnapshot(rf.snapshotIndex, rf.snapshotTerm, args.Data)
}


func (rf *Raft) sendInstallSnapshot(iPeer int, data []byte) {
	DPrintf("S%v, SENDING INSTALL SNAPSHOT to:%v", rf.me, iPeer)
	targetPeer := rf.peers[iPeer]
	args := InstallSnapshotArgs{
		Term : rf.term,
		LeaderId : rf.me,
		LastIncludedIndex : rf.snapshotIndex,
		LastIncludedTerm : rf.snapshotTerm,
		Data : data}
	reply := InstallSnapshotReply{}
	DPrintf("S%v, sending data of len:%v", rf.me, len(data))
	ok := targetPeer.Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		DPrintf("S%v, INSTALL SNAPSHOT COUDLN'T REACH: %v", rf.me, iPeer)
	} else {
		DPrintf("S%v, INSTALL SNAPSHOT on:%v SUCCEEDED", rf.me, iPeer)
	}
}


// first index is 1
func (rf *Raft) matchingLogIndex(lastIncludedIndex, lastIncludedTerm int) int {
	for i := rf.snapshotLogLen + 1; i <= rf.lenLog(); i ++ {
		logEntry := rf.accessLog(i)
		if logEntry.Index == lastIncludedIndex && logEntry.Term == lastIncludedTerm {
			return i
		}
	}
	return -1 
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
	Term       int
	Success    bool
	FailReason int // 1 is because term < currentTerm
				   // 2 is because log doesn't match
}



func (rf *Raft) sendApplyMsg(prevCommitIndex, commitIndex int, 
							snapshot []byte) {
	rf.mu.Lock()
	DPrintf("S%v, sendApplyMsg(), from IDX:%v to IDX:%v",
			rf.me, prevCommitIndex+1, commitIndex)
	start := prevCommitIndex + 1
	end := commitIndex
	rf.mu.Unlock()
	for i := start; i <= end; i ++ {
		rf.mu.Lock()
		logEntry := rf.accessLog(i)
		DPrintf("S%v, accessing log:%v, has IDX:%v", 
				rf.me, i, logEntry.Index)
		applyMsg := ApplyMsg {
			CommandValid : true,
			Command : logEntry.Command,
			CommandIndex : logEntry.Index,
			SnapshotValid : false,
			Snapshot : nil,
			SnapshotTerm : -1,
			SnapshotIndex : -1,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
}


func (rf *Raft) sendApplyMsgSnapshot(snapshotIndex, snapshotTerm int, 
									snapshot []byte) {
	DPrintf("S%v, sendApplyMsgSnapshot()", rf.me)
	applyMsg := ApplyMsg {
		CommandValid : false,
		Command : nil,
		CommandIndex : -1,
		SnapshotValid : true,
		Snapshot : snapshot,
		SnapshotTerm : snapshotTerm,
		SnapshotIndex : snapshotIndex,
	}
	rf.applyCh <- applyMsg
	DPrintf("S%v, sendApplyMsgSnapshot() COMPLETE", rf.me)
}


func (rf *Raft) commitLog(args *AppendEntriesArgs) {
	// set commitIndex = min(leaderCommit, index of last new entry)
	prevCommitIndex := rf.commitIndex
	DPrintf("S%v, APPEND ENTR, leader C: %v > C: %v", 
			rf.me, args.LeaderCommit, rf.commitIndex)
	lastEntryIndex := 2147483647 // max 32 bit int
	if len(args.Entries) > 0 {
		lastEntryIndex = args.Entries[len(args.Entries) - 1].Index
	}
	rf.commitIndex = lastEntryIndex
	if args.LeaderCommit < lastEntryIndex {
		rf.commitIndex = args.LeaderCommit
	}
	DPrintf("S%v, C->%v", rf.me, rf.commitIndex)
	if prevCommitIndex <= rf.snapshotLogLen && rf.snapshotLogLen != 0 {
		prevCommitIndex = rf.snapshotLogLen 
	}
	rf.mu.Unlock()
	rf.sendApplyMsg(prevCommitIndex, rf.commitIndex, nil)
	rf.mu.Lock()
	DPrintf("S%v, sendApplyMsg done", rf.me)
}


// - reject heartbeat if term in args is lower than my term
// - if their term > my term, update my term, convert to follower
// - if i am candidate, and recieve heartbeat with term equal to 
//	  mine, become a follower (election lost)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("S%v, RECIEVED APPEND ENTR from: %v", rf.me, args.LeaderId)
	if args.Term < rf.term {
		reply.Success = false
		reply.FailReason = 1
		reply.Term = rf.term
		DPrintf("S %v, REJECTED APPEND ENTR from lower T:%v", rf.me, args.Term)
		return
	}

	if args.PrevLogIndex > rf.lenLog() {
		reply.Success = false
		reply.FailReason = 2
		reply.Term = rf.term
		DPrintf("S%v, REJECTED APPEND ENTR: prev log DOESN'T MATCH, prevLogIndex: %v, prevLogTerm:%v, logLen:%v", 
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.lenLog())
		return
	} else if args.PrevLogIndex != 0 && rf.accessLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.FailReason = 2
		reply.Term = rf.term
		DPrintf("S%v, REJECTED APPEND ENTR: prev log DOESN'T MATCH, prevLogIndex: %v, prevLogTerm:%v, T at prevLogIndex:%v", 
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.accessLogTerm(args.PrevLogIndex))
		return
	} 

	if len(args.Entries) != 0 {
		DPrintf("S%v, APPEND ENTR, appending NEW ENTRIES", rf.me)
		rf.appendNewEntries(args.Entries)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitLog(args)
	}
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.state = 0
		DPrintf("S%v, T->%v from APPEND ENTR", rf.me, args.Term)
	}
	// election lost if i am candidate and recieve heartbeat 
	// with same term so become follower
	if rf.state == 1 && args.Term == rf.term {
		rf.state = 0
		DPrintf("S%v, ELECTION LOST from APPEND ENTR", rf.me)
	}
	rf.heartbeat = true
	reply.Term = rf.term
	reply.Success = true
	DPrintf("S%v, SUCCESS APPEND ENTR", rf.me)
}


// - send heartbeat to all peers
// - if reply contains a rejection because peers term number
//    is greater than my tern num, step down as leader
func (rf *Raft) sendAppendEntries(me, iPeer int, targetPeer *labrpc.ClientEnd, 
								args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("S%v, SENDING APPEND ENTR to: %v, len(entries): %v", 
			me, iPeer, len(args.Entries))
	ok := targetPeer.Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("S%v, APPEND ENTR COUDLN'T REACH: %v", me, iPeer)
	}
	return ok
}


func (rf *Raft) appendNewEntries(newEntries []LogEntry) {
	for i := 0; i < len(newEntries); i ++ {
		if rf.checkLogConflict(newEntries[i]) {
			rf.removeConflictingLogs(newEntries[i].Index)
		} 
		if newEntries[i].Index == rf.lenLog() + 1 {
			rf.appendLog(newEntries[i])

		}
	}
}


func (rf *Raft) checkLogConflict(newEntry LogEntry) bool {
	if (newEntry.Index > rf.lenLog()) {
		return false
	}
	if (rf.accessLogTerm(newEntry.Index) != newEntry.Term) {
		return true
	}
	return false
}


func (rf *Raft) removeConflictingLogs(index int) {
	for rf.lenLog() >= index {
		rf.popLog()
	}
}


func (rf *Raft) getAppendEntriesArgs(iPeer int) AppendEntriesArgs {
	DPrintf("S%v, getAppendEntriesArgs(), for peer:%v" ,rf.me, iPeer)
	entries := make([]LogEntry, 0)
	if rf.lenLog() >= rf.nextIndex[iPeer] {
		for i := rf.nextIndex[iPeer]; i <= rf.lenLog(); i ++ {
			entries = append(entries, *rf.accessLog(i))
		}
	}
	prevLogIndex := rf.nextIndex[iPeer] - 1
	prevLogTerm := rf.accessLogTerm(prevLogIndex)
	args := AppendEntriesArgs{
		Term : rf.term, 
		LeaderId : rf.me,
		PrevLogIndex : 	prevLogIndex,
		PrevLogTerm : prevLogTerm,
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
					if rf.nextIndex[i] <= rf.snapshotLogLen {
						data := rf.persister.ReadSnapshot()
						rf.sendInstallSnapshot(i, data)
						rf.nextIndex[i] = rf.snapshotLogLen + 1
					}
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
					if !reply.Success && reply.FailReason == 1 {
						// step down as leader if peer has higher term
						rf.updateTerm(reply.Term)
						rf.state = 0
						DPrintf("S%v, APPEND ENTR to: %v REJECTED, peer T%v > my T%v, becoming follower ", 
								rf.me, i, reply.Term, rf.term)
						rf.mu.Unlock()
						return
					} else if !reply.Success && reply.FailReason == 2 { // log incosistency 
						rf.nextIndex[i] --
						logInconsistency = true
						DPrintf("S%v, APPEND ENTR to: %v REJECTED, log inconsistency, RETRYING", 
									rf.me, i)
					} else if reply.Success {
						nextIndex := rf.lenLog() + 1
						matchIndex := rf.lenLog()
						rf.nextIndex[i] = nextIndex
						rf.matchIndex[i] = matchIndex
						DPrintf("S%v, APPEND ENTR to: %v, SUCCESS, peer next IDX->%v, peer match IDX->%v", 
									rf.me, i, nextIndex, matchIndex)
					}
					rf.mu.Unlock()
				}
			} (rf, i)
		}
	}
}


func (rf *Raft) printPeerIndexes(peerIndexes []int) {
	for i := 0; i < len(peerIndexes); i ++ {
		fmt.Printf("%v ", peerIndexes[i])
	}
	fmt.Printf("\n")
 }


func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// duplicated on e.g. 2/5 peer forms 
	// majority when including leader
	majority := int(len(rf.peers) / 2)
	if rf.commitIndex + 1 > rf.lenLog() + 1 {
		return
	}
	maxN := rf.commitIndex
	for N := rf.lenLog(); N >= rf.commitIndex + 1; N-- {
		count := 0
		for iPeer := 0; iPeer < len(rf.peers); iPeer ++ {
			if rf.matchIndex[iPeer] >= N {
				count ++
			}
		}
		DPrintf("No. peers with IDX >= %v: %v, majority:%v, Nth log term:%v, my term:%v\n", 
					N, count, majority, rf.accessLogTerm(N), rf.term)
		if count >= majority && rf.accessLogTerm(N) == rf.term {
			DPrintf("count exceeds majority\n")
			maxN = N
			break
		} 
	}
	if rf.commitIndex != maxN {
		rf.commitIndex = maxN
		rf.matchIndex[rf.me] = rf.commitIndex
		rf.nextIndex[rf.me] = rf.commitIndex + 1
		DPrintf("S%v, C->%v", rf.me, rf.commitIndex)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	isLeader := rf.state == 2

	if isLeader {
		DPrintf("S%v, Start(), command:%v", rf.me, command)
		index = rf.lenLog() + 1
		rf.appendLog(LogEntry{Command : command, 
							  Term : rf.term, 
							  Index : index})
		DPrintf("S%v, APPEND LOG, T:%v, IDX:%v", rf.me, rf.term, index)
	}
	return index, rf.term, isLeader
}


func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	vreqArgs := RequestVoteArgs{
		Term: rf.term, 
		CandidateId: rf.me,
	}
	if rf.lenLog() == rf.snapshotLogLen && rf.snapshotLogLen != 0 {
		vreqArgs.LastLogIndex = rf.snapshotIndex
		vreqArgs.LastLogTerm = rf.snapshotTerm
	} else {
		lastLogEntry := rf.accessLog(rf.lenLog())
		if lastLogEntry != nil {
			vreqArgs.LastLogIndex = lastLogEntry.Index
			vreqArgs.LastLogTerm = lastLogEntry.Term	
		} else {
			vreqArgs.LastLogIndex = 0
			vreqArgs.LastLogTerm = 0
		}
	}
	return vreqArgs
}


func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.updateTerm(rf.term + 1)
	rf.votedFor = rf.me
	rf.persist(rf.persister.ReadSnapshot())
	rf.state = 1
	votes := 1 // vote for myself
	DPrintf("S%v, started ELECTION, for term: %v", rf.me, rf.term)
	rf.mu.Unlock()
	var waitCount int32 = 0
	var won int32 = 0

	for vreq := 0; vreq < len(rf.peers); vreq ++ {
		if vreq != rf.me {
			atomic.AddInt32(&waitCount, 1)

			go func(rf *Raft, waitCount *int32, 
					votes *int, vreq int, won *int32) {
				vreqArgs := rf.getRequestVoteArgs()
				vreqReply := RequestVoteReply{}
				ok := rf.sendRequestVote(vreq, &vreqArgs, &vreqReply)
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

			} (rf, &waitCount, &votes, vreq, &won)	
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
		prevCommitIndex := rf.commitIndex
		rf.updateCommitIndex()
		if prevCommitIndex != rf.commitIndex {
			rf.sendApplyMsg(prevCommitIndex, rf.commitIndex, nil)
			DPrintf("S%v, sendApplyMsg done", rf.me)
		}
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
	DPrintf("S%v, T->%v", rf.me, rf.term)
	rf.votedFor = -1
	rf.persist(rf.persister.ReadSnapshot())
}


func (rf *Raft) initIndexState() {
	rf.matchIndex = nil
	rf.nextIndex = nil
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, rf.lenLog() + 1)
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
	rf.term = 0
	rf.votedFor = -1
	rf.electionTimeoutMin = 600
	rf.electionTimeoutRange = 300
	rf.heartbeatTime = 125
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.snapshotLogLen = 0
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
