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
	term int
	state int // 0: follower
			  // 1: candidate
			  // 2: leader
	votedFor int
	electionTimeoutMin int64
	electionTimeoutRange int64
	heartbeatTime int64

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	Term int
	CandidateId int
	// (2B stuff TODO)

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2a:
	// - reject if my term is greater than term in args, and reply with my term
	// - can only vote once 
	// - if my term is less than (or equal?) to term in args, step down as leader
	// 		(if applicable), and update term, and vote for server who requested vote
	// - persist who voted for in each term so if crashes doesn't re-vote

	// reject request if my term is ahead 
	DPrintf("server: %v, vote request from: %v, for term: %v", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.term {
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}
	// i have already won the election
	if args.Term == rf.term && rf.state == 2 {
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}

	// become follower and update term if my term is behind
	if args.Term > rf.term {
		rf.state = 0
		rf.updateTerm(args.Term)
	}


	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.term
	}
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
	return ok
}




type AppendEntriesArgs struct {
	Term int
	LeaderId int
	// TODO others
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// - reject heartbeat if term in args is lower than my term
	// - if their term > my term, update my term, convert to follower
	// - if i am candidate, and recieve heartbeat with term equal to 
	//	  mine, become a follower (election lost)
	DPrintf("server: %v, RECIEVED HEARTBEAT from: %v", rf.me, args.LeaderId)
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		DPrintf("server: %v, REJECTED HEARTBEAT from lower term", rf.me)
		return
	}
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.state = 0
		DPrintf("server: %v, UPDATING TERM from HEARTBEAT", rf.me)
	}
	// election lost if i am candidate and recieve heartbeat with same term
	// so become follower
	if rf.state == 1 && args.Term == rf.term {
		rf.state = 0
		DPrintf("server: %v, ELECTION LOST from HEARTBEAT", rf.me)
	}
	rf.heartbeat = true
	reply.Term = rf.term
	reply.Success = true
	DPrintf("server: %v, SUCCESS HEARTBEAT", rf.me)
}

func (rf *Raft) sendAppendEntries(targetServer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// - send heartbeat to all peers
	// - if reply contains a rejection because peers term number
	//    is greater than my tern num, step down as leader
	DPrintf("server: %v, SENDING HEARTBEAT to: %v", rf.me, targetServer)
	ok := rf.peers[targetServer].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("server: %v, HEARTBEAT COUDLN'T REACH: %v", rf.me, targetServer)
	}
	return ok
}


func (rf *Raft) broadcastAppendEntries() {
	args := AppendEntriesArgs{Term: rf.term, LeaderId: rf.me}
	for i := 0; i < len(rf.peers); i ++ {
		if i != rf.me {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			// ignore response if peer un-reachable
			if !ok {
				continue
			}
			// step down as leader if peer has higher term
			if !reply.Success {
				rf.updateTerm(reply.Term)
				rf.state = 0
				DPrintf("server: %v, HEARTBEAT to: %v REJECTED, becoming follower ", rf.me, i)
				return
			} 
		}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}


func (rf *Raft) startElection() {
	// - increments term 
	// - change state to candidate
	// - votes for myself
	// - sends vote request to each peer
	// TODO: restart election after election timeout?
	rf.term ++
	DPrintf("server: %v, started ELECTION, for term: %v", rf.me, rf.term)
	rf.state = 1
	votes := 1
	vreqArgs := RequestVoteArgs{
		Term: rf.term, 
		CandidateId: rf.me}
	
	for vreq := 0; vreq < len(rf.peers); vreq ++ {
		if vreq != rf.me {
			// check if election lost (from recieving heartbeat of same term)
			if rf.state == 0 {
				return
			}
			vreqReply := RequestVoteReply{}
			ok := rf.sendRequestVote(vreq, &vreqArgs, &vreqReply)
			// only consider response if peer is reachable
			if ok {
				if vreqReply.Term > rf.term {
					// update term and become follower
					rf.updateTerm(vreqReply.Term)
					rf.state = 0
					return
				}
				if vreqReply.VoteGranted {
					DPrintf("server: %v, recieved vote from: %v", rf.me, vreq)
					votes ++
				}
			}
			if int(votes / 2) + 1 > len(rf.peers) / 2 {
				// TODO check if need over half of all peers including down ones, or only working peers
				DPrintf("server: %v, WON ELECTION", rf.me)
				rf.state = 2
				return			
			}
		}
	}
}


func (rf *Raft) leaderLoop() {
	for rf.killed() == false && rf.state == 2 {
		rf.broadcastAppendEntries()
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Check if a leader election should be started.

		// - if follower set heartbeat seen to false
		rf.heartbeat = false


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := rf.electionTimeoutMin + (rand.Int63() % rf.electionTimeoutRange)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// - if heartbeat seen is still false then start an election
		if !rf.heartbeat {
			rf.startElection()
			if rf.state == 2 {
				rf.leaderLoop()
			}
		}
	}
}

func (rf *Raft) updateTerm(newTerm int) {
	rf.term = newTerm
	rf.votedFor = -1
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
	rf.electionTimeoutRange = 200
	rf.heartbeatTime = 125

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
