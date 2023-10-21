package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

var debugStart time.Time = time.Now()

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %v", time, format)
		fmt.Printf(prefix + "\n", a...)
	}
	return
}



type Op struct {
	Idx		int
	Term	int
	OpType  string // "Get", "Put", or "Append"
	ID 		int64
}


type KVServer struct {
	mu      	 sync.Mutex
	me      	 int
	rf      	 *raft.Raft
	applyCh 	 chan raft.ApplyMsg
	dead    	 int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	
	db			 []KV
	opQue		 []Op
	opSubmitted  map[int64]Op
}


type KV struct {
	key		string
	val 	string
}


func (kv *KVServer) isMyOp(idxExp int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(kv.opQue) == 0 {
		return false
	}
	return kv.opQue[len(kv.opQue)-1].Idx == idxExp
}


func (kv *KVServer) opQueRmv() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.opQue = kv.opQue[1:]
}


func (kv *KVServer) opQueAdd(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.opQue = append(kv.opQue, op)
}


func (kv *KVServer) opQueLen() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.opQue)
}


func (kv *KVServer) dbGet(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			return kv.db[i].val
		}
	}
	return ""
}


func (kv *KVServer) dbAppend(key, val string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			kv.db[i].val = kv.db[i].val + val
			return
		}
	}
	kv.db = append(kv.db, KV{key : key, val : val})
}


func (kv *KVServer) dbPut(key, val string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			kv.db[i].val = val
			return
		}
	}
	kv.db = append(kv.db, KV{key : key, val : val})
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("S%v, RECV GET, key:%v, opID:%v", kv.me, args.Key, args.OpID)
	op, ok := kv.opSubmitted[args.OpID]
	var idxExp int
	if !ok {
		DPrintf("S%v, OpID not seen before, submitting to raft", kv.me)
		op.OpType = "Get"
		op.ID = args.OpID
		var term int
		var isLeader bool
		idxExp, term, isLeader = kv.rf.Start(op)
		op.Term = term
		if !isLeader {
			delete(kv.opSubmitted, args.OpID)
			reply.Err = ErrWrongLeader
			return
		} else {
			kv.opSubmitted[op.ID] = op
		}
	} else {
		DPrintf("S%v, OpID SEEN before", kv.me)
		idxExp = op.Idx
	}
	DPrintf("S%v, GET waiting", kv.me)
	for !kv.isMyOp(idxExp) {
		// return if I'm no longer leader
		raftTerm, isLeader := kv.rf.GetState()
		if raftTerm != op.Term && !isLeader{
			DPrintf("S%v, not leader anymore, returning", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
	if kv.opQueLen() > 0 {
		kv.opQueRmv()
	}
	reply.Value = kv.dbGet(args.Key)
	reply.Err = OK
	DPrintf("S%v, GET returning val:%v", kv.me, reply.Value)
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, 
							reply *PutAppendReply) {
	// if args.Value != "" {
		DPrintf("S%v, RECV PUTAPPEND, key:%v, val:%v", 
				kv.me, args.Key, args.Value)
	// }
	op, ok := kv.opSubmitted[args.OpID]
	var idxExp int
	if !ok {
		DPrintf("S%v, OpID NOT SEEN before, submitting to raft", kv.me)
		op.OpType = args.Op
		op.ID = args.OpID
		var term int
		var isLeader bool
		idxExp, term, isLeader = kv.rf.Start(op)
		op.Term = term
		if !isLeader {
			delete(kv.opSubmitted, args.OpID)
			reply.Err = ErrWrongLeader
			return
		} else {
			kv.opSubmitted[op.ID] = op
		}
	} else {
		DPrintf("S%v, OpID SEEN before", kv.me)
		idxExp = op.Idx
	}
	DPrintf("S%v, PUTAPPEND waiting", kv.me)
	for !kv.isMyOp(idxExp) {
		// return if I'm no longer leader
		raftTerm, isLeader := kv.rf.GetState()
		if raftTerm != op.Term && !isLeader{
			DPrintf("S%v, not leader anymore, returning", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
	kv.opQueRmv()
	if args.Op == "Put" {
		kv.dbPut(args.Key, args.Value)
	} else if args.Op == "Append" {
		kv.dbAppend(args.Key, args.Value)
	}
	reply.Err = OK
}


func (kv *KVServer) applyChListen() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("S%v, msg recv from applyCh is wrong type", kv.me)
				break
			}
			op.Idx = msg.CommandIndex
			kv.opQueAdd(op)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make([]KV, 0)
	kv.opQue = make([]Op, 0)
	kv.opSubmitted = make(map[int64]Op)

	go kv.applyChListen()

	return kv
}
