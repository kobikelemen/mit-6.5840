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



type OpEntry struct {
	Idx			int
	Term		int
	OpType  	string // "Get", "Put", or "Append"
	// ID 			int64
	Key			string
	Value 		string

	// client info
	ClientId	int64
	SeqNum 		int
}


type DupOpElem struct {
	seqNum		int
	res			string
}


func (kv *KVServer) dupOpTableGet(clientId int64) (DupOpElem, bool) {
	kv.dupOpMu.Lock()
	dupOpElem, ok := kv.dupOpTable[clientId]
	kv.dupOpMu.Unlock()
	return dupOpElem, ok
}

func (kv *KVServer) dupOpTableSet(clientId int64, dupOpElem DupOpElem) {
	kv.dupOpMu.Lock()
	DPrintf("S%v, dupOpTableSET() clientId:%v seqNum:%v", kv.me, clientId, dupOpElem.seqNum)
	kv.dupOpTable[clientId] = dupOpElem
	kv.dupOpMu.Unlock()
}

type KVServer struct {
	mu      	 sync.Mutex
	dupOpMu		 sync.Mutex
	me      	 int
	rf      	 *raft.Raft
	applyCh 	 chan raft.ApplyMsg
	dead    	 int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	db			 []KV
	dupOpTable	 map[int64]DupOpElem // indexed by client Id
							 		 // indicates if current 
									 // req is duplicate
}


type KV struct {
	key		string
	val 	string
}


func (kv *KVServer) dbGet(key string) string {
	DPrintf("S%v, dbGet() waiting for lock", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("S%v, dbGet(), key:%v", kv.me, key)
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			DPrintf("S%v, dbGet(), returning val:%v", 
						kv.me, kv.db[i].val)
			return kv.db[i].val
		}
	}
	return ""
}


func (kv *KVServer) dbAppend(key, val string) {
	DPrintf("S%v, dbAppend() waiting for lock", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("S%v, dbAppend(), key:%v, val%v", 
					kv.me, key, val)
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			kv.db[i].val = kv.db[i].val + val
			return
		}
	}
	kv.db = append(kv.db, KV{key : key, val : val})
}


func (kv *KVServer) dbPut(key, val string) {
	DPrintf("S%v, dbPut() waiting for lock", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("S%v, dbPut(), key:%v, val%v", 
					kv.me, key, val)
	for i := 0; i < len(kv.db); i ++ {
		if kv.db[i].key == key {
			kv.db[i].val = val
			return
		}
	}
	kv.db = append(kv.db, KV{key : key, val : val})
}





func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("S%v, RECV GET, key:%v", kv.me, args.Key)
	dupOpElem, ok := kv.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", kv.me, args.ClientId)
		kv.dupOpTableSet(args.ClientId, DupOpElem{-1, ""})	
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := OpEntry{OpType: "Get", 
					  Key: args.Key,
					  ClientId: args.ClientId, 
					  SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", kv.me)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", kv.me)
			dupOpElem, _ = kv.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = kv.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", kv.me)
	}
	DPrintf("S%v, SUCCESS", kv.me)
	reply.Value = dupOpElem.res
	reply.Err = OK
}



func (kv *KVServer) PutAppend(args *PutAppendArgs, 
							reply *PutAppendReply) {
	DPrintf("S%v, RECV PUTAPPEND, key:%v, val:%v", kv.me, args.Key, args.Value)
	dupOpElem, ok := kv.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", kv.me, args.ClientId)
		kv.dupOpTableSet(args.ClientId, DupOpElem{-1, ""})
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := OpEntry{OpType: args.Op,
					  Key: args.Key,
					  Value: args.Value,
					  ClientId: args.ClientId, 
					  SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", kv.me)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", kv.me)
			dupOpElem, _ = kv.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = kv.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", kv.me)
	}
	DPrintf("S%v, SUCCESS", kv.me)
	reply.Err = OK
}


func (kv *KVServer) applyChListen() {
	for msg := range kv.applyCh {
		DPrintf("S%v, msg recv from applyCh", kv.me)
		if msg.CommandValid {
			opEntry, ok := msg.Command.(OpEntry)
			if !ok {
				DPrintf("S%v, msg recv from applyCh is wrong type", kv.me)
				break
			}
			if opEntry.OpType == "Get" {
				DPrintf("S%v, applyChListen Get", kv.me)
				res := kv.dbGet(opEntry.Key)
				kv.dupOpTableSet(opEntry.ClientId, 
					DupOpElem{seqNum: opEntry.SeqNum, res: res})
			} else if opEntry.OpType == "Put" {
				DPrintf("S%v, applyChListen Put", kv.me)
				kv.dbPut(opEntry.Key, opEntry.Value)
				kv.dupOpTableSet(opEntry.ClientId, 
					DupOpElem{seqNum: opEntry.SeqNum}) 
			} else if opEntry.OpType == "Append" {
				DPrintf("S%v, applyChListen Append", kv.me)
				kv.dbAppend(opEntry.Key, opEntry.Value)
				kv.dupOpTableSet(opEntry.ClientId, 
					DupOpElem{seqNum: opEntry.SeqNum}) 
			}
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
	labgob.Register(OpEntry{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make([]KV, 0)
	// kv.opQue = make([]OpEntry, 0)
	kv.dupOpTable = make(map[int64]DupOpElem)

	go kv.applyChListen()

	return kv
}
