package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Idx    int
	Term   int
	OpType string // "Get", "Put", or "Append"
	// ID 			int64
	Key   string
	Value string

	// client info
	ClientId int64
	SeqNum   int

	NewConfig shardctrler.Config
}

type DupOpElem struct {
	seqNum int
	res    string
}

func (kv *ShardKV) dupOpTableGet(clientId int64) (DupOpElem, bool) {
	// kv.dupOpMu.Lock()
	dupOpElem, ok := kv.dupOpTable[clientId]
	// kv.dupOpMu.Unlock()
	return dupOpElem, ok
}

func (kv *ShardKV) dupOpTableSet(clientId int64, dupOpElem DupOpElem) {
	// kv.dupOpMu.Lock()
	DPrintf("S%v, dupOpTableSET() clientId:%v seqNum:%v", kv.me, clientId, dupOpElem.seqNum)
	kv.dupOpTable[clientId] = dupOpElem
	// kv.dupOpMu.Unlock()
}

type ShardKV struct {
	mu           sync.Mutex
	muDb         sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	ctrlClerk  *shardctrler.Clerk
	config     shardctrler.Config
	db         map[string]string
	dupOpTable map[int64]DupOpElem // indexed by client Id
	// indicates if current
	// req is duplicate
}

func (kv *ShardKV) SendShards(args *SendShardsArgs,
	reply *SendShardsReply) {
	kv.muDb.Lock()
	defer kv.muDb.Unlock()
	op := Op{OpType: "AddShards",
		NewShards: args.NewShards}
	reply.Err = OK
}

func (kv *ShardKV) addShardsToDb(newKVs map[string]string) {
	// TODO add key key-vals to my db
}

func (kv *ShardKV) recvedAllShards() bool {
	kv.muDb.Lock()
	defer kv.muDb.Unlock()
	// TODO iterate over kv.db to check if I've recvied all shards
	// from other servers
}

func (kv *ShardKV) getShardsToSend(gid int) map[string]string {
	// TODO returns key-val map that need to be sent
	// to given group

}

func (kv *ShardKV) copyOpConfig(newConfig shardctrler.Config) {
	kv.config.Num = newConfig.Num
	copy(kv.config.Shards[:], newConfig.Shards[:])
	kv.config.Groups = make(map[int][]string)
	for k, v := range newConfig.Groups {
		kv.config.Groups[k] = v
	}
}

func (kv *ShardKV) sendShardsToGroup(shardsToSend map[string]string) {

}

func (kv *ShardKV) changeConfig(op Op) {
	DPrintf("S%v, changeConfig()", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.copyOpConfig(op.NewConfig)

	// TODO change following if condition so only done if I am leader
	// (don't want it to be sent many times)
	if op.NewConfig.Num > 1 {
		// TODO
		// call RPC on other groups involved to send my shards
		// use make_end to convert gid to actual server
		// wait for other groups to send stuff to me
		for gid, _ := range kv.config.Groups {
			shardsToSend := kv.getShardsToSend(gid)
			kv.sendShardsToGroup(shardsToSend)
			// TODO call SendShards RPC
		}
		for !kv.recvedAllShards() {
			// wait
		}
	}
}

func (kv *ShardKV) listenChangeConfig() {
	for {
		time.Sleep(time.Duration(100) * time.Millisecond)
		newestConfig := kv.ctrlClerk.Query(-1)
		if kv.config.Num != newestConfig.Num {
			op := Op{OpType: "ChangeConfig",
				NewConfig: newestConfig}
			DPrintf("S%v, submitting CONFIG to raft", kv.me)
			// if I'm not leader then kv.rf.Start() won't do anything
			kv.rf.Start(op)
		}
	}
}

func (kv *ShardKV) isAtleastFirstConfig() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num >= 1
}

func (kv *ShardKV) dbGet(key string) string {
	DPrintf("S%v, dbGet()", kv.me)
	kv.muDb.Lock()
	defer kv.muDb.Unlock()
	val, ok := kv.db[key]
	if ok {
		return val
	}
	return ""
}

func (kv *ShardKV) dbAppend(key, newval string) {
	DPrintf("S%v, dbAppend()", kv.me)
	kv.muDb.Lock()
	defer kv.muDb.Unlock()
	val, ok := kv.db[key]
	if ok {
		kv.db[key] = val + newval
		return
	}
	kv.db[key] = newval
}

func (kv *ShardKV) dbPut(key, newval string) {
	kv.muDb.Lock()
	defer kv.muDb.Unlock()
	DPrintf("S%v, dbPut()", kv.me)
	kv.db[key] = newval
}

func (kv *ShardKV) isMyShard(key string) bool {
	iShard := key2shard(key)
	return kv.config.Shards[iShard] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("S%v, RECV GET, key:%v", kv.me, args.Key)
	for !kv.isAtleastFirstConfig() {
		// wait until first config set
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isMyShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	dupOpElem, ok := kv.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", kv.me, args.ClientId)
		kv.dupOpTableSet(args.ClientId, DupOpElem{-1, ""})
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: "Get",
			Key:      args.Key,
			ClientId: args.ClientId,
			SeqNum:   args.SeqNum}
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	DPrintf("S%v, RECV PUTAPPEND, key:%v, val:%v", kv.me, args.Key, args.Value)
	for !kv.isAtleastFirstConfig() {
		// wait until first config set
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isMyShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	dupOpElem, ok := kv.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", kv.me, args.ClientId)
		kv.dupOpTableSet(args.ClientId, DupOpElem{-1, ""})
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: args.Op,
			Key:      args.Key,
			Value:    args.Value,
			ClientId: args.ClientId,
			SeqNum:   args.SeqNum}
		DPrintf("S%v, submitting to raft", kv.me)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			DPrintf("S%v, wrong leader", kv.me)
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
		DPrintf("S%v, DUPLICATE req, clientId:%v, seqNum:%v", kv.me, args.ClientId, args.SeqNum)
	}
	DPrintf("S%v, SUCCESS", kv.me)
	reply.Err = OK
}

func (kv *ShardKV) get(op Op) {
	res := kv.dbGet(op.Key)
	DPrintf("S%v, get(), key:%v, val:%v", kv.me, op.Key, res)
	kv.dupOpTableSet(op.ClientId, DupOpElem{seqNum: op.SeqNum, res: res})
}

func (kv *ShardKV) put(op Op) {
	DPrintf("S%v, put(), key:%v, val:%v", kv.me, op.Key, op.Value)
	kv.dbPut(op.Key, op.Value)
	// DPrintf("S%v, db:%v", kv.me, kv.db)
	kv.dupOpTableSet(op.ClientId, DupOpElem{seqNum: op.SeqNum})
}

func (kv *ShardKV) append(op Op) {
	DPrintf("S%v, append(), key:%v, val:%v", kv.me, op.Key, op.Value)
	kv.dbAppend(op.Key, op.Value)
	kv.dupOpTableSet(op.ClientId, DupOpElem{seqNum: op.SeqNum})
}

func (kv *ShardKV) applyChListen() {
	for msg := range kv.applyCh {
		DPrintf("S%v, msg recv from applyCh", kv.me)
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("S%v, msg recv from applyCh is wrong type", kv.me)
				break
			}
			if op.OpType == "Get" {
				DPrintf("S%v, applyChListen Get, k:%v",
					kv.me, op.Key)
				kv.get(op)
			} else if op.OpType == "Put" {
				DPrintf("S%v, applyChListen Put, k:%v, v:%v",
					kv.me, op.Key, op.Value)
				kv.put(op)
			} else if op.OpType == "Append" {
				DPrintf("S%v, applyChListen Append, k:%v, v:%v",
					kv.me, op.Key, op.Value)
				kv.append(op)
			} else if op.OpType == "ChangeConfig" {
				kv.changeConfig(op)
			}
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.config.Num = 0
	kv.ctrlClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dupOpTable = make(map[int64]DupOpElem)
	kv.db = make(map[string]string)

	go kv.listenChangeConfig()
	go kv.applyChListen()

	return kv
}
