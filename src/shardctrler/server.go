package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"


type ShardCtrler struct {
	mu      sync.Mutex
	dupOpMu		 sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dupOpTable	 map[int64]DupOpElem // indexed by client Id
							 		 // indicates if current 
									 // req is duplicate
}


type Op struct {
	OpType  	string // "Join", "Leave", "Move", or "Query"
	
	Servers map[int][]string // Join args
	GIDs []int // Leave arg
	Shard int // Move args
	GID   int
	Num int // Query args
	
	ClientId int64 // client info
	SeqNum int
}


type DupOpElem struct {
	seqNum		int
	config		Config
	err 		Err
}


func (sc *ShardCtrler) dupOpTableGet(clientId int64) (DupOpElem, bool) {
	sc.dupOpMu.Lock()
	dupOpElem, ok := sc.dupOpTable[clientId]
	sc.dupOpMu.Unlock()
	return dupOpElem, ok
}


func (sc *ShardCtrler) dupOpTableSet(clientId int64, dupOpElem DupOpElem) {
	sc.dupOpMu.Lock()
	DPrintf("S%v, dupOpTableSET() clientId:%v seqNum:%v", sc.me, clientId, dupOpElem.seqNum)
	sc.dupOpTable[clientId] = dupOpElem
	sc.dupOpMu.Unlock()
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("S%v, RECV JOIN", sc.me)
	dupOpElem, ok := sc.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", sc.me, args.ClientId)
		sc.dupOpTableSet(args.ClientId, DupOpElem{seqNum: -1, err: ""})	
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: "Join", 
				 Servers: args.Servers,
				 ClientId: args.ClientId, 
				 SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", sc.me)
		_, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", sc.me)
			// reply.Err = OK
			reply.WrongLeader = true
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", sc.me)
			dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", sc.me)
	}
	DPrintf("S%v, SUCCESS", sc.me)
	reply.Err = OK
	reply.WrongLeader = false
}


func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("S%v, RECV LEAVE", sc.me)
	dupOpElem, ok := sc.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", sc.me, args.ClientId)
		sc.dupOpTableSet(args.ClientId, DupOpElem{seqNum: -1, err: ""})	
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: "Leave", 
				 GIDs: args.GIDs,
				 ClientId: args.ClientId, 
				 SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", sc.me)
		_, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", sc.me)
			reply.WrongLeader = true
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", sc.me)
			dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", sc.me)
	}
	DPrintf("S%v, SUCCESS", sc.me)
	reply.Err = OK
	reply.WrongLeader = false
}


func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("S%v, RECV MOVE", sc.me)
	dupOpElem, ok := sc.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", sc.me, args.ClientId)
		sc.dupOpTableSet(args.ClientId, DupOpElem{seqNum: -1, err: ""})	
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: "Move", 
				 Shard: args.Shard,
				 GID: args.GID,
				 ClientId: args.ClientId, 
				 SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", sc.me)
		_, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", sc.me)
			// reply.Err = OK
			reply.WrongLeader = true
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", sc.me)
			dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", sc.me)
	}
	DPrintf("S%v, SUCCESS", sc.me)
	reply.Err = OK
	reply.WrongLeader = false
}


func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("S%v, RECV QUERY", sc.me)
	dupOpElem, ok := sc.dupOpTableGet(args.ClientId)
	if !ok {
		// first request from client
		DPrintf("S%v, first request from C:%v", sc.me, args.ClientId)
		sc.dupOpTableSet(args.ClientId, DupOpElem{seqNum: -1, err: ""})	
		dupOpElem.seqNum = -1
	}
	if dupOpElem.seqNum != args.SeqNum {
		op := Op{OpType: "Query", 
				 Num: args.Num,
				 ClientId: args.ClientId, 
				 SeqNum: args.SeqNum}
		DPrintf("S%v, submitting to raft", sc.me)
		_, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			DPrintf("S%v,  wrong leader", sc.me)
			reply.WrongLeader = true
			return
		} else {
			// wait for applyCh
			DPrintf("S%v, waiting for applyCh", sc.me)
			dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			for dupOpElem.seqNum != args.SeqNum {
				time.Sleep(time.Duration(10) * time.Millisecond)
				dupOpElem, _ = sc.dupOpTableGet(args.ClientId)
			}
		}
	} else {
		DPrintf("S%v, DUPLICATE req", sc.me)
	}
	DPrintf("S%v, SUCCESS", sc.me)
	reply.Err = OK
	reply.Config = dupOpElem.config
	reply.WrongLeader = false
}


func (sc *ShardCtrler) applyChListen() {
	for msg := range sc.applyCh {
		DPrintf("S%v, msg recv from applyCh", sc.me)
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("S%v, msg recv from applyCh is wrong type", sc.me)
				break
			}
			if op.OpType == "Join" {
				DPrintf("S%v, applyChListen Join", sc.me)
				err := sc.join(op.Servers)
				sc.dupOpTableSet(op.ClientId, 
					DupOpElem{seqNum: op.SeqNum, err: err})
			} else if op.OpType == "Leave" {
				DPrintf("S%v, applyChListen Leave", sc.me)
				err := sc.leave(op.GIDs)
				sc.dupOpTableSet(op.ClientId, 
					DupOpElem{seqNum: op.SeqNum, err: err})
			} else if op.OpType == "Move" {
				DPrintf("S%v, applyChListen Move", sc.me)
				err := sc.move(op.Shard, op.GID)
				sc.dupOpTableSet(op.ClientId, 
					DupOpElem{seqNum: op.SeqNum, err: err})
			} else if op.OpType == "Query" {
				DPrintf("S%v, applyChListen Query", sc.me)
				config, err := sc.query(op.Num)
				sc.dupOpTableSet(op.ClientId, 
					DupOpElem{seqNum: op.SeqNum, err: err, 
							  config: config})
			}
		}
	}
}

func (sc *ShardCtrler) newConfig() {
	DPrintf("S%v, newConfig()", sc.me)
	configNum := len(sc.configs) + 1
	groupsNew := make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
		sc.addToGroup(groupsNew, gid, servers)
	}
	sc.configs = append(sc.configs, Config{Num: configNum, Groups: groupsNew})
	// Can only copy prev. Config's Shards if this wasn't first config added
	copy(sc.configs[len(sc.configs)-1].Shards[0:], 
		 sc.configs[len(sc.configs)-2].Shards[0:])
}


func (sc *ShardCtrler) firstConfig(groups map[int][]string) {
	DPrintf("S%v, firstConfig()", sc.me)
	gids := make([]int, len(groups))
	i := 0
	for gid := range groups {
		gids[i] = gid
		i ++
	}
	configNum := 1
	sc.configs = append(sc.configs, Config{Num: configNum, Groups: groups})
	// Initialise shards so split evenly amoung groups
	g := 0
	for s := 0; s < NShards; s++ {
		sc.configs[len(sc.configs)-1].Shards[s] = gids[g]
		g = (g + 1) % len(gids)
	}
}


func (sc *ShardCtrler) join(groups map[int][]string) Err {
	DPrintf("S%v, join(), len(sc.configs):%v, groups:%v", sc.me, len(sc.configs), groups)
	if len(sc.configs) == 0 {
		sc.firstConfig(groups)
	} else {
		sc.newConfig()
	}
	// Join logic
	for gid, servers := range groups {
		_, contains := sc.configs[len(sc.configs)-1].Groups[gid]
		if !contains {
			sc.addToGroup(sc.configs[len(sc.configs)-1].Groups, 
												gid, servers)
		}
	}
	sc.balanceShards(&sc.configs[len(sc.configs)-1])
	return OK
}


func (sc *ShardCtrler) leave(GIDs []int) Err {
	DPrintf("S%v, leave()", sc.me)
	sc.newConfig()
	var gidNotRmv int
	// finding a gid that is not being removed (i.e. not in GIDs)
	for gid := range sc.configs[len(sc.configs)-1].Groups {
		if !sc.contains(GIDs, gid) {
			gidNotRmv = gid
		}
	}
	for _, gidRmv := range GIDs {
		// change shard assignments to group not being removed
		for shard := range sc.configs[len(sc.configs)-1].Shards {
			if sc.configs[len(sc.configs)-1].Shards[shard] == gidRmv {
				sc.configs[len(sc.configs)-1].Shards[shard] = gidNotRmv 
			}
		}
		sc.removeFromGroup(sc.configs[len(sc.configs)-1].Groups, gidRmv)
	}
	sc.balanceShards(&sc.configs[len(sc.configs)-1])
	return OK
}


func (sc *ShardCtrler) move(shard int, GID int) Err {
	sc.newConfig()
	sc.configs[len(sc.configs)-1].Shards[shard] = GID
	return OK
}


func (sc *ShardCtrler) query(num int) (Config, Err) {
	DPrintf("S%v, query()", sc.me)
	var res Config
	if len(sc.configs) == 0 {
		return Config{}, OK
	}
	if num == 0 {
		return Config{}, OK
	}
	if num < 0 || num >= len(sc.configs) {
		res = sc.configs[len(sc.configs)-1]
	} else {
		res = sc.configs[num-1]
	}
	return res, OK
}


func (sc *ShardCtrler) balanceShardsMax(gidToNShard map[int]int) (int, int) {
	var gidRes int
	nShardMax := 0
	// DPrintf("S%v, balanceShardsMax(), gidToNShard:%v", sc.me, gidToNShard)
	for gid, nShard := range gidToNShard {
		
		if nShard > nShardMax {
			gidRes = gid
			nShardMax = nShard
			// DPrintf("S%v, balanceShardsMax(), nShardMax:%v", sc.me, nShardMax)
		}
	}
	return gidRes, nShardMax
} 


func (sc *ShardCtrler) balanceShardsMin(gidToNShard map[int]int) (int, int) {
	var gidRes int
	nShardMin := 4294967296 // asuming no more 
							// than this many shards
	for gid, nShard := range gidToNShard {
		if nShard < nShardMin {
			gidRes = gid
			nShardMin = nShard
		}
	}
	return gidRes, nShardMin
} 

func (sc *ShardCtrler) balanceShardsCount(gidToNShard map[int]int) {
	for gid := range sc.configs[len(sc.configs)-1].Groups {
		gidToNShard[gid] = 0
	}
	for s := 0; s < NShards; s ++ {
		gid := sc.configs[len(sc.configs)-1].Shards[s]
		_, ok := gidToNShard[gid]
		if !ok {
			DPrintf("S%v, PROBLEM", sc.me)
		} else {
			gidToNShard[gid] ++
		}
	}
}

func (sc *ShardCtrler) balanceShards(config *Config) {
	DPrintf("S%v, balanceShards()", sc.me)
	gidToNShard := make(map[int]int)
	sc.balanceShardsCount(gidToNShard)
	gidMax, nShardMax := sc.balanceShardsMax(gidToNShard)
	gidMin, nShardMin := sc.balanceShardsMin(gidToNShard)
	for nShardMax - nShardMin > 1 {
		diff := nShardMax - nShardMin
		var shardsToMove int = int(diff / 2)
		for iShard := 0; iShard < NShards; iShard ++ {
			if config.Shards[iShard] == gidMax && shardsToMove > 0 {
				config.Shards[iShard] = gidMin
				shardsToMove --
			}
		}
		sc.balanceShardsCount(gidToNShard)
		gidMax, nShardMax = sc.balanceShardsMax(gidToNShard)
		gidMin, nShardMin = sc.balanceShardsMin(gidToNShard)
		gidToNShard = make(map[int]int)
	}
}


func (sc *ShardCtrler) addToGroup(groups map[int][]string, 
								gid int, servers []string) {
	serversNew := make([]string, len(servers))
	copy(serversNew, servers)
	groups[gid] = serversNew
}


func (sc *ShardCtrler) removeFromGroup(groups map[int][]string, gid int) {
	delete(groups, gid)
}


func (sc *ShardCtrler) contains(s []int, e int) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 0)
	// sc.configs[0].Groups = map[int][]string{}
	// sc.configs[0].Num = 1


	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.dupOpTable = make(map[int64]DupOpElem)

	go sc.applyChListen()

	return sc
}
