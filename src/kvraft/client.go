package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	iLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.iLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key : key, OpID : nrand()}
	reply := GetReply{}
	for true {
		DPrintf("C, SENDING GET to:%v", ck.iLeader)
		ok := ck.servers[ck.iLeader].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("C, GET COULDN'T REACH:%v", ck.iLeader)
			continue
		}
		if reply.Err == OK {
			DPrintf("C, OK from:%v reply:%v", ck.iLeader, reply.Value)
			return reply.Value
		} else if reply.Err == ErrWrongLeader {
			DPrintf("C, wrong leader, retrying")
			ck.iLeader = (ck.iLeader + 1) % len(ck.servers)
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key : key, Value : value, Op : op, OpID : nrand()}
	reply := PutAppendReply{}
	for true {
		if value != "" {
			DPrintf("C, SENDING PUTAPPEND to:%v, key:%v, val:%v", ck.iLeader, key, value)
		}
		ok := ck.servers[ck.iLeader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("C, GET COULDN'T REACH:%v", ck.iLeader)
			continue
		}
		if reply.Err == OK {
			DPrintf("C, OK from:%v", ck.iLeader)
			return
		} else if reply.Err == ErrWrongLeader {
			DPrintf("C, %v wrong leader, retrying", ck.iLeader + 1)
			ck.iLeader = (ck.iLeader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
