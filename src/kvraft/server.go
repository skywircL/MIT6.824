package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Option   string
	ClientId int64
	OptionId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	kvMap        map[string]string //维护一个kvMap
	lastOptionId map[int64]int
	executeChan  map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	// Your code here.
	op := Op{Key: args.Key, Option: "Get", ClientId: args.ClientId, OptionId: args.OptionId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//阻塞等待 - - 要设置超时时间
	ch := kv.GetChan(index)
	select {
	case result := <-ch:
		if result.OptionId != args.OptionId || result.ClientId != args.ClientId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		kv.mu.Lock()
		_, ok := kv.kvMap[args.Key]
		if !ok {
			reply.Value = ErrNoKey
		} else {
			reply.Value = kv.kvMap[args.Key]
		}
		kv.mu.Unlock()
		DPrintf("Key= %s Get value = %s\n", args.Key, reply.Value)
	case <-time.After(100 * time.Millisecond):
		DPrintf("Get Timeout\n")
		reply.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		delete(kv.executeChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Option: args.Op, ClientId: args.ClientId, OptionId: args.OptionId, Value: args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.GetChan(index)
	select {
	case result := <-ch:
		if result.OptionId != args.OptionId || result.ClientId != args.ClientId {
			DPrintf("ErrWrongLeader: result.OptionId =%d args.OptionId=%d result.ClientId=%d  args.ClientId=%d\n", result.OptionId, args.OptionId, result.ClientId, args.ClientId)
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK

		}
		//DPrintf("PutAppend kvMap = %v,replyErr = %v\n", kv.kvMap, reply.Err)

	case <-time.After(100 * time.Millisecond):
		DPrintf("PutAppend Timeout\n")
		reply.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		DPrintf("%d delet chan: %d\n", kv.me, index)
		delete(kv.executeChan, index)
		kv.mu.Unlock()
	}()

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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.executeChan = make(map[int]chan Op)
	kv.lastOptionId = make(map[int64]int)
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:

			command := msg.Command.(Op)
			kv.mu.Lock()
			DPrintf("%d server applyCh msg = %v\n", kv.me, msg)
			if !kv.IsDuplicateRequest(command.ClientId, command.OptionId) {

				switch command.Option {
				case "Append":
					kv.kvMap[command.Key] += command.Value

				case "Put":
					kv.kvMap[command.Key] = command.Value
				}

				kv.lastOptionId[command.ClientId] = command.OptionId

			}
			kv.mu.Unlock()
			kv.GetChan(msg.CommandIndex) <- command

		}

	}
}

func (kv *KVServer) GetChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.executeChan[index]
	if !ok {
		kv.executeChan[index] = make(chan Op, 1)
		ch = kv.executeChan[index]
	}
	log.Println("create chan index", index)
	return ch
}

func (kv *KVServer) IsDuplicateRequest(clientId int64, OptionId int) bool {

	_, ok := kv.lastOptionId[clientId]
	if ok {
		return OptionId <= kv.lastOptionId[clientId]
	}
	return ok
}
