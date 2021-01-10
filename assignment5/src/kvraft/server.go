package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[SERVER]"+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandID       int
	nextAppliedID   int
	raftState       map[string]string
	applyMsgMapping map[int]chan string
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	commandID, valid := kv.sendCommand(Op{args.Key, "", GET})
	if valid == false {
		reply.WrongLeader = true
		return
	}
	result := kv.getCommandResult(commandID)
	reply.WrongLeader = false
	reply.Value = result
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	commandID, valid := kv.sendCommand(Op{args.Key, args.Value, args.Op})
	if valid == false {
		reply.WrongLeader = true
		return
	}
	kv.getCommandResult(commandID)
	reply.WrongLeader = false
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.commandID = 1
	kv.nextAppliedID = 1
	kv.applyMsgMapping = make(map[int]chan string)
	kv.raftState = make(map[string]string)
	go kv.applyMessage()
	return kv
}

func (kv *RaftKV) applyMessage() {
	for {
		applyMsg := <-kv.applyCh
		commitIndex := applyMsg.Index
		operation := applyMsg.Command.(Op)
		result := ""
		if operation.Operation == GET {
			result, _ = kv.raftState[operation.Key]
		} else if operation.Operation == PUT {
			kv.raftState[operation.Key] = operation.Value
		} else { // APPEND
			value, _ := kv.raftState[operation.Key]
			kv.raftState[operation.Key] = value + operation.Value
		}
		kv.applyMsgMapping[commitIndex] <- result
	}
}

func (kv *RaftKV) sendCommand(operation Op) (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valid := false
	commitIndex, _, isLeader := kv.rf.Start(operation)
	if isLeader == true {
		valid = true
		kv.applyMsgMapping[commitIndex] = make(chan string, 1)
		//DPrintf("Index %v for command op `%v`, key `%v`, value `%v`", commitIndex, operation.Operation, operation.Key, operation.Value)
	}

	return commitIndex, valid
}

func (kv *RaftKV) getCommandResult(commitIndex int) string {
	result := <-kv.applyMsgMapping[commitIndex]
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.applyMsgMapping, commitIndex)
	return result
}
