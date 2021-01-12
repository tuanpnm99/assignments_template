package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
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
	ID        CommandID
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandID             int
	nextAppliedID         int
	raftState             map[string]string
	applyMsgMapping       map[int]chan string
	executedResult        map[CommandID]string
	receivedCommand       map[CommandID]int
	previousCommittedTerm int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("Node %v server receive command %v", kv.me, args)

	// Your code here.
	reply.WrongLeader = false
	if ok, result := kv.isExecuted(args.ID); ok {
		DPrintf("Command %v has been executed", args.ID)
		reply.Value = result
		return
	}
	ok, commandID := kv.sendCommand(Op{args.Key, "", GET, args.ID})
	if ok == false {
		reply.WrongLeader = true
		return
	}
	result := kv.getCommandResult(commandID)
	reply.Value = result
	DPrintf("Result value %v for args %v", result, args)
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("Node %v server receive command %v", kv.me, args)

	// Your code here.
	reply.WrongLeader = false
	if ok, _ := kv.isExecuted(args.ID); ok {
		DPrintf("Command %v has been executed", args.ID)
		return
	}

	ok, commandID := kv.sendCommand(Op{args.Key, args.Value, args.Op, args.ID})
	if ok == false {
		reply.WrongLeader = true
		return
	}
	kv.getCommandResult(commandID)
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
	kv.receivedCommand = make(map[CommandID]int)
	kv.executedResult = make(map[CommandID]string)
	go kv.applyMessage()
	return kv
}

func (kv *RaftKV) applyMessage() {
	for {
		applyMsg := <-kv.applyCh
		commitIndex := applyMsg.Index
		operation, valid := applyMsg.Command.(Op)
		kv.previousCommittedTerm = applyMsg.Term
		if !valid {
			continue
		}
		result := ""
		if operation.Operation == GET {
			result, _ = kv.raftState[operation.Key]
		} else if operation.Operation == PUT {
			kv.raftState[operation.Key] = operation.Value
		} else { // APPEND
			value, _ := kv.raftState[operation.Key]
			kv.raftState[operation.Key] = value + operation.Value
		}
		kv.mu.Lock()
		kv.executedResult[operation.ID] = result
		//if outChannel, ok := kv.applyMsgMapping[commitIndex]
		//DPrintf("Node %v server waits for commit %v", kv.me, commitIndex)
		if outChannel, ok := kv.applyMsgMapping[commitIndex]; ok {
			outChannel <- result
		}
		kv.mu.Unlock()
		//DPrintf("Node %v server waits done waiting for commit %v", kv.me, commitIndex)
	}
}

func (kv *RaftKV) sendCommand(operation Op) (bool, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commitIndex, received := kv.receivedCommand[operation.ID]; received == true {
		//DPrintf("Command %v has already received with index %v", operation, commitIndex)
		return true, commitIndex
	}
	valid := false
	currentState, currentTerm := kv.rf.GetCurrentStateAndTerm()

	if currentState != "leader" || currentTerm > kv.previousCommittedTerm {
		DPrintf("Node %v refuse command, state %v currentTerm %v prevCommittedTerm %v", kv.me, currentState, currentTerm, kv.previousCommittedTerm)
		return false, -1
	}
	DPrintf("Node %v accept command at currentTerm %v prevCommittedTerm %v", kv.me, currentTerm, kv.previousCommittedTerm)

	commitIndex, _, isLeader := kv.rf.Start(operation)

	if isLeader == true {
		valid = true
		kv.receivedCommand[operation.ID] = commitIndex
		kv.applyMsgMapping[commitIndex] = make(chan string, 1)
		DPrintf("Node %v, Index %v for command op `%v`, key `%v`, value `%v` and ID %v", kv.me, commitIndex, operation.Operation, operation.Key, operation.Value, operation.ID)
	}

	return valid, commitIndex
}

func (kv *RaftKV) getCommandResult(commitIndex int) string {
	result := <-kv.applyMsgMapping[commitIndex]
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.applyMsgMapping, commitIndex)
	return result
}

func (kv *RaftKV) isExecuted(ID CommandID) (bool, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, executed := kv.executedResult[ID]
	return executed, result
}
