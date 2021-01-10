package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const (
	GET              = "GET"
	APPEND           = "APPEND"
	PUT              = "PUT"
	MaxQueueCapacity = 10000
	GETRPC           = "RaftKV.Get"
	PUTAPPENDRPC     = "RaftKV.PutAppend"
)

type Operation struct {
	key        string
	value      string
	opType     string
	outChannel chan string
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	queue chan Operation
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
	// You'll have to add code here.
	ck.queue = make(chan Operation, MaxQueueCapacity)
	go ck.workerRoutine()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	outChannel := make(chan string, 1)
	ck.addOperation(Operation{key, "", GET, outChannel})
	result := <-outChannel
	return result

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) addOperation(operation Operation) {
	// You will have to modify this function.
	ck.queue <- operation
	return
}

func (ck *Clerk) Put(key string, value string) {
	outChannel := make(chan string, 1)
	ck.addOperation(Operation{key, value, PUT, outChannel})
	<-outChannel
}
func (ck *Clerk) Append(key string, value string) {
	outChannel := make(chan string, 1)
	ck.addOperation(Operation{key, value, APPEND, outChannel})
	<-outChannel
}

func (ck *Clerk) workerRoutine() {
	for {
		operation := <-ck.queue
		isTimeout := make(chan bool, 1)
		var result string
		for {
			for server := 0; server < len(ck.servers); server++ {
				go func(s int) {
					wrongLeader := true
					if operation.opType == GET {
						reply := &GetReply{}
						ck.servers[s].Call(GETRPC, &GetArgs{operation.key}, reply)
						result = reply.Value
						wrongLeader = reply.WrongLeader
					} else {
						reply := &GetReply{}
						ck.servers[s].Call(PUTAPPENDRPC, &PutAppendArgs{operation.key, operation.value, operation.opType}, reply)
						wrongLeader = reply.WrongLeader
					}
					if wrongLeader == true {
						return
					}
					isTimeout <- false

				}(server)
			}
			go func() {
				time.Sleep(1000 * time.Millisecond)
				isTimeout <- true
			}()

			if <-isTimeout == false {
				break
			}
			//DPrintf("Timeout, resend command %v", operation)
		}
		operation.outChannel <- result
	}
}
