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
	queue               chan Operation
	clientID            int64
	operationID         int
	lastContactedServer int
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
	ck.operationID = 1
	ck.clientID = time.Now().UnixNano()
	ck.lastContactedServer = int(nrand()) % len(ck.servers)
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
		result := ""
		ID := CommandID{ck.clientID, ck.operationID}
		timeoutCnt := 0
		for {
			timeout := make(chan bool, 1)
			rpc := make(chan bool, 1)
			rpcResult := ""
			wrongLeader := true
			valid := false
			go func(s int) {
				if operation.opType == GET {
					reply := &GetReply{}
					if !ck.servers[s].Call(GETRPC, &GetArgs{operation.key, ID}, reply) {
						return
					}
					rpcResult = reply.Value
					wrongLeader = reply.WrongLeader
				} else {
					reply := &GetReply{}
					if !ck.servers[s].Call(PUTAPPENDRPC, &PutAppendArgs{operation.key, operation.value, operation.opType, ID}, reply) {
						return
					}
					wrongLeader = reply.WrongLeader

				}

				//DPrintf("Result for %v is from node %v and ID %v", operation, s, ID)
				rpc <- true

			}(ck.lastContactedServer)
			go func() {
				time.Sleep(300 * time.Millisecond)
				timeout <- true
			}()
			select {
			case <-timeout:
				timeoutCnt++
				if timeoutCnt == 1 {
					ck.lastContactedServer = (ck.lastContactedServer + 1) % len(ck.servers)
					timeoutCnt = 0
				}
			case <-rpc:
				if wrongLeader == false {
					//DPrintf("Right leader, result is %v", rpcResult)
					valid = true
					break
				}
				ck.lastContactedServer = (ck.lastContactedServer + 1) % len(ck.servers)
				timeoutCnt = 0
			}
			if valid {
				result = rpcResult
				break
			}
			//DPrintf("Timeout, resend to node %v command %v %v", ck.lastContactedServer, operation, ID)
		}

		DPrintf("Result for %v and ID %v is %v", operation, ID, result)
		operation.outChannel <- result
		ck.operationID++
	}
}
