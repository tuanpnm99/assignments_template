package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	LEADER                  = "leader"
	FOLLOWER                = "follower"
	CANDIDATE               = "candidate"
	RPCTimeoutInMs          = 20
	ElectionTimeoutBaseInMs = 50
	HeartbeatIntervalInMs   = 30
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex

	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentState          string
	currentTerm           int
	votedFor              int
	lastReceivedHeartbeat time.Time
	//log []interface{}

	// commitIndex int
	// lastApplied int
	// nextIndex   []int
	// matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = false
	if rf.currentState == LEADER {
		isleader = true
	}
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateID int
	// lastLogIndex int
	// lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	CurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// use lock to avoid race condition with election
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var result bool
	// grant vote only if the term is higher than the current term
	//
	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		// the node can be in the middle of election or currently a leader
		// so if we grant the vote, it is critical to convert to follower
		// to avoid being in an election/or leader and grant vote at the same time
		rf.currentState = FOLLOWER
		result = true
	} else {
		result = false
	}
	DPrintf("Node %v receives requestVote from %v in term %v and grantedVote is %v", rf.me, args.CandidateID, args.Term, result)
	reply.VoteGranted = result
	reply.CurrentTerm = Max(rf.currentTerm, args.Term)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here.
	Term int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Success bool
}

// make RPC call to append entry
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// append entry RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Node %v receive append entries with term %v and its current term is %v", rf.me, args.Term, rf.currentTerm)
	// discard append entry with lower term
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term == rf.currentTerm && rf.currentState == LEADER {
		DPrintf("ERROR: 2 leader get elected in the same term %v", rf.currentTerm)
		reply.Success = false
		return
	}
	// if receive a valid append entry rpc, convert to follower
	rf.lastReceivedHeartbeat = time.Now()
	rf.currentState = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1
	reply.Success = true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentState = FOLLOWER
	rf.lastReceivedHeartbeat = time.Now()

	// lauch the main raft routine that manage the raft node (i.e perform leader election, send heartbeats, etc.)
	go raftRountine(rf)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func startElection(raft *Raft, wg *sync.WaitGroup) {
	defer wg.Done()

	// it is important to use lock here to make sure there is no race condition
	// when incrementing currentTerm
	raft.mu.Lock()
	raft.currentTerm++
	newTerm := raft.currentTerm
	raft.votedFor = raft.me
	raft.mu.Unlock()

	totalVotes, totalPeers := 1, len(raft.peers)
	out := make(chan int, totalPeers-1)
	arg := RequestVoteArgs{newTerm, raft.me}

	DPrintf("Node %v about to request votes for term %v from %v peers >>>", raft.me, newTerm, totalPeers-1)
	// send vote request for all peers
	for server := 0; server < totalPeers; server++ {
		if server == raft.me { //skip it self
			continue
		}

		go func(s int) {
			timeout := make(chan bool, 1)
			rpc := make(chan bool, 1)
			rpcResult := make(chan int, 1)
			go func() { //lauch a timeout go routine
				time.Sleep(RPCTimeoutInMs * time.Millisecond)
				timeout <- true
			}()
			go func() { // make rpc call to peer
				reply := &RequestVoteReply{}
				if raft.sendRequestVote(s, arg, reply) == true && reply.VoteGranted == true {
					rpcResult <- 1
				} else {
					rpcResult <- 0
				}
				rpc <- true
			}()
			// select what comes first either timeout routine or rpc routine
			select {
			case <-rpc:
				r := <-rpcResult
				out <- r
			case <-timeout: // treat timeout as fail rpc
				out <- 0
			}
		}(server)
	}
	for i := 0; i < totalPeers-1; i++ { // count all the vote from channel
		totalVotes += <-out
	}
	DPrintf("Node %v got %v votes for term %v election", raft.me, totalVotes, newTerm)
	// Make sure the state is consistent before and after receiving vote replies
	if raft.currentState != CANDIDATE || raft.currentTerm != newTerm {
		DPrintf("Node %v aborts election with state %v and term %v", raft.me, raft.currentState, raft.currentTerm)
		return
	}

	if totalVotes*2 > totalPeers { // Win election with majority votes
		raft.currentState = LEADER
		DPrintf("Node %v becomes leader", raft.me)
	} else { // If the candidate doesn't get the majority votes, reset timeout
		electionTimeout := ElectionTimeoutBaseInMs + rand.Intn(ElectionTimeoutBaseInMs)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	}
}

func convertToCandidate(raft *Raft, wg *sync.WaitGroup) {
	defer wg.Done()

	electionTimeout := ElectionTimeoutBaseInMs + rand.Intn(ElectionTimeoutBaseInMs)
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	// If receive valid heartbeat during the sleep, remain as follower
	if time.Now().Sub(raft.lastReceivedHeartbeat) < time.Duration(electionTimeout)*time.Millisecond {
		return
	}
	raft.currentState = CANDIDATE
}

func sendHeartbeat(raft *Raft, wg *sync.WaitGroup) { // add term to make sure don't send heartbeat in different term
	defer wg.Done()
	if raft.currentState != LEADER {
		return
	}
	currentTerm := raft.currentTerm
	DPrintf("Node %v sending heartbeats in term %v", raft.me, currentTerm)

	//Send heartbeats
	arg := AppendEntriesArgs{currentTerm}
	for server := 0; server < len(raft.peers); server++ {
		if server == raft.me {
			continue
		}
		go func(s int) {
			raft.sendAppendEntries(s, arg, &AppendEntriesReply{})
		}(server)
	}
	time.Sleep(HeartbeatIntervalInMs * time.Millisecond)
}

func raftRountine(raft *Raft) {
	// use wait group to synchronize between different go routines with the main raft routine
	wg := sync.WaitGroup{}
	prev := raft.currentState
	for {
		wg.Add(1)
		if raft.currentState != prev {
			DPrintf("Node %v in raft routine with state %v in term %v", raft.me, raft.currentState, raft.currentTerm)
			prev = raft.currentState
		}
		if raft.currentState == LEADER {
			go sendHeartbeat(raft, &wg)
		} else if raft.currentState == FOLLOWER {
			go convertToCandidate(raft, &wg)
		} else { // candidate
			go startElection(raft, &wg)
		}
		wg.Wait()
	}
}
