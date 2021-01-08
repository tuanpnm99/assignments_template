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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	LEADER                  = "leader"
	FOLLOWER                = "follower"
	CANDIDATE               = "candidate"
	RPCTimeoutInMs          = 50
	ElectionTimeoutBaseInMs = 600
	HeartbeatIntervalInMs   = 200
	ApplyLogIntervalInMs    = 100
	MaxEntriesToSend        = 1000
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type PersistRaftState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex
	//logLock syn.Mutex
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

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := false
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
	rf.mu.Lock()
	saveState := PersistRaftState{rf.currentTerm, rf.votedFor, rf.log}
	rf.mu.Unlock()
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(saveState); err != nil {
		DPrintf("WARNING: Encode error msg: %v", err)
	}
	rf.persister.SaveRaftState(buffer.Bytes())
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
	buffer := bytes.NewBuffer(data)
	encoder := gob.NewDecoder(buffer)
	object := &PersistRaftState{0, -1, []LogEntry{LogEntry{-1, nil}}}
	if err := encoder.Decode(object); err != nil {
		DPrintf("WARNING: Decode error msg: %v", err)
	}
	rf.currentTerm = object.CurrentTerm
	rf.votedFor = object.VotedFor
	rf.log = object.Log
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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
	//DPrintf("Node %v got request vote from %v with last log term %v and last log index %v", rf.me, args.CandidateID, args.LastLogTerm, args.LastLogIndex)

	isOlderTermLog := args.LastLogTerm > rf.log[len(rf.log)-1].Term
	isLongerLog := args.LastLogTerm == rf.log[len(rf.log)-1].Term && len(rf.log) <= args.LastLogIndex+1
	moreUpToDateLog := isOlderTermLog || isLongerLog
	result := false

	// convert to follower and update term if the vote is from older term
	// this make sure we don't stuck in the case the node which can be a leader gets behind some lagged node with higher term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.votedFor = -1
		// grant vote only if the term is higher than the current term and with older log
		if moreUpToDateLog {
			rf.votedFor = args.CandidateID
			result = true
		}
	}
	//DPrintf("Node %v receives requestVote from %v in term %v and grantedVote is %v", rf.me, args.CandidateID, args.Term, result)
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
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term                  int
	Success               bool
	ConflictTerm          int
	FirstConflictLogIndex int
}

// make RPC call to append entry
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// append entry RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Node %v receive append entries with term %v and its current term is %v", rf.me, args.Term, rf.currentTerm)
	// discard append entry with lower term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = Max(rf.currentTerm, args.Term)
	reply.Success = false

	if rf.currentState == LEADER && rf.currentTerm == args.Term {
		DPrintf("FATAL: more than 1 leaders in same term %v", args.Term)
		return
	}

	if rf.currentTerm > args.Term {
		DPrintf("Node %v is receives a RPC from lower term leader node %v", rf.me, args.LeaderID)
		return
	}

	if args.PrevLogIndex >= len(rf.log) { // incompatible previous log
		reply.ConflictTerm = -1
		reply.FirstConflictLogIndex = len(rf.log) - 1
		DPrintf("Node %v received RPC but log too short", rf.me)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Node %v received RPC but it's conflict", rf.me)
		conflictTerm := rf.log[args.PrevLogIndex].Term
		firstConflictLogIdx := args.PrevLogIndex
		for rf.log[firstConflictLogIdx-1].Term == conflictTerm {
			firstConflictLogIdx--
		}
		reply.ConflictTerm = conflictTerm
		reply.FirstConflictLogIndex = firstConflictLogIdx
		return
	}
	//DPrintf("Node %v received valid append entries from %v", rf.me, args.LeaderID)
	// if receive a valid append entry rpc, always convert to follower
	rf.lastReceivedHeartbeat = time.Now()
	rf.currentState = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1
	reply.Success = true

	if len(args.Entries) == 0 {
		rf.commitIndex = Max(Min(args.LeaderCommit, args.PrevLogIndex), rf.commitIndex)
		//DPrintf("Node %v have commit index %v", rf.me, rf.commitIndex)
		return
	}

	// rf.logLock.Lock()
	// defer rf.logLock.Unlock()
	curLogIndex := args.PrevLogIndex + 1
	if curLogIndex < len(rf.log) && rf.log[curLogIndex].Term != args.Term {
		rf.log = rf.log[:curLogIndex]
	}
	spaceToAppendEntry := Max(0, len(args.Entries)-(len(rf.log)-curLogIndex))
	for i := 0; i < spaceToAppendEntry; i++ {
		rf.log = append(rf.log, LogEntry{})
	}
	for i := 0; i < len(args.Entries); i++ {
		rf.log[i+curLogIndex] = args.Entries[i]
	}
	if args.LeaderCommit > rf.commitIndex {
		Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		//DPrintf("Node %v have commit index %v", rf.me, rf.commitIndex)
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := (rf.currentState == LEADER)

	if isLeader == true {
		index = len(rf.log)
		DPrintf("Node %v received a new command at term %v index %v", rf.me, term, index)
		rf.log = append(rf.log, LogEntry{term, command})

	}
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
	rf.currentState = FOLLOWER
	rf.lastReceivedHeartbeat = time.Now()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// lauch the main raft routine that manage the raft node (i.e perform leader election, send heartbeats, etc.)
	go raftRountine(rf)

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
	lastLogIndex := len(raft.log) - 1
	lastLogTerm := raft.log[lastLogIndex].Term
	raft.mu.Unlock()

	totalVotes, totalPeers := 1, len(raft.peers)
	out := make(chan int, totalPeers-1)
	arg := RequestVoteArgs{newTerm, raft.me, lastLogIndex, lastLogTerm}

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
	currentState, currentTerm := raft.getCurrentStateAndTerm()
	if currentState != CANDIDATE || currentTerm != newTerm {
		DPrintf("Node %v aborts election with state %v and term %v", raft.me, currentState, currentTerm)
		return
	}

	if totalVotes*2 > totalPeers { // Win election with majority votes
		raft.setCurrentState(LEADER)

		DPrintf("Node %v becomes leader", raft.me)
		lastLogIndex := len(raft.log)
		for i := 0; i < totalPeers; i++ {
			raft.nextIndex[i] = lastLogIndex
			raft.matchIndex[i] = 0
		}
		//raft.Start(nil) // no-op command
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
	if time.Now().Sub(raft.getLastHeartbeatTime()) < time.Duration(electionTimeout)*time.Millisecond {
		return
	}
	raft.setCurrentState(CANDIDATE)
}

func sendHeartbeat(raft *Raft, wg *sync.WaitGroup) { // add term to make sure don't send heartbeat in different term
	defer wg.Done()

	state, term := raft.getCurrentStateAndTerm()
	if state != LEADER || term != raft.currentTerm {
		return
	}

	//DPrintf("Node %v sending AppendEntry in term %v", raft.me, term)
	matchIndices := []int{}

	//Send heartbeats
	for server := 0; server < len(raft.peers); server++ {
		if server == raft.me {
			continue
		}
		matchIndices = append(matchIndices, raft.matchIndex[server])
		go func(s int) {
			raft.mu.Lock()
			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderID = raft.me
			args.PrevLogIndex = Min(len(raft.log)-1, raft.nextIndex[s]-1)
			args.LeaderCommit = raft.commitIndex
			args.PrevLogTerm = raft.log[args.PrevLogIndex].Term
			numEntriesToSend := Max(0, Min(MaxEntriesToSend, len(raft.log)-args.PrevLogIndex-1))
			args.Entries = raft.log[args.PrevLogIndex+1 : args.PrevLogIndex+1+numEntriesToSend]
			raft.mu.Unlock()

			reply := &AppendEntriesReply{}
			if raft.sendAppendEntries(s, args, reply) == false {
				return
			}

			currentState, currentTerm := raft.getCurrentStateAndTerm()
			if currentState != LEADER || currentTerm != term {
				return
			}
			raft.mu.Lock()
			defer raft.mu.Unlock()
			if reply.Term > args.Term {
				raft.currentState = FOLLOWER
				raft.currentTerm = reply.Term
				return
			}

			if reply.Success == true && len(args.Entries) > 0 {
				raft.nextIndex[s] += len(args.Entries)
				raft.matchIndex[s] = Max(0, raft.nextIndex[s]-1)
			} else if reply.Success == false && reply.Term == args.Term {
				if reply.ConflictTerm == -1 || raft.log[reply.FirstConflictLogIndex].Term == reply.ConflictTerm {
					raft.nextIndex[s] = reply.FirstConflictLogIndex + 1
				} else {
					raft.nextIndex[s] = reply.FirstConflictLogIndex
				}
			}
		}(server)
	}
	raft.checkCommitIndex(matchIndices, term)
	time.Sleep(HeartbeatIntervalInMs * time.Millisecond)
}

func (rf *Raft) checkCommitIndex(matchIndices []int, term int) {
	// find the majority match index
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchIndices = append(matchIndices, len(rf.log)-1)
	sort.Ints(matchIndices)
	majorityLogIndex := matchIndices[len(matchIndices)/2]

	if majorityLogIndex > rf.commitIndex && majorityLogIndex < len(rf.log) && rf.log[majorityLogIndex].Term == term {
		rf.commitIndex = majorityLogIndex
		DPrintf("Node %v, new commit index %v from %v and log len %v", rf.me, majorityLogIndex, matchIndices, len(rf.log))
	}
}

func raftRountine(raft *Raft) {

	go raft.applyLog()
	// use wait group to synchronize between different go routines with the main raft routine
	wg := sync.WaitGroup{}
	prev, _ := raft.getCurrentStateAndTerm()
	for {
		wg.Add(1)
		currentState, currentTerm := raft.getCurrentStateAndTerm()
		if currentState != prev {
			DPrintf("Node %v in raft routine with state %v in term %v", raft.me, currentState, currentTerm)
			prev = currentState
		}
		if currentState == LEADER {
			go sendHeartbeat(raft, &wg)
		} else if currentState == FOLLOWER {
			go convertToCandidate(raft, &wg)
		} else { // candidate
			go startElection(raft, &wg)
		}
		wg.Wait()
	}
}

func (rf *Raft) applyLog() {
	for {
		commitIndex := rf.getCommitIndex()
		rf.persist()
		for commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{}
			msg.Index = rf.lastApplied
			rf.mu.Lock()
			msg.Command = rf.log[rf.lastApplied].Command
			rf.mu.Unlock()
			rf.applyCh <- msg
			DPrintf("Node %v done applying log at index %v", rf.me, rf.lastApplied)
		}
		time.Sleep(ApplyLogIntervalInMs * time.Millisecond)
	}
}

func (rf *Raft) setCurrentState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentState = state
}

func (rf *Raft) getCurrentStateAndTerm() (string, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentState, rf.currentTerm
}

func (rf *Raft) getLastHeartbeatTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastReceivedHeartbeat
}

func (rf *Raft) setCommitIndex(idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = idx
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}
