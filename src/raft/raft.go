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
	"6.5840/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state State

	// Your data here (2A, 2B, 2C).
	IsGetHeartbeat chan int
	currentTerm    int       // incr default 0
	votedFor       int       // 投票给了谁
	log            []Entries // log Entry
	isleader       bool
	Ticker         *time.Ticker
	nextIndex      []int
	matchIndex     []int
	commitIndex    int
	lastApplied    int
	applyCond      *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int

	applyCh chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) RaftStateSize() int {

	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isleader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var log []Entries
	var voteFor int
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&term) != nil || d.Decode(&log) != nil || d.Decode(&voteFor) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("read persist err")
	} else {
		rf.log = log
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.lastIncludedIndex = lastIncludeIndex
		rf.lastIncludedTerm = lastIncludeTerm
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.GetFirstIndex()
	if index > firstIndex {
		sLogs := make([]Entries, 0)
		rf.lastIncludedIndex = index - 1
		rf.lastIncludedTerm = rf.log[index-firstIndex-1].Term
		rf.log = append(sLogs, rf.log[index-firstIndex:]...)
		DPrintf("%d [snapshot]: rf.log: %v\n", rf.me, rf.log)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.log)
		e.Encode(rf.votedFor)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, snapshot)
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.IsGetHeartbeat <- 0
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.isleader = false
	rf.state = Follower
	if args.LastIncludeIndex < rf.commitIndex {
		return
	}

	rf.commitIndex = args.LastIncludeIndex + 1
	rf.lastApplied = rf.commitIndex

	if rf.commitIndex >= rf.GetAllLogLen() {
		tempLog := make([]Entries, 0)
		rf.log = tempLog
	} else {
		tempLog := make([]Entries, 0)
		DPrintf("install snapshot append lastindex %d,rf.lastindex %d\n", args.LastIncludeIndex, rf.lastIncludedIndex)
		rf.log = append(tempLog, rf.log[args.LastIncludeIndex-rf.lastIncludedIndex:]...)
	}
	rf.lastIncludedTerm = args.LastIncludeTerm
	rf.lastIncludedIndex = args.LastIncludeIndex
	DPrintf("term = %d,get snapshot from %d to %d\n", rf.currentTerm, args.LeaderId, rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludeTerm,
			SnapshotIndex: args.LastIncludeIndex + 1,
		}
	}()
	return
}

func (rf *Raft) SendInstallSnapshot(i int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{}
	reply := &InstallSnapshotReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeTerm = rf.lastIncludedTerm
	args.LastIncludeIndex = rf.lastIncludedIndex
	args.Data = rf.persister.ReadSnapshot()
	DPrintf("SendInstallSnapshot lastIncludedIndex %d to %d\n", rf.lastIncludedIndex, i)
	rf.mu.Unlock()
	ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
	DPrintf("installSnapshot ok = %v\n", ok)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.isleader = false
			rf.state = Follower // 重置选举时间?
			rf.persist()
		}
		newNext := args.LastIncludeIndex + 1
		newMatch := args.LastIncludeIndex
		if newNext > rf.nextIndex[i] {
			rf.nextIndex[i] = newNext
		}
		if newMatch > rf.matchIndex[i] {
			rf.matchIndex[i] = newMatch
		}
	}

}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		firstIndex := rf.GetFirstIndex()
		if rf.lastApplied < firstIndex {
			rf.lastApplied = firstIndex
		}
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			aMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-2-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("rf.lastApplied %d , rf.commitIndex %d ,%d commit %v\n", rf.lastApplied, rf.commitIndex, rf.me, aMsg)
			rf.mu.Unlock()
			rf.applyCh <- aMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

type Entries struct {
	Term    int
	Command interface{} //machine command
	Index   int
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entries
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term            int
	Success         bool
	FastGoBackIndex int //快速回退index
	FastGoBackTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //heartbeat timeout implementation 由leader发送
	// Your code here (2A, 2B).
	//接收到heartbeat，刷新计时器
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	rf.isleader = false
	rf.state = Follower
	rf.persist()
	//if args.PrevLogIndex >= len(rf.log) {
	//	DPrintf("not %d want result\n", rf.me)
	//	reply.FastGoBackTerm = 0
	//	reply.Term = rf.currentTerm
	//	reply.Success = false
	//	return
	//}

	if len(args.Entries) == 0 {
		rf.IsGetHeartbeat <- 0
		reply.Success = true
		reply.Term = rf.currentTerm

		curIndex := len(rf.log) + rf.lastIncludedIndex
		if rf.lastIncludedIndex > args.PrevLogIndex {
			reply.Success = false
			reply.FastGoBackIndex = rf.GetLastIndex() + 1
			return
		}
		if curIndex > args.PrevLogIndex {
			if rf.CheckTermIsFirst(args.PrevLogIndex) {
				reply.FastGoBackTerm = 0
				reply.FastGoBackIndex = 0
			} else {
				reply.FastGoBackTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
				index := args.PrevLogIndex - 1
				for index >= rf.GetFirstIndex() && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
					index--
				}
				reply.FastGoBackIndex = index + 1
			}
			reply.Success = false
			return
		}

		if curIndex < args.PrevLogIndex {
			reply.Success = false
			reply.FastGoBackTerm = 0
			reply.FastGoBackIndex = curIndex
			return
		}

		if curIndex == args.PrevLogIndex {
			if !rf.CheckTermIsFirst(args.PrevLogIndex) {
				if rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm {
					reply.Success = false
					reply.FastGoBackTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
					index := args.PrevLogIndex - 1
					for index >= rf.GetFirstIndex() && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
						index--
					}
					reply.FastGoBackIndex = index + 1
					return
				}
			}
		}

		DPrintf("know lastApplied me; %d,lastapp: %d, len: %d,index ;%d log: %v\n", rf.me, rf.lastApplied, len(rf.log), args.PrevLogIndex, rf.log)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Broadcast()

	} else {
		rf.IsGetHeartbeat <- 0
		reply.Term = rf.currentTerm
		if !rf.CheckPreLogIndex(args) {
			curIndex := len(rf.log) + rf.lastIncludedIndex
			if rf.lastIncludedIndex > args.PrevLogIndex {
				reply.Success = false
				reply.FastGoBackIndex = rf.GetLastIndex() + 1
				return
			}

			if curIndex > args.PrevLogIndex {
				if !rf.CheckTermIsFirst(args.PrevLogIndex) {
					DPrintf("%d get args.PrevLogIndex= %d,rf.lastIncludedIndex= %d\n", rf.me, args.PrevLogIndex, rf.lastIncludedIndex)
					reply.FastGoBackTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
					index := args.PrevLogIndex - 1
					for index >= rf.GetFirstIndex() && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
						index--
					}
					reply.FastGoBackIndex = index + 1
				}
				reply.Success = false
				return
			}

			if curIndex < args.PrevLogIndex {
				reply.Success = false
				reply.FastGoBackTerm = 0
				reply.FastGoBackIndex = curIndex
				return
			}

			if curIndex == args.PrevLogIndex {
				if !rf.CheckTermIsFirst(curIndex) {
					if rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm {
						reply.Success = false
						reply.FastGoBackTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
						index := args.PrevLogIndex - 1
						for index >= rf.GetFirstIndex() && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
							index--
						}
						reply.FastGoBackIndex = index + 1
						return
					}
				}
			}

		}
		reply.Success = true
		for index, entry := range args.Entries {
			if entry.Index >= rf.GetFirstIndex() && (entry.Index >= rf.GetAllLogLen() || rf.log[entry.Index-rf.lastIncludedIndex-1].Term != entry.Term) {
				rf.log = append(rf.log[:entry.Index-rf.lastIncludedIndex-1], args.Entries[index:]...)
				break
			}
		}
		rf.persist()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Broadcast()
	}
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//发给所有的node
	if rf.killed() {
		return
	}

	for i, v := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}

		next := rf.nextIndex[i]
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = next - 1

		if next <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.SendInstallSnapshot(i)
			continue
		}

		if rf.CheckTermIsFirst(args.PrevLogIndex) {
			args.Entries = make([]Entries, len(rf.log))
			args.PrevLogTerm = rf.GetFirstTerm()
		} else {
			args.Entries = make([]Entries, rf.GetLastIndex()-args.PrevLogIndex) //?
			DPrintf("%d send: %v,term %d, to %d \n", rf.me, rf.nextIndex, rf.currentTerm, i)
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		}
		if next < rf.GetAllLogLen() {
			copy(args.Entries, rf.log[next-rf.lastIncludedIndex-1:])
		}
		rf.mu.Unlock()
		go func(v *labrpc.ClientEnd, i int, args AppendEntriesArgs, reply AppendEntriesReply) {
			DPrintf("Raft.AppendEntries %d, %v,%v,%v,%v", rf.nextIndex, args, i, args.Entries, rf.commitIndex)
			ok := v.Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if reply.Success {

					DPrintf("reply.Success: %d send to %d,len:%d,nextindex = %v", rf.me, i, len(args.Entries), rf.nextIndex)
					newNext := args.PrevLogIndex + len(args.Entries) + 1
					newMatch := args.PrevLogIndex + len(args.Entries)
					if newNext > rf.nextIndex[i] {
						rf.nextIndex[i] = newNext
					}
					if newMatch > rf.matchIndex[i] {
						rf.matchIndex[i] = newMatch
					}

					for end := len(rf.log) - 1; end >= 0; end-- {
						if rf.log[end].Term != rf.currentTerm {
							break
						}
						n := 1
						for k := range rf.matchIndex {
							if rf.me != k && rf.matchIndex[k] >= end+rf.lastIncludedIndex+1 {
								n++
							}
						}
						DPrintf("end = %d,n = %d\n", end, n)

						if n > len(rf.peers)/2 {
							rf.commitIndex = end + rf.lastIncludedIndex + 2
							DPrintf("commitIndex = %d\n", rf.commitIndex)
							break
						}
					}

					rf.applyCond.Broadcast()
				} else {
					if rf.state != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.isleader = false

						rf.persist()
						return
					} else {
						if reply.Term == rf.currentTerm {
							DPrintf("FastGoBackIndex: me : %d - > %d ,reply.FastGoBackIndex %d,reply.FastGoBackTerm %d,rf.currentTerm %d,reply.Term %d, rf.nextIndex %v rf.matchindex %v", rf.me, i, reply.FastGoBackIndex, reply.FastGoBackTerm, rf.currentTerm, reply.Term, rf.nextIndex, rf.matchIndex)

							if reply.Term == 0 {
								rf.nextIndex[i] = reply.FastGoBackIndex
							} else {
								index := reply.FastGoBackIndex
								if index <= rf.lastIncludedIndex {
									rf.nextIndex[i] = index
								}
								firstIndex := rf.GetFirstIndex()
								if index >= firstIndex && index < rf.GetAllLogLen() && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
									for index >= firstIndex && rf.log[index-rf.lastIncludedIndex-1].Term == reply.FastGoBackTerm {
										index--
									}
									rf.nextIndex[i] = index + 1
								} else {
									rf.nextIndex[i] = reply.FastGoBackIndex

								}
							}
						}
					}

				}
			}
		}(v, i, *args, *reply)

	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	Votegranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) { //candidate 在选举期间发起
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Votegranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.IsGetHeartbeat <- 0
		rf.isleader = false
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		lv := rf.logVote(args)
		if lv {
			rf.votedFor = args.CandidateId
			reply.Votegranted = true
		}

	} else {
		if args.Term == rf.currentTerm { //如果是投给的那个人来要票，那就再给他投一次
			rf.IsGetHeartbeat <- 0
			lv := rf.logVote(args)
			if (rf.votedFor == args.CandidateId || rf.votedFor == -1) && lv {
				rf.votedFor = args.CandidateId
				reply.Votegranted = true

			}

		} else {
			reply.Votegranted = false

		}

	}
	rf.persist()
	reply.Term = rf.currentTerm
	return
}

// logVote 日志检查
func (rf *Raft) logVote(args *RequestVoteArgs) bool {
	if rf.GetAllLogLen() == 0 {
		return true
	}
	lastTerm := rf.GetLastTerm()
	if lastTerm > args.LastLogTerm {
		return false
	} else {
		if lastTerm == args.LastLogTerm && rf.GetAllLogLen()-1 > args.LastLogIndex {
			return false
		}
	}
	return true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.isleader

	if isLeader {
		en := Entries{Term: term, Command: command, Index: len(rf.log) + rf.lastIncludedIndex + 1}
		rf.log = append(rf.log, en)
		rf.nextIndex[rf.me] = len(rf.log) + rf.lastIncludedIndex + 1
		rf.persist()
		DPrintf("[start]:me: %d,log:%v\n", rf.me, rf.log)
	} else {
		return -1, term, isLeader
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	reply := &AppendEntriesReply{}
	go rf.sendAppendEntries(args, reply)

	return rf.GetAllLogLen(), term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.Ticker.Stop()
	rf.mu.Unlock()

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		//在ticker里发心跳
		rf.mu.Lock()
		if rf.state == Leader {
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(args, reply)
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(50 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.IsGetHeartbeat = make(chan int)

	// Your initialization code here (2A, 2B, 2C).
	rf.isleader = false
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = make([]Entries, 0)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	Ticker := time.NewTicker(time.Duration(300+(rand.Int63()%150)) * time.Millisecond)
	rf.Ticker = Ticker
	rf.log = make([]Entries, 0)
	rf.lastIncludedIndex = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	go func() {

		for {
			select {
			case <-Ticker.C:
				//开始选举

				rf.Ticker.Reset(time.Duration(300+(rand.Int63()%150)) * time.Millisecond)
				if rf.state == Follower || rf.state == Candidate {
					rf.state = Candidate
					rf.currentTerm = rf.currentTerm + 1
					llIndex := 0
					llTerm := 0
					if rf.GetAllLogLen() == 0 {
						llIndex = -1
						llTerm = 0
					} else {
						if rf.lastIncludedIndex == -1 {

						}
						if len(rf.log) == 0 {
							llIndex = rf.lastIncludedIndex
							llTerm = rf.lastIncludedTerm
						} else {
							llIndex = rf.GetLastIndex()
							llTerm = rf.GetLastTerm()
						}

					}

					args := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: llIndex,
						LastLogTerm:  llTerm,
					}

					//先投自己一票
					rf.votedFor = me
					rf.persist()
					count := 1
					fmt.Println("[candidate]", rf.me, rf.currentTerm, time.Now())
					for v := range rf.peers {
						if v == rf.me {
							continue
						}
						go func(args *RequestVoteArgs, v int, count *int) {
							reply := RequestVoteReply{}
							var ok bool = false
							ok = rf.sendRequestVote(v, args, &reply)
							fmt.Println("who send vote ", v, reply, rf.isleader, rf.state, rf.currentTerm)
							if ok {
								rf.mu.Lock()

								if reply.Term > rf.currentTerm {

									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.state = Follower
									rf.persist()
								}

								if reply.Votegranted {
									if rf.currentTerm == args.Term {
										*count += 1
									}
									fmt.Println("[count num]", *count, rf.me)
									if *count > len(rf.peers)/2 {
										*count = 0
										rf.isleader = true
										rf.state = Leader

										rf.persist()
										fmt.Println("[election leader]", rf.me)
										for i := range rf.nextIndex {
											rf.nextIndex[i] = rf.GetAllLogLen()
											rf.matchIndex[i] = -1
										}

										args := &AppendEntriesArgs{
											Term:     rf.currentTerm,
											LeaderId: rf.me,
										}

										reply := &AppendEntriesReply{}
										rf.mu.Unlock()
										rf.sendAppendEntries(args, reply)
									} else {
										rf.mu.Unlock()
									}
								} else {
									rf.mu.Unlock()
								}

							}
						}(args, v, &count)

					}

				}

			case <-rf.IsGetHeartbeat: //收到心跳
				rf.Ticker.Reset(time.Duration(300+(rand.Int63()%150)) * time.Millisecond)
			}
		}
	}()

	return rf
}
