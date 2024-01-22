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

	applyCh chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type Entries struct {
	Term    int
	Command interface{} //machine command
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
	Term            int //让leader自我更新 ？
	Success         bool
	FastGoBackIndex int //快速回退index
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
	rf.state = Follower //  ok ?
	if len(args.Entries) == 0 {
		rf.IsGetHeartbeat <- 0
		reply.Success = true
		reply.Term = rf.currentTerm
		//必须通过提交来比

		if len(rf.log)-1 > args.PrevLogIndex { //没有共识的leader
			rf.log = rf.log[:args.PrevLogIndex+1]
			reply.Success = false
			reply.FastGoBackIndex = len(rf.log)
			return
		}

		if len(rf.log)-1 < args.PrevLogIndex { //没有共识的leader
			reply.Success = false
			reply.FastGoBackIndex = len(rf.log)
			return
		}

		if len(rf.log)-1 == args.PrevLogIndex {
			if !CheckTermIsFirst(len(rf.log) - 1) {
				if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					reply.Success = false
					rf.log = rf.log[:args.PrevLogIndex]
					reply.FastGoBackIndex = rf.commitIndex
					return
				}
			}
		}

		fmt.Println("know lastApplied", rf.me, rf.lastApplied, len(rf.log), args.PrevLogIndex)
		for rf.lastApplied < args.LeaderCommit {
			rf.lastApplied++
			aMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- aMsg
			rf.commitIndex = rf.lastApplied
		}

	} else { //如果不为空，不为心跳消息
		rf.IsGetHeartbeat <- 0
		if len(rf.log)-1 > args.PrevLogIndex {
			rf.log = rf.log[:args.PrevLogIndex+1]
			reply.Success = false
			reply.FastGoBackIndex = len(rf.log)
			return
		}

		if len(rf.log)-1 < args.PrevLogIndex {
			reply.Success = false
			reply.FastGoBackIndex = len(rf.log)
			return
		}

		if len(rf.log)-1 == args.PrevLogIndex {
			if !CheckTermIsFirst(len(rf.log) - 1) {
				if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					reply.Success = false
					rf.log = rf.log[:args.PrevLogIndex]
					reply.FastGoBackIndex = rf.commitIndex
					return
				}
			}
			reply.Success = true
			rf.log = append(rf.log, args.Entries...)
		}
	}
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//发给所有的node
	if rf.killed() {
		return
	}
	CommitSum := 1
	for i, v := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(v *labrpc.ClientEnd, i int, CommitSum *int, args AppendEntriesArgs, reply AppendEntriesReply) {
			rf.mu.Lock()
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if CheckTermIsFirst(args.PrevLogIndex) {
				args.PrevLogTerm = 0
			} else {
				fmt.Println("who send: ", rf.me, rf.nextIndex, rf.currentTerm, i)
				args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term // maybe have error in here
			}

			if rf.nextIndex[i] < len(rf.log) {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			// 按道理来说的话不应该出现nextIndex要大于log的长度
			rf.mu.Unlock()

			fmt.Println("Raft.AppendEntries", rf.nextIndex, args, i, rf.log, rf.commitIndex)
			ok := v.Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				ok = v.Call("Raft.AppendEntries", &args, &reply)
			}
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Success {
					//关于commit 持久化
					fmt.Println("reply.Success: ", rf.me, i, len(args.Entries), rf.nextIndex[i])

					if args.LeaderCommit == rf.commitIndex {
						*CommitSum++
					}
					rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
					rf.matchIndex[i] = rf.nextIndex[i] - 1

					if *CommitSum >= len(rf.peers)/2+1 { //commit
						*CommitSum = 0
						for rf.lastApplied < rf.nextIndex[i] {
							rf.lastApplied++
							aMsg := ApplyMsg{
								CommandValid: true,
								Command:      rf.log[rf.lastApplied-1].Command,
								CommandIndex: rf.lastApplied,
							}
							rf.applyCh <- aMsg
							rf.commitIndex = rf.lastApplied
							rf.nextIndex[rf.me] = rf.commitIndex
						}
					}

				} else {

					if reply.Term > rf.currentTerm { //如果leader的term小于某个节点，那么会强行提升term与领先一致，变为follower，解决离群节点重恢复term过大的问题
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.isleader = false
						return
					}
					fmt.Println("FastGoBackIndex: ", rf.me, i, reply.FastGoBackIndex, rf.currentTerm, reply.Term, rf.nextIndex)
					rf.nextIndex[i] = reply.FastGoBackIndex
				}
			}
		}(v, i, &CommitSum, *args, *reply)

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
	//进行投票 , 同步term，如果candidate的term比自己大
	//会重置election timeout

	if rf.killed() {
		reply.Votegranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//当出现有多个candidate的情况，怎么根据任期来解决这个问题
	if args.Term > rf.currentTerm { //什么情况下会有相同的term，同为candidate或着接收到vote的follower
		rf.IsGetHeartbeat <- 0
		rf.isleader = false
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		//先看log再投票
		lv := rf.logVote(args)
		if lv {
			rf.votedFor = args.CandidateId
			reply.Votegranted = true
		}

	} else {
		if args.Term == rf.currentTerm { //如果是投给的那个人来要票，那就再给他投一次
			lv := rf.logVote(args)
			if (rf.votedFor == args.CandidateId || rf.votedFor == -1) && lv {
				rf.IsGetHeartbeat <- 0
				rf.votedFor = args.CandidateId
				reply.Votegranted = true

			}

		} else {
			reply.Votegranted = false

		}

	}

	reply.Term = rf.currentTerm
	return
}

// logVote 日志检查
func (rf *Raft) logVote(args *RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		return false
	} else {
		if rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex {
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
		en := Entries{Term: term, Command: command}
		rf.log = append(rf.log, en)
		fmt.Printf("[start]:me: %d,log:%v\n", rf.me, rf.log)
	} else {
		return -1, term, isLeader
	}

	return len(rf.log), term, isLeader
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

	Ticker := time.NewTicker(time.Duration(150+(rand.Int63()%200)) * time.Millisecond)
	rf.Ticker = Ticker
	rf.log = make([]Entries, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//再启一个gorutine，用于election timeout
	go func() {

		for {
			select {
			case <-Ticker.C: //heartbeat timeout
				//开始选举

				rf.Ticker.Reset(time.Duration(150+(rand.Int63()%200)) * time.Millisecond)
				if rf.state == Follower || rf.state == Candidate {
					rf.state = Candidate
					rf.currentTerm = rf.currentTerm + 1
					llIndex := 0
					llTerm := 0
					if len(rf.log) == 0 {
						llIndex = -1
						llTerm = 0
					} else {
						llIndex = len(rf.log) - 1
						llTerm = rf.log[len(rf.log)-1].Term
					}

					args := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: llIndex,
						LastLogTerm:  llTerm,
					}

					//先投自己一票
					rf.votedFor = me
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

							fmt.Println(rf.me, reply, rf.isleader, rf.state, rf.currentTerm)
							if ok {
								rf.mu.Lock()
								defer rf.mu.Unlock()
								if reply.Term > rf.currentTerm {

									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.state = Follower

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
										fmt.Println("[election leader]", rf.me)

										for i := range rf.nextIndex {
											rf.nextIndex[i] = len(rf.log)
											rf.matchIndex[i] = 0
										}

										args := &AppendEntriesArgs{Term: rf.currentTerm,
											LeaderId:     rf.me,
											PrevLogIndex: len(rf.log) - 1,
											PrevLogTerm:  0,
										}

										reply := &AppendEntriesReply{}
										go rf.sendAppendEntries(args, reply)

									}
								}
							}
						}(args, v, &count)

					}

				}

			case <-rf.IsGetHeartbeat: //收到心跳
				rf.Ticker.Reset(time.Duration(150+(rand.Int63()%200)) * time.Millisecond)
			}
		}
	}()

	return rf
}
