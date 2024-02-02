package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) GetFirstIndex() int {
	return rf.lastIncludedIndex + 1
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) CheckTermIsFirst(index int) bool {
	if rf.lastIncludedIndex == -1 {
		if index == -1 {
			return true
		}
	} else {
		if index == rf.lastIncludedIndex {
			return true
		}
	}
	return false
}

func (rf *Raft) GetFirstTerm() int {
	if rf.lastIncludedIndex == -1 {
		return 0
	}
	return rf.lastIncludedTerm
}

func (rf *Raft) GetLastTerm() int {
	if rf.lastIncludedIndex == -1 {
		return rf.log[len(rf.log)-1].Term
	}

	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}

	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetAllLogLen() int {
	if rf.lastIncludedIndex == -1 {
		return len(rf.log)
	}
	return rf.lastIncludedIndex + len(rf.log) + 1
}

func (rf *Raft) CheckPreLogIndex(args *AppendEntriesArgs) bool {
	if rf.CheckTermIsFirst(args.PrevLogIndex) {
		return true
	}
	if args.PrevLogIndex >= rf.GetFirstIndex() && args.PrevLogIndex < rf.GetAllLogLen() && rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term == args.PrevLogTerm {
		return true
	}
	return false
}
