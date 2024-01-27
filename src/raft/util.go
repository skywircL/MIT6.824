package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func CheckTermIsFirst(index int) bool {
	if index == -1 {
		return true
	}
	return false
}

func (rf *Raft) CheckPreLogIndex(args *AppendEntriesArgs) bool {
	if CheckTermIsFirst(args.PrevLogIndex) {
		return true
	}
	if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		return true
	}
	return false
}
