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
