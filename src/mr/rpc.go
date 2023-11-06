package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


type Task struct {
	TaskType   TaskType   
	TaskNum int     //任务的序号
	ReduceSum int //the sum of tasks needed to reduce
	MapSum int // the sum of tasks needed to map
	Filename   string   
}

// TaskArgs rpc应该传入的参数，可实际上应该什么都不用传,因为只是worker获取一个任务
type TaskArgs struct{
	TaskType   TaskType   
	TaskNum int
}

type FinishArgs struct{
	Ts Task
}

type TaskType int

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type State int


const (
	MapTask TaskType = iota
	ReduceTask
	CompleteTask    
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota 
	Waiting            
	Done            
)



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
