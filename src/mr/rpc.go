package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here.

type ClientId = string

// 定义枚举类型
type TaskType int

const (
	TypeMap TaskType = iota
	TypeReduce
	TypeDown
	TypeWait
)

type AskTaskArgs struct {
	Id          ClientId
	PreTaskType TaskType
}

type AskTaskReply struct {
	TType TaskType
}

type AskMapArgs struct {
	Id ClientId
}

type AskMapReply struct {
	ReduceNum     int
	InputFilename []string
	// Lines         []string
}

type AskReduceArgs struct {
	Id ClientId
}

type AskReduceReply struct {
	CompleteReduceTaskId int
	IntermediateFiles    []string
}

type CompleteTaskArgs struct {
	Id                   ClientId
	TType                TaskType
	InputFileNames       []string
	CompleteMapFileNames []string
	CompleteReduceTaskId *int
}

type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
