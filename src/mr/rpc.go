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

// MessageType
const (
	MsgForStartTask = iota
	MsgForFinishMap
	MsgForFinishReduce
)

type MyArgs struct {
	MachineName string
	MessageType int
	FileName    string
}

// send intermediate files' filename to master
type MyIntermediateFile struct {
	MachineName string
	Index       int
	FileName    string
}

type MyReply struct {
	Filename           string // get a filename
	MapNumAllocated    int
	NReduce            int
	ReduceNumAllocated int
	TaskType           string
	ReduceFileList     []string // File list about
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
