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



// Add your RPC definitions here.

type Err string

type GetNumReducesArgs struct {}

type GetNumReducesReply struct {
	Err Err
	NumReduces int
}

type GetNumFileArgs struct {}

type GetNumFileReply struct {
	Err Err
	NumFile int
}


type RequestMappingTaskArgs struct {}

type RequestMappingTaskReply struct {
	Err Err
	Filename string
	Complete bool
	IFile int
}

type CompleteMappingTaskArgs struct {
	IFile int
}

type CompleteMappingTaskReply struct {
	Err Err
}



type RequestReduceTaskArgs struct {}

type RequestReduceTaskReply struct {
	Err Err
	IReduce int
	Complete bool
}

type CompleteReduceTaskArgs struct {
	IReduce int 
}

type CompleteReduceTaskReply struct {
	Err Err
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
