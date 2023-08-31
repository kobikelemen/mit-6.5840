package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	// Your definitions here.
	nReduces int
	iFiles int
	files []string
	iFilesMut sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetNumReduces(args *GetNumReducesArgs, 
									reply *GetNumReducesReply) error {
	reply.NumReduces = c.nReduces
	return nil
}

func (c *Coordinator) RequestMappingTask(
						args *RequestMappingTaskArgs, 
						reply *RequestMappingTaskReply) error {
	c.iFilesMut.Lock()
	if c.iFiles >= len(c.files) {
		reply.Filename = ""
		reply.Complete = true
		reply.iFile = -1
	} else {
		reply.Filename = c.files[c.iFiles]
		reply.Complete = false
		reply.iFile = c.iFiles
		c.iFiles ++
	}
	c.iFilesMut.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// Returns when all reduces are finished


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	c := Coordinator{nReduces : nReduce, iFiles : 0, files : files}

	// Your code here.

	c.server()
	return &c
}
