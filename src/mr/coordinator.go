package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"
import "strings"
import "io/ioutil"

type Coordinator struct {
	nReduces int
	// iFiles int // not needed anymore
	inpFiles []string
	inpFilesStatus []TaskStatus
	inpFilesMut sync.Mutex
	intrFilesStatus []TaskStatus
	intrFilesMut sync.Mutex
	msTimeout int64
	reduceComplete bool
	reduceCompleteMut sync.Mutex
}


type TaskStatus struct {
	status int // 0: not started
			   // 1: in progress
			   // 2: complete
	startTime time.Time
}


func NewCoordinator(msTimeout int64, nReduces int, inpFiles []string) Coordinator {
	inpFilesStatus := make([]TaskStatus, len(inpFiles))
	for i := 0; i < len(inpFiles); i++ {
		inpFilesStatus[i].status = 0
		inpFilesStatus[i].startTime = time.Now()
	}
	intrFilesStatus := make([]TaskStatus, nReduces)
	for i := 0; i < nReduces; i++ {
		intrFilesStatus[i].status = 0
		intrFilesStatus[i].startTime = time.Now()
	}
	return Coordinator{
		nReduces : nReduces,
		inpFiles : inpFiles,
		inpFilesStatus : inpFilesStatus,
		intrFilesStatus : intrFilesStatus,
		msTimeout : msTimeout,
		reduceComplete : false}
}


// RPC handlers for the worker to call.

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


func (c *Coordinator) RequestReduceTask(args *RequestReduceTaskArgs, 
										reply *RequestReduceTaskReply) error {
	c.intrFilesMut.Lock()
	c.TimeoutTasks(c.intrFilesStatus)
	iReduce, reduceComplete := c.GetTask(c.intrFilesStatus)
	reply.IReduce = iReduce
	reply.Complete = reduceComplete
	if reduceComplete {
		c.reduceCompleteMut.Lock()
		c.reduceComplete = true
		c.reduceCompleteMut.Unlock()
	}
	c.intrFilesMut.Unlock()
	return nil

}


func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, 
										reply *CompleteReduceTaskReply) error {
	fmt.Printf("completed reduce %v\n", args.IReduce)
	c.intrFilesStatus[args.IReduce].status = 2
	return nil
}


func (c *Coordinator) RequestMappingTask(
						args *RequestMappingTaskArgs, 
						reply *RequestMappingTaskReply) error {
	c.inpFilesMut.Lock()
	// c.PrintStatus()
	c.TimeoutTasks(c.inpFilesStatus)
	iFile, mappingComplete := c.GetTask(c.inpFilesStatus) 
	if mappingComplete {
		reply.Filename = ""
		reply.Complete = true
		reply.IFile = -1
	} else {
		if iFile == -1 { 
			reply.Filename = ""
		} else {
			reply.Filename = c.inpFiles[iFile]
		}
		reply.Complete = false
		reply.IFile = iFile
	}
	c.inpFilesMut.Unlock()
	return nil
}

//
// when a worker completes its mapping task it calls this.
// sets mapping task status to complete
//
func (c* Coordinator) CompleteMappingTask(
						args *CompleteMappingTaskArgs, 
						reply *CompleteMappingTaskReply) error {
	fmt.Printf("completed mapping %v\n", args.IFile)
	c.inpFilesStatus[args.IFile].status = 2
	return nil
}



//
// New section
//


//
// finds first mapping task that is not started
//
func (c *Coordinator) GetTask(statusArr []TaskStatus) (int, bool) {
	inProgress := false
	for iFile := 0; iFile < len(statusArr); iFile++ {
		if statusArr[iFile].status == 0 {
			statusArr[iFile].status = 1
			statusArr[iFile].startTime = time.Now()
			return iFile, false
		} else if statusArr[iFile].status == 1 {
			inProgress = true
		}
	}
	if inProgress {
		// no statuses are 0 but also not all 2 (some in progress)
		return -1, false
	}
	return -1, true
}

//
// set all in progress mapping statuses that are over
// timeout threshold to not started
//
func (c *Coordinator) TimeoutTasks(statusArr []TaskStatus) {
	for iFile := 0; iFile < len(statusArr); iFile ++ {
		var timeTaken int64 = time.Now().Sub(statusArr[iFile].startTime).Milliseconds()
		if statusArr[iFile].status == 1 && timeTaken >= c.msTimeout {
			statusArr[iFile].status = 0
		}
	}
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
	c.reduceCompleteMut.Lock()
	defer c.reduceCompleteMut.Unlock()
	return c.reduceComplete
}


func (c *Coordinator) PrintStatus() {
	fmt.Printf("inp file statuses:  ")
	for i := 0; i < len(c.inpFilesStatus); i++ {
		fmt.Printf("%v, ", c.inpFilesStatus[i].status)
	}
	fmt.Printf("\n")
}


func deleteFilesPrefix(prefix string) {
	files, err := ioutil.ReadDir(".")
    if err != nil {
        fmt.Println(err)
        return
    }
	for _, file := range files {
        if strings.HasPrefix(file.Name(), prefix) {
            err := os.Remove(file.Name())
			if err != nil {
				log.Fatal(err)
			}
        }
    }
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(inpFiles []string, nReduce int) *Coordinator {
	deleteFilesPrefix("mr-out-")
	var msTimeout int64 = 10000
	c := NewCoordinator(msTimeout, nReduce, inpFiles)
	c.server()
	return &c
}
