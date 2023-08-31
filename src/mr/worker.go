package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
// import "sync"
// import "time"
import "os"
import "io/ioutil"
import "strconv"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func getIntermediateFiles(iFile, nReduces int) []*os.File {
	var intrFiles []*os.File = make([]*os.File, 0)
	for iReduce := 0; iReduce < nReduces; iReduce++ {
		filename := "mr-" + strconv.Itoa(iFile) + "-" + strconv.Itoa(iReduce)
		if _, errExists := os.Stat(filename); errExists == nil {
			// first delete file so contents are reset
			errDel := os.Remove(filename)
			if errDel != nil {
				log.Fatalf("cannot delete %v", filename)
			}
		}
		// recreate file so is empty.
		// this prevents duplicate KV between runs
		file, errCreate := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0755)
		if errCreate != nil {
			log.Fatalf("cannot create %v", filename)
		}
		intrFiles = append(intrFiles, file)
	}
	return intrFiles
}


func getEncorders(intermediateFiles []*os.File) []*json.Encoder {
	encoders := make([]*json.Encoder, 0)
	for _, file := range intermediateFiles {
		encoders = append(encoders, json.NewEncoder(file))
	}
	return encoders
}


func Mapping(mapf func(string, string) []KeyValue,
			 inpFilename string, nReduces int, iFile int) {
	// time.Sleep(time.Duration(1000) * time.Millisecond)
	file, err := os.Open(inpFilename)
	if err != nil {
		log.Fatalf("cannot open %v", inpFilename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inpFilename)
	}
	intrFiles := getIntermediateFiles(iFile, nReduces)
	encoders := getEncorders(intrFiles)
	kva := mapf(inpFilename, string(content))
	for _, kv := range kva {
		iReduce := ihash(kv.Key) % nReduces
		// errEnc := enc.Encode(&kv)
		errEnc := encoders[iReduce].Encode(&kv)
		if errEnc != nil {
			log.Fatalf("cannot encode, err: %v\n", errEnc)
		}
	}
	for _, file := range intrFiles {
		file.Close()
	}
}


func Reduce(reducef func(string, []string) string, *os.File oFile, iReduce, nFile int) {
	
	allIntermediate := []KeyValue{}

	for iFile := 0; iFile < nFile; iFile++ {
		filename := "mr-" + strconv.Itoa(iFile) + "-" + strconv.Itoa(iReduce)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot oen %v\n", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			allIntermediate = append(allIntermediate, kv)
		}
	}

	sort.Sort(ByKey(allIntermediate))

	i := 0
	for i < len(allIntermediate) {
		j := i + 1
		for j < len(allIntermediate) && allIntermediate[j].Key == allIntermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allIntermediate[k].Value)
		}
		output := reducef(allIntermediate[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", allIntermediate[i].Key, output)
		i = j
	}
}


func CallRequestReduceTask() (int, bool) {
	// TODO
	return iReduce, complete
}

func CallCompleteReduceTask(iReduce int) {
	// TODO
}


func CallRequestMappingTask() (string, bool, int) {
	requestMappingTaskArgs := RequestMappingTaskArgs{}
	requestMappingTaskReply := RequestMappingTaskReply{}
	ok := call("Coordinator.RequestMappingTask", 
				&requestMappingTaskArgs, &requestMappingTaskReply)
	inputFilename := requestMappingTaskReply.Filename
	complete := requestMappingTaskReply.Complete
	iFile := requestMappingTaskReply.IFile
	if ok {
		fmt.Printf("Mapping task filename: %v\n", inputFilename)
		fmt.Printf("Mapping tasks complete: %v\n", complete)
	} else {
		fmt.Printf("Failed to request mapping task\n")
	}
	return inputFilename, complete, iFile
}


func CallCompleteMappingTask(iFile int) {
	completeMappingTaskArgs := CompleteMappingTaskArgs{}
	completeMappingTaskArgs.IFile = iFile
	completeMappingTaskReply := CompleteMappingTaskReply{}
	ok := call("Coordinator.CompleteMappingTask",&completeMappingTaskArgs, &completeMappingTaskReply)
	if ok {
		fmt.Printf("Mapping task %v completed\n", iFile)
	} else {
		fmt.Printf("Failed to complete mapping task %v\n", iFile)
	}
}


func CallGetNumReduces() int {
	getNumReducesArgs := GetNumReducesArgs{}
	getNumReducesReply := GetNumReducesReply{}
	ok := call("Coordinator.GetNumReduces", &getNumReducesArgs, 
											&getNumReducesReply)
	if ok {
		nReduces := getNumReducesReply.NumReduces
		fmt.Printf("Worker got nReduces as: %v\n", nReduces)
		return nReduces
	} else {
		fmt.Printf("Failed to get nReduces in Worker\n")
		return -1
	}
}


func CallGetNumFile() int {
	// TODO
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}




//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	
	var nReduces int = CallGetNumReduces()
	var nFile int = CallGetNumFile()
	fmt.Printf("nReduces worker: %v\n", nReduces)
	
	inputFilename, mappingComplete, iFile := CallRequestMappingTask()
	for !mappingComplete {
		if iFile != -1 {
			Mapping(mapf, inputFilename, nReduces, iFile)
			CallCompleteMappingTask(iFile)
		}
		inputFilename, mappingComplete, iFile = CallRequestMappingTask()
	}

	//TODO: create oFile is doesn't exixt

	iReduce, reduceComplete := CallRequestReduceTask()
	for !reduceComplete {
		Reduce(reducef, oFile, iReduce, nFile)
		CallCompleteReduceTask(iReduce)
		iReduce, reduceComplete := CallRequestReduceTask()
	}



	fmt.Printf("mapping completed\n")

	// Plan:
	// - Spin up nMapping threads (need to get nMapping 
	//	 from coordinator, start hard coded tho...)
	// - Each thread makes RPC call to get mapping task 
	// - Call mapf on the return from this
}