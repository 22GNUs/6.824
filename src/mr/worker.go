package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// combine tmp file and json encoder
type intermedidateTmpOut struct {
	tmpFile *os.File
	encoder *json.Encoder
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := generateId()
	for {
		request := TaskRequest{workerId}
		var assignment TaskAssignment
		// if fail would existed
		call("Coordinator.AssignTask", &request, &assignment)
		if !assignment.Empty() {
			log.Printf("Get task: [%d:%s] from coordinator", assignment.TaskId, assignment.TaskType)
			switch assignment.TaskType {
			case Map:
				doMap(mapf, assignment)
			case Reduce:
				doReduce(reducef, assignment)
			}
		} else {
			log.Print("Get empty task from coordinator")
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, assignment TaskAssignment) {
	// map to list of k-v
	tmpOuts := make(map[int]*intermedidateTmpOut)
	for _, filename := range assignment.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {
			reduceTaskId := ihash(kv.Key) % assignment.NReduce
			tmpOut, ok := tmpOuts[reduceTaskId]
			if !ok {
				tmpFile, err := ioutil.TempFile("", "map-tmp-")
				if err != nil {
					log.Fatalln("Couldn't create tmp file")
				}
				// init encoder
				encoder := json.NewEncoder(tmpFile)
				tmpOut = &intermedidateTmpOut{tmpFile, encoder}
				tmpOuts[reduceTaskId] = tmpOut
			}
			// append kv to tmp file
			tmpOut.encoder.Encode(kv)
		}
		// write tmp files to os
		finalOutFiles := []string{}
		for reduceIdx, tmpOut := range tmpOuts {
			tmpFile := tmpOut.tmpFile.Name()
			outFile := fmt.Sprintf("mr-%d-%d", assignment.TaskId, reduceIdx)
			err := os.Rename(tmpFile, outFile)
			if err != nil {
				log.Fatalf("Rename tmpFile: %s to %s failed", tmpFile, outFile)
			}
			finalOutFiles = append(finalOutFiles, outFile)
			log.Printf("Map task: %d, presistent intermediate file to %s", assignment.TaskId, outFile)
		}

		notify := TaskDoneNotification{assignment.TaskId, assignment.TaskType, finalOutFiles}
		var ack TaskAck
		call("Coordinator.TaskDone", &notify, &ack)
		if ack.Ok {
			log.Printf("Map task: %d notify coordinator success", assignment.TaskId)
		} else {
			log.Printf("Map task: %d notify coordinator failed", assignment.TaskId)
		}
	}

}

func doReduce(reducef func(string, []string) string, assignment TaskAssignment) {
	intermediate := []KeyValue{}
	for _, filename := range assignment.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	tmpFile, _ := ioutil.TempFile("", "reduce-tmp-")
	defer os.Remove(tmpFile.Name())
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to tmpFile.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFileName := tmpFile.Name()
	outFileName := fmt.Sprintf("mr-out-%d", assignment.TaskId)
	if err := os.Rename(tmpFileName, outFileName); err != nil {
		log.Fatalf("Rename tmpFile: %s to %s failed", tmpFileName, outFileName)
	}
	log.Printf("Reduce task: %d presistent file: %s success", assignment.TaskId, outFileName)
	notify := TaskDoneNotification{assignment.TaskId, assignment.TaskType, []string{outFileName}}
	var ack TaskAck
	call("Coordinator.TaskDone", &notify, &ack)
	if ack.Ok {
		log.Printf("Reduce task: %d notify coordinator success", assignment.TaskId)
	} else {
		log.Printf("Reduce task: %d notify coordinator failed", assignment.TaskId)
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func generateId() string {
	return fmt.Sprintf("Worker-%d", time.Now().Unix())
}
