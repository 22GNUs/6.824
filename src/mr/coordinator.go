package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	files             []string
	nReduce           int
	mapAssignments    taskTable
	reduceAssignments taskTable
	done              bool
	mux               sync.Mutex
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapAssignments.Done() && c.reduceAssignments.Done()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		// assign map tasks by files
		mapAssignments: splitMapAssignments(files),
		// init reduce task reference first
		// after map task complete, will create reduce task
		reduceAssignments: make(taskTable),
		done:              false,
	}

	c.server()
	return &c
}

func splitMapAssignments(files []string) taskTable {
	// simple implemention
	// just one file one task
	taskTable := make(taskTable)
	idGenerator := IdInstance()
	for _, file := range files {
		taskTable[idGenerator.getId()] = &taskInfo{
			filenames: []string{file},
			status:    idle,
		}
	}
	return taskTable
}
