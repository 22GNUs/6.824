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
	Files             []string
	NReduce           int
	MapAssignments    TaskTable
	ReduceAssignments TaskTable
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
	return c.MapAssignments.Done() && c.ReduceAssignments.Done()
}

// AssignTask assign task to worker, call from worker by rpc
func (c *Coordinator) AssignTask(request *TaskRequest, assignment *TaskAssignment) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	finder := func(taskInfo *TaskInfo) bool {
		// find the idle or failed task to assign
		return taskInfo.Status == Idle || taskInfo.Status == Failed
	}

	// if any map task remain, then assign map task first
	id, mapTask := c.MapAssignments.FindFirst(finder)
	if mapTask != nil {
		// assign map task to worker
		mapTask.inProgress(request.WorkerId)
		// fill assign values
		assignment.Fill(id, c.NReduce, *mapTask)
		log.Printf("Map task %d assigned to worder: %d", id, request.WorkerId)
		return nil
	}

	// then assign reduce tasks
	id, reduceTask := c.ReduceAssignments.FindFirst(finder)
	if reduceTask != nil {
		reduceTask.inProgress(request.WorkerId)
		assignment.Fill(id, c.NReduce, *reduceTask)
		log.Printf("Reduce task %d assigned to worder: %d", id, request.WorkerId)
		return nil
	}

	// no task to assign
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:   files,
		NReduce: nReduce,
		// assign map tasks by files
		MapAssignments: splitMapAssignments(files),
		// init reduce task reference first
		// after map task complete, will create reduce task
		ReduceAssignments: make(TaskTable),
	}

	c.server()
	return &c
}

func splitMapAssignments(files []string) TaskTable {
	// simple implemention
	// just one file one task
	taskTable := make(TaskTable)
	idGenerator := IdInstance()
	for _, file := range files {
		taskId := idGenerator.getId()
		taskTable[taskId] = &TaskInfo{
			Filenames: []string{file},
			Status:    Idle,
			Type:      Map,
		}
		log.Printf("Assigned file: %s to map task: %d", file, taskId)
	}
	return taskTable
}
