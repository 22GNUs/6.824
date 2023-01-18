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
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.MapAssignments.Done() && c.ReduceAssignments.Done()
}

// Called when a worker finish his job
func (c *Coordinator) TaskDone(notify *TaskDoneNotification, ack *TaskAck) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	finder := func(id int, _ *TaskInfo) bool {
		// find the task by id
		return id == notify.TaskId
	}
	switch notify.TaskType {
	case Map:
		id, taskInfo := c.MapAssignments.FindFirst(finder)
		if taskInfo.Status != Done {
			// collect map intermedidate files, and assign to right reduce tasks
			for _, file := range notify.Filenames {
				reduceTaskId := getReduceTaskId(file)
				reduceTask, created := c.ReduceAssignments[reduceTaskId]
				if !created {
					reduceTask = &TaskInfo{
						Type:   Reduce,
						Status: Idle,
					}
					c.ReduceAssignments[reduceTaskId] = reduceTask
				}
				reduceTask.Filenames = append(reduceTask.Filenames, file)
			}
			taskInfo.Status = Done
			log.Printf("Map task: %d is done", id)
		}
	case Reduce:
		id, taskInfo := c.ReduceAssignments.FindFirst(finder)
		// simply mark reduce task as done
		if taskInfo.Status != Done {
			taskInfo.Status = Done
			log.Printf("Reduce task: %d is done", id)
		}
	}
	ack.Ok = true
	return nil
}

func getReduceTaskId(file string) int {
	return int(file[len(file)-1])
}

// AssignTask assign task to worker, call from worker by rpc
func (c *Coordinator) AssignTask(request *TaskRequest, assignment *TaskAssignment) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	finder := func(_ int, taskInfo *TaskInfo) bool {
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
		log.Printf("Map task %d assigned to worker: %s", id, request.WorkerId)
		return nil
	}

	// then assign reduce tasks
	id, reduceTask := c.ReduceAssignments.FindFirst(finder)
	if reduceTask != nil {
		reduceTask.inProgress(request.WorkerId)
		assignment.Fill(id, c.NReduce, *reduceTask)
		log.Printf("Reduce task %d assigned to worker: %s", id, request.WorkerId)
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
