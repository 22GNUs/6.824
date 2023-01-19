package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const Timeout time.Duration = time.Second * 10

type Coordinator struct {
	Files             []string
	NReduce           int
	MapAssignments    TaskTable
	ReduceAssignments TaskTable
	mux               sync.Mutex
	done              bool
	ch                chan bool
}

func (c *Coordinator) printStatus() {
	printer := func(name TaskType, taskTable TaskTable) {
		fmt.Printf("==== Current status of %s:\n", name)
		for id, task := range taskTable {
			fmt.Printf("==== Task[%d,%s] is in %s\n", id, task.Type, task.Status)
		}
		fmt.Printf("==========================\n")
	}
	printer(Map, c.MapAssignments)
	printer(Reduce, c.ReduceAssignments)
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
	go c.Listen()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mux.Lock()
	done := c.MapAssignments.Done() && c.ReduceAssignments.Done()
	c.mux.Unlock()
	if done {
		c.mux.Lock()
		c.done = true
		c.mux.Unlock()

		log.Print("Waiting for listener signal...")
		<-c.ch
		log.Print("All tasks done, shutting down...")
		defer close(c.ch)
	}
	return done
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

	if !c.MapAssignments.Done() {
		// this check is important, otherwise it may lead to problems with concurrent scenarios
		log.Print("There still non finish map tasks, wait unitl all map tasks finish")
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

func (c *Coordinator) Listen() {
	done := false
	for !done {
		// check pre 2 seconds
		time.Sleep(2 * time.Second)
		if !c.mux.TryLock() {
			log.Printf("Try get lock faild, wait for another time")
			continue
		}
		log.Println("Checking timeouted tasks...")
		finder := func(_ int, taskInfo *TaskInfo) bool {
			now := time.Now()
			return taskInfo.Status == InProgress && taskInfo.AssignedAt.Add(Timeout).Before(now)
		}

		markTimeoutFailed := func(assignments TaskTable) {
			if tasks := assignments.Filter(finder); tasks != nil {
				for id, task := range tasks {
					log.Printf("Task: [%d,%s] is timout, setting it to failed", id, task.taskInfo.Type)
					task.taskInfo.Status = Failed
				}
			}
		}

		// mark timeouted tasks
		markTimeoutFailed(c.MapAssignments)
		markTimeoutFailed(c.ReduceAssignments)
		done = c.done

		c.printStatus()
		c.mux.Unlock()
	}
	log.Print("Find coordinator done, send signal to channel")
	c.ch <- true
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
		done:              false,
		ch:                make(chan bool, 1),
	}

	c.server()
	return &c
}

func getReduceTaskId(file string) int {
	// get last char as int
	return int(file[len(file)-1] - '0')
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
