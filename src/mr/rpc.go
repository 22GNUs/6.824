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

// Add your RPC definitions here.
// TaskRequest define structure to request coordinator a task
type TaskRequest struct {
	WorkerId string
}

type TaskAssignment struct {
	TaskId    int
	TaskType  TaskType
	NReduce   int
	Filenames []string
}

func (assignment *TaskAssignment) Fill(taskId int, nReduce int, taskInfo TaskInfo) {
	assignment.TaskId = taskId
	assignment.TaskType = taskInfo.Type
	assignment.Filenames = taskInfo.Filenames
	assignment.NReduce = nReduce
}

func (assignment TaskAssignment) Empty() bool {
	return len(assignment.Filenames) == 0
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
