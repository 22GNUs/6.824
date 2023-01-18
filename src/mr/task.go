package mr

import "time"

type TaskStatus string

const (
	Idle       TaskStatus = "Idle"
	Failed                = "Failed"
	InProgress            = "InProgress"
	Done                  = "Done"
)

type TaskType string

const (
	Map    TaskType = "Map"
	Reduce          = "Reduce"
)

// TaskInfo repensent a task assign to worker
type TaskInfo struct {
	// default is 0, beging in 1
	WorkerId   string
	Filenames  []string
	Type       TaskType
	Status     TaskStatus
	AssignedAt time.Time
}

// inProgress called when a task is assigned to a worker
// will update self status to inProgress
func (taskInfo *TaskInfo) inProgress(workerId string) {
	taskInfo.WorkerId = workerId
	taskInfo.Status = InProgress
	taskInfo.AssignedAt = time.Now()
}

// TaskTable is a k-v table to save taskInfo
type TaskTable map[int]*TaskInfo

// Done check is all tasks in table is done
func (taskTable TaskTable) Done() bool {
	for _, v := range taskTable {
		if v.Status != Done {
			return false
		}
	}
	return true
}

// FindFirst find the first task that matches the input predicate
func (taskTable TaskTable) FindFirst(predicate func(id int, taskInfo *TaskInfo) bool) (int, *TaskInfo) {
	for id, task := range taskTable {
		if predicate(id, task) {
			return id, task
		}
	}
	return 0, nil
}
