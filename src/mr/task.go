package mr

type taskStatus string

const (
	idle       taskStatus = "Idle"
	failed                = "Failed"
	inProgress            = "InProgress"
	done                  = "Done"
)

// taskInfo repensent a task assign to worker
type taskInfo struct {
	// default is 0, beging in 1
	workerId  int
	filenames []string
	status    taskStatus
}

// taskTable is a k-v table to save taskInfo
type taskTable map[int]*taskInfo

// Done check is all tasks in table is done
func (taskTable taskTable) Done() bool {
	for _, v := range taskTable {
		if v.status != done {
			return false
		}
	}
	return true
}
