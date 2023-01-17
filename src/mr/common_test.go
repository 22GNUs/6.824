package mr

import (
	"log"
	"testing"
)

// TestIdGenerator test is idGenerator always give unique id
func TestIdGenerator(t *testing.T) {
	existMap := make(map[int]bool)
	ins := IdInstance()
	for i := 0; i < 100; i++ {
		id := ins.getId()
		if existMap[id] {
			log.Fatal("The generator have generated same id")
			return
		}
		existMap[id] = true
	}
}
