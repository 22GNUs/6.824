package mr

import (
	"sync"
	"sync/atomic"
)

// IdGenerator returns different and unique id every call
type IdGenerator interface {
	getId() int
}

type inMemeryAutoImcIdGenerator struct {
	id uint32
}

func (generator *inMemeryAutoImcIdGenerator) getId() int {
	defer func() {
		atomic.AddUint32(&generator.id, 1)
	}()
	return int(generator.id)
}

var (
	once     sync.Once
	instance IdGenerator
)

// IdInstance creator a IdGenerator and return
func IdInstance() IdGenerator {
	once.Do(func() {
		instance = &inMemeryAutoImcIdGenerator{1}
	})
	return instance
}
