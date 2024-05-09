package asynq

import (
	"sync"
)

type Operations struct {
	mu       *sync.RWMutex
	Handlers map[string]func(string) Handler
}

var ops = &Operations{mu: &sync.RWMutex{}, Handlers: make(map[string]func(string) Handler)}

func AddHandler(key string, handler func(string) Handler) {
	ops.mu.Lock()
	ops.Handlers[key] = handler
	ops.mu.Unlock()
}

func GetHandler(key string) func(string) Handler {
	return ops.Handlers[key]
}

func AvailableHandlers() []string {
	var op []string
	for opt := range ops.Handlers {
		op = append(op, opt)
	}
	return op
}
