package gorder

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrWaitForAck is returned in Next() when the previous task is waiting for ack
	ErrWaitForAck = errors.New("previous task iswaiting for ack")
	// ErrQueueNotFound is returned in Next() when the queue is not found
	ErrQueueNotFound = errors.New("queue not found")
	// ErrQueueIsEmpty is returned in Next() when the queue is empty
	ErrQueueIsEmpty = errors.New("queue is empty")
	// ErrNothingToAck is returned in Ack() when there is no tasks to ack
	ErrNothingToAck = errors.New("nothing to ack")
)

// TaskFunc is a general function to execute as a task.
type TaskFunc func(ctx context.Context) error

// Task contains a name for information and a function to be executed.
type Task struct {
	Name string
	Foo  TaskFunc
}

// QueueStat contains information about the queue.
type QueueStat struct {
	Length       int
	lastUpdate   time.Time
	IsWaitForAck bool
}

// NewTask creates a new task.
func NewTask(name string, f TaskFunc) Task {
	return Task{Name: name, Foo: f}
}

// Queue provides in memory task queue with requirements
//  1. Strict task ordering;
//  2. Guarantees that tasks won't get lost with Ack mechanics.
type Queue[T comparable] struct {
	queues     map[T]*LinkedList[Task]
	waitForAck map[T]struct{}
	lastUpdate map[T]time.Time

	mu sync.Mutex
}

// NewQueue creates a new queue.
func NewQueue[T comparable]() *Queue[T] {
	return &Queue[T]{
		queues:     make(map[T]*LinkedList[Task]),
		waitForAck: make(map[T]struct{}),
		lastUpdate: make(map[T]time.Time),
	}
}

// Next returns a next task from the queue according to the order key. You should call
// Ack() after handling this task to remove it from queue. Two subsequent calls of Next()
// without Ack() will return error.
func (q *Queue[T]) Next(queueKey T) (Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.waitForAck[queueKey]; ok {
		return Task{}, ErrWaitForAck
	}

	tasks, ok := q.queues[queueKey]
	if !ok {
		return Task{}, ErrQueueNotFound
	}

	task, ok := tasks.Front()
	if !ok {
		return Task{}, ErrQueueIsEmpty
	}

	q.waitForAck[queueKey] = struct{}{}

	return task, nil
}

// Ack removes task from queue, it can be called only after Next() or it will return error.
func (q *Queue[T]) Ack(queueKey T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.waitForAck[queueKey]; !ok {
		return ErrNothingToAck
	}

	tasks, ok := q.queues[queueKey]
	if !ok {
		return ErrQueueNotFound
	}

	_, ok = tasks.PopFront()
	if !ok {
		return ErrQueueIsEmpty
	}

	delete(q.waitForAck, queueKey)

	q.lastUpdate[queueKey] = time.Now()

	return nil
}

// Push adds task to the end of queue.
func (q *Queue[T]) Push(queueKey T, task Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	tasks, ok := q.queues[queueKey]
	if !ok {
		tasks = NewLinkedList[Task]()
		q.queues[queueKey] = tasks
	}
	tasks.PushBack(task)

	q.lastUpdate[queueKey] = time.Now()
}

// AllQueues returns list of all queue keys.
func (q *Queue[T]) AllQueues() []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]T, 0, len(q.queues))
	for k := range q.queues {
		out = append(out, k)
	}
	return out
}

// WaitingQueues returns list of all queue keys with waiting tasks.
func (q *Queue[T]) WaitingQueues() []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]T, 0, len(q.queues))
	for key, queue := range q.queues {
		if queue.Len() == 0 {
			continue // queue is empty
		}
		if _, ok := q.waitForAck[key]; ok {
			continue // task is being processed
		}
		out = append(out, key)
	}

	return out
}

// Stat returns number of tasks for each queue
func (q *Queue[T]) Stat() map[T]QueueStat {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make(map[T]QueueStat, len(q.queues))
	for k, v := range q.queues {
		_, ok := q.waitForAck[k]
		out[k] = QueueStat{
			Length:       v.Len(),
			lastUpdate:   q.lastUpdate[k],
			IsWaitForAck: ok,
		}
	}

	return out
}

// DeleteUnusedQueues removes empty queues with last active duration > activeThreshold;
// return keys of deleted queues. It also returns keys of queues with active tasks, but with last active duration > activeThreshold
func (q *Queue[T]) DeleteUnusedQueues(activeThreshold time.Duration) (deleted []T, struggling []T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for k, t := range q.lastUpdate {
		if time.Since(t) < activeThreshold {
			continue
		}

		list, ok := q.queues[k]
		if !ok || list == nil {
			continue
		}

		if list.Len() != 0 {
			struggling = append(struggling, k)
			continue
		}

		delete(q.queues, k)
		delete(q.lastUpdate, k)

		deleted = append(deleted, k)
	}

	return deleted, struggling
}
