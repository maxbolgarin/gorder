package gorder

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultFlushInterval is the maximum interval to sleep between sending tasks to workers if another value is not set.
	DefaultFlushInterval = 100 * time.Millisecond
	// DefaultUnusedThreshold is the threshold for deleting unused queues to save memory if another value is not set.
	DefaultUnusedThreshold = 5 * time.Minute
	// DefaultRetries is the maximum number of retries for failed tasks if another value is not set.
	DefaultRetries = math.MaxInt
	// DefaultRetryBackoffMinTimeout is the minimum timeout between retries if another value is not set.
	DefaultRetryBackoffMinTimeout = 100 * time.Millisecond
	// DefaultRetryBackoffMaxTimeout is the maximum timeout between retries if another value is not set.
	DefaultRetryBackoffMaxTimeout = 10 * time.Second
)

// Options contains options for Gorder
type Options struct {
	// Workers is the number of workers to handle tasks functions, defaults to runtime.NumCPU
	Workers int
	// FlushInterval is the maximum interval to sleep between sending tasks to workers
	FlushInterval time.Duration
	// UnusedThreshold is the threshold for deleting unused queues to save memory
	UnusedThreshold time.Duration
	// Retries is the maximum number of retries for failed tasks
	Retries int
	// RetryBackoffMinTimeout is the minimum timeout between retries
	RetryBackoffMinTimeout time.Duration
	// RetryBackoffMaxTimeout is the maximum timeout between retries
	RetryBackoffMaxTimeout time.Duration
	// ThrowOnShutdown is a flag to throw broken tasks on shutdown
	ThrowOnShutdown bool

	Log Logger
}

// Logger is an interface for logging. You can use slog.Default(). It should be structural logger.
type Logger interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

// Gorder is a task queue with strict ordering
type Gorder[T comparable] struct {
	q   *Queue[T]
	log Logger

	workerChan chan taskWithKey[T]
	stopChan   chan struct{}

	opts Options

	counter    atomic.Uint64
	isShutdown atomic.Bool

	brokenQueues map[T]int
	mu           sync.Mutex
}

// New creates a new Gorder with default options.
func New[T comparable](ctx context.Context, workers int, lg Logger) *Gorder[T] {
	return NewWithOptions[T](ctx, Options{Workers: workers, Log: lg})
}

// NewWithOptions creates a new Gorder with custom options.
func NewWithOptions[T comparable](ctx context.Context, opts Options) *Gorder[T] {
	opts = opts.withDefault()

	if opts.Log == nil {
		opts.Log = noopLogger{}
	}

	q := &Gorder[T]{
		q:            NewQueue[T](),
		log:          opts.Log,
		workerChan:   make(chan taskWithKey[T], opts.Workers),
		stopChan:     make(chan struct{}),
		opts:         opts,
		brokenQueues: make(map[T]int),
	}

	for i := 0; i < opts.Workers; i++ {
		runGoroutine(opts.Log, func() { q.worker(ctx) })
	}

	runGoroutine(opts.Log, q.flusher)
	runGoroutine(opts.Log, q.deleteUnusedQueues)

	return q
}

// Shutdown gracefully shutdowns the queue. It just waits for all tasks to be processed without sleeping.
func (q *Gorder[T]) Shutdown(ctx context.Context) error {
	q.isShutdown.Store(true)

	var total int
	for _, v := range q.q.Stat() {
		total += v.Length
	}

	if total > 0 {
		q.log.Info("starting shutdown task queue", "tasks", total)
	}

	select {
	case <-q.stopChan:
		close(q.workerChan)
		return nil

	case <-ctx.Done():
		return ctx.Err()

	}
}

// Push adds task to the end of the queue. It will process tasks sequentially for the same queue key
// and parallel for different queue keys. Don't use it after shutdown.
func (q *Gorder[T]) Push(queueKey T, name string, f TaskFunc) {
	q.counter.Add(1)
	name = name + ":" + strconv.Itoa(int(q.counter.Load()))

	q.q.Push(queueKey, NewTask(name, f))
}

// Stat returns statistics for each queue.
func (q *Gorder[T]) Stat() map[T]QueueStat {
	return q.q.Stat()
}

// BrokenQueues returns current number of retries for all broken queues.
func (q *Gorder[T]) BrokenQueues() map[T]int {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make(map[T]int, len(q.brokenQueues))
	for k, v := range q.brokenQueues {
		out[k] = v
	}

	return out
}

func (q *Gorder[T]) worker(ctx context.Context) {
	for task := range q.workerChan {
		retry := 0

		for {
			err := task.Foo(ctx)
			if err == nil {
				if retry > 0 {
					q.log.Info("success task after retries", "queue", task.QueueKey, "name", task.Name, "retry", retry)
				} else {
					q.log.Debug("success task", "queue", task.QueueKey, "name", task.Name)
				}
				break
			}
			q.log.Error("failed task", "error", err, "queue", task.QueueKey, "name", task.Name, "retry", retry)

			retry++
			if retry > q.opts.Retries {
				q.log.Error("throw task after retries", "queue", task.QueueKey, "name", task.Name, "retry", retry)
				break
			}

			if q.opts.ThrowOnShutdown && q.isShutdown.Load() {
				q.log.Error("throw task on shutdown", "queue", task.QueueKey, "name", task.Name, "retry", retry)
				break
			}

			q.mu.Lock()
			q.brokenQueues[task.QueueKey] = retry
			q.mu.Unlock()

			sleepTime := getSleepTime(retry, q.opts.RetryBackoffMinTimeout, q.opts.RetryBackoffMaxTimeout)
			time.Sleep(sleepTime)
		}

		if err := q.q.Ack(task.QueueKey); err != nil {
			q.log.Warn("cannot ack", "error", err, "queue", task.QueueKey, "name", task.Name)
		}

		if retry > 0 {
			q.mu.Lock()
			delete(q.brokenQueues, task.QueueKey)
			q.mu.Unlock()
		}
	}
}

func (q *Gorder[T]) flusher() {
	for {
		var (
			cycleStart = time.Now()
			queues     = q.q.AllQueues()

			empty   int
			working int
		)

		for _, key := range queues {
			task, err := q.q.Next(key)
			switch {
			case errors.Is(err, ErrQueueIsEmpty) || errors.Is(err, ErrQueueNotFound):
				empty++
				continue

			case errors.Is(err, ErrWaitForAck):
				working++
				continue
			}

			q.log.Debug("send task to worker", "queue", key, "name", task.Name)

			q.workerChan <- taskWithKey[T]{
				Task:     task,
				QueueKey: key,
			}
		}

		// in shutdown we want to process all tasks
		if q.isShutdown.Load() {
			if empty < len(queues) {
				continue
			}

			q.log.Info("shutdown task queue, all tasks are done")
			close(q.stopChan)
			return
		}

		// Some tasks were sent to workers, so try to check for one more time
		if empty+working < len(queues) {
			continue
		}

		// Here we got all queues is empty or working

		sleep := cycleStart.Add(q.opts.FlushInterval).Sub(time.Now())
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

func (q *Gorder[T]) deleteUnusedQueues() {
	t := time.NewTicker(q.opts.UnusedThreshold)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if q.isShutdown.Load() {
				return
			}

			deleted, struggled := q.q.DeleteUnusedQueues(q.opts.UnusedThreshold)

			if len(deleted) > 0 {
				q.log.Info("deleted unused queues", "queues", deleted)
			}

			if len(struggled) > 0 {
				q.log.Warn("there are struggling queues", "queues", struggled)
			}

		case <-q.stopChan:
			return
		}
	}
}

func getSleepTime(retry int, min, max time.Duration) time.Duration {
	sleepTime := float64(min) * math.Pow(2, float64(retry))
	sleepTime = rand.Float64()*(sleepTime-float64(min)) + float64(min)
	if sleepTime > float64(max) {
		sleepTime = float64(max)
	}
	return time.Duration(sleepTime)
}

type taskWithKey[T any] struct {
	Task
	QueueKey T
}

func runGoroutine(l Logger, f func()) {
	var foo func()
	fn := f
	foo = func() {
		defer func() {
			if err := recover(); err != nil {
				l.Error(string(debug.Stack()), "error", err) // build with -trimpath to avoid printing build path in trace
				go foo()
			}
		}()
		fn()
	}
	go foo()
}

func (opt Options) withDefault() Options {
	if opt.Workers <= 0 {
		opt.Workers = runtime.NumCPU()
	}
	if opt.FlushInterval <= 0 {
		opt.FlushInterval = DefaultFlushInterval
	}
	if opt.UnusedThreshold <= 0 {
		opt.UnusedThreshold = DefaultUnusedThreshold
	}
	if opt.Retries <= 0 {
		opt.Retries = DefaultRetries
	}
	if opt.RetryBackoffMinTimeout <= 0 {
		opt.RetryBackoffMinTimeout = DefaultRetryBackoffMinTimeout
	}
	if opt.RetryBackoffMaxTimeout <= 0 {
		opt.RetryBackoffMaxTimeout = DefaultRetryBackoffMaxTimeout
	}
	return opt
}

type noopLogger struct{}

func (noopLogger) Debug(_ string, _ ...any) {}
func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Warn(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}
