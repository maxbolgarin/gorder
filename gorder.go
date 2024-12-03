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

// Options contains options for Gorder. Every field is optional.
type Options struct {
	// Workers is the number of workers to handle tasks functions.
	// The default value is the number of CPUs.
	Workers int

	// FlushInterval is the max time of sleeping between flushing tasks to workers.
	// The default value is 100ms.
	FlushInterval time.Duration

	// UnusedThreshold is the max time between pushing tasks to queue before it gets deleted.
	// The default value is 5 minutes.
	UnusedThreshold time.Duration

	// Retries is the max number of retries for failed task before throwing it.
	// Use -1 to make retries unlimited.
	// The default value is 10.
	Retries int

	// RetryBackoffMinTimeout is the min timeout between retries.
	// The default value is 100ms.
	RetryBackoffMinTimeout time.Duration

	// RetryBackoffMaxTimeout is the max timeout between retries.
	// The default value is 10s.
	RetryBackoffMaxTimeout time.Duration

	// NoRetries is a flag that indicates that we should not retry failed tasks.
	// The default value is false.
	NoRetries bool

	// DoNotThrowOnShutdown is a flag that indicates that we should wait for all tasks to complete before shutting down.
	// The default value is false.
	DoNotThrowOnShutdown bool

	// Logger is used to log messages in case of errors.
	// The default value is nil, so there is no logging.
	Logger Logger
}

// Gorder is a in-memory task worker with strict ordering.
// It handles tasks in parallel using worker pool.
// It is safe for concurrent use.
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

// New creates a new Gorder. It starts workers and flusher goroutines.
func New[T comparable](ctx context.Context, rawOpts ...Options) *Gorder[T] {
	var opts Options
	if len(rawOpts) > 0 {
		opts = rawOpts[0]
	}
	opts = opts.withDefault()

	if opts.Logger == nil {
		opts.Logger = noopLogger{}
	}

	q := &Gorder[T]{
		q:            NewQueue[T](),
		log:          opts.Logger,
		workerChan:   make(chan taskWithKey[T], opts.Workers),
		stopChan:     make(chan struct{}),
		opts:         opts,
		brokenQueues: make(map[T]int),
	}

	for i := 0; i < opts.Workers; i++ {
		runGoroutine(opts.Logger, func() { q.worker(ctx) })
	}

	runGoroutine(opts.Logger, q.flusher)
	runGoroutine(opts.Logger, q.deleteUnusedQueues)

	return q
}

// Shutdown stops the Gorder and waits for all tasks to complete.
// By default it throws broken tasks without retries. To change this behavior, use DoNotThrowOnShutdown option.
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

// Push adds task to the end of queue. It is non blocking and safe for concurrent use.
// Tasks with different queue keys are executed in parallel, not sequentially.
// Tasks with the same queue key are executed in order.
// Don't use it after Shutdown.
func (q *Gorder[T]) Push(queueKey T, name string, f TaskFunc) {
	q.counter.Add(1)
	name = name + ":" + strconv.Itoa(int(q.counter.Load()))

	q.q.Push(queueKey, NewTask(name, f))
}

// Stat returns number of tasks for each queue and some additional info.
func (q *Gorder[T]) Stat() map[T]QueueStat {
	return q.q.Stat()
}

// BrokenQueues returns number of retries for each broken queue.
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
			if q.opts.NoRetries {
				q.log.Error("failed task, throw it", "error", err, "queue", task.QueueKey, "name", task.Name)
				break
			}
			q.log.Error("failed task", "error", err, "queue", task.QueueKey, "name", task.Name, "retry", retry)

			retry++
			if retry > q.opts.Retries {
				q.log.Error("throw task after retries", "queue", task.QueueKey, "name", task.Name, "retry", retry)
				break
			}

			if !q.opts.DoNotThrowOnShutdown && q.isShutdown.Load() {
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
		opt.FlushInterval = defaultFlushInterval
	}
	if opt.UnusedThreshold <= 0 {
		opt.UnusedThreshold = defaultUnusedThreshold
	}
	if opt.Retries == 0 {
		opt.Retries = maxRetries
	}
	if opt.Retries < 0 {
		opt.Retries = math.MaxInt32
	}
	if opt.RetryBackoffMinTimeout <= 0 {
		opt.RetryBackoffMinTimeout = minTimeout
	}
	if opt.RetryBackoffMaxTimeout <= 0 {
		opt.RetryBackoffMaxTimeout = maxTimeout
	}
	return opt
}

const (
	defaultFlushInterval   = 100 * time.Millisecond
	defaultUnusedThreshold = 5 * time.Minute
	maxRetries             = 10
	minTimeout             = 100 * time.Millisecond
	maxTimeout             = 10 * time.Second
)

// Logger is an interface for logging. You can use slog.
type Logger interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(_ string, _ ...any) {}
func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Warn(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}
