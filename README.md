# gorder

[![Go Version][version-img]][doc] [![GoDoc][doc-img]][doc] [![Build][ci-img]][ci] [![GoReport][report-img]][report]


<picture>
  <img src=".github/logo.jpg" width="500" alt="gorder logo">
</picture>


**Gorder** is a highly efficient, in-memory task worker with strict ordering capabilities. It is designed for parallel task execution using a worker pool and supports safe concurrent usage. With features such as task retries, backoff mechanisms, and task queue management, Gorder ensures reliable and ordered task processing. It can be used for scheduling tasks in a particular order, eg. for async database operations with a strict order of execution.


## Installation

To use Gorder in your Go project, install it via Go modules:

```bash
go get github.com/yourusername/gorder
```


## Features

- **Worker Pool**: Utilizes multiple workers to handle tasks in parallel.
- **Strict Ordering**: Ensures tasks with the same key are processed in the order they are received.
- **Retry Mechanism**: Configurable number of retries with exponential backoff.
- **Graceful Shutdown**: Options to wait for task completion before shutdown.
- **Logging Interface**: Customizable logging for better monitoring and debugging.


### Usage

```go
q := gorder.New[string](ctx, gorder.Options{
    Workers: 100,
    Logger: slog.Default(),
})

q.Push("queue1", "task1", func(ctx context.Context) error {
    time.Sleep(1 * time.Second)
    println("1")
    return nil
})
q.Push("queue1", "task2", func(ctx context.Context) error {
    time.Sleep(200*time.Millisecond)
    println("2")
    return nil
})
q.Push("queue1", "task3", func(ctx context.Context) error {
    println("3")
    return nil
})

// Output:
// 1
// 2
// 3
```

* Tasks in different queues can be executed in parallel. Queue key passed to `Push()` as first argument is used to identify the queue. 
* You can use any comparable type as queue key. For example, string, int, or any struct with string fields.
* It will retry failed tasks until the maximum number of retries is reached. If the number of retries is exceeded, the task will be thrown. You can configure the maximum number of retries in `Options`.


### Configuration

Gorder provides several configuration options via the `Options` struct:

- `Workers`: Number of workers (default: number of CPUs).
- `FlushInterval`: Maximum time between flushing tasks to workers (default: 100ms).
- `UnusedThreshold`: Time to keep idle queues before deletion (default: 5 minutes).
- `Retries`: Maximum number of retries for a failed task (default: 10).
- `RetryBackoffMinTimeout`: Minimum backoff time between retries (default: 100ms).
- `RetryBackoffMaxTimeout`: Maximum backoff time (default: 10s).
- `NoRetries`: Disable retries completely (default: false).
- `DoNotThrowOnShutdown`: Wait for task completion on shutdown (default: false).
- `Logger`: Custom logger for error, info, and debug logs.


### Handling Errors

Gorder provides feedback on task execution through logging. Ensure your logger is properly set up to capture logs at different severity levels for effective error handling and debugging.

### Cons

- **No Persistence**: Tasks are handled in-memory, risking loss on crash without durable storage integration.


## License

This project is licensed under the terms of the [MIT License](LICENSE).

[MIT License]: LICENSE.txt
[version-img]: https://img.shields.io/badge/Go-%3E%3D%201.18-%23007d9c
[doc-img]: https://pkg.go.dev/badge/github.com/maxbolgarin/gorder
[doc]: https://pkg.go.dev/github.com/maxbolgarin/gorder
[ci-img]: https://github.com/maxbolgarin/gorder/actions/workflows/go.yaml/badge.svg
[ci]: https://github.com/maxbolgarin/gorder/actions
[report-img]: https://goreportcard.com/badge/github.com/maxbolgarin/gorder
[report]: https://goreportcard.com/report/github.com/maxbolgarin/gorder

