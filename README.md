# gorder

[![Go Version][version-img]][doc] [![GoDoc][doc-img]][doc] [![Build][ci-img]][ci] [![GoReport][report-img]][report]

**gorder** is a zero-dependency in-memory task queue with strict ordering. It can be used for scheduling tasks in a particular order, eg. for async database operations with a strict order of execution.

## Installation

```bash
go get -u github.com/maxbolgarin/gorder
```

## Usage

```go
q := gorder.New[string](ctx, 10, slog.Default())
q.Push("queue1", "task1", func(ctx context.Context) error {
    println("1")
    return nil
})
q.Push("queue1", "task2", func(ctx context.Context) error {
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
* You can also use `NewWithOptions()` to configure the **gorder** instance.
* You can use any comparable type as queue key. For example, string, int, or any struct with string fields.
* It will retry failed tasks until the maximum number of retries is reached. If the number of retries is exceeded, the task will be thrown. You can configure the maximum number of retries using `NewWithOptions()`. **Default is infinite.**
* It will throw tasks on shutdown if `ThrowOnShutdown` is true.


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
