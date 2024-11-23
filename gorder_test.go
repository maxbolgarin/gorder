package gorder_test

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maxbolgarin/gorder"
)

func TestGorder(t *testing.T) {
	t.Run("SingleQueue", func(t *testing.T) {
		q := gorder.New[string](context.Background(), 0, nil)

		var out []string
		var mu sync.Mutex

		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push("0", "task"+ind, func(ctx context.Context) error {
				time.Sleep(time.Duration(rand.Int63n(1)) * time.Millisecond)
				mu.Lock()
				out = append(out, ind)
				mu.Unlock()
				return nil
			})
		}

		stat := q.Stat()
		if stat["0"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["0"].Length)
		}

		time.Sleep(100 * time.Millisecond)

		err := q.Shutdown(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		broken := q.BrokenQueues()
		if len(broken) != 0 {
			t.Fatalf("expected 0, got %d", len(broken))
		}

		mu.Lock()
		defer mu.Unlock()

		if len(out) != 100 {
			t.Fatalf("expected 100, got %d", len(out))
		}
		for i := 0; i < 100; i++ {
			if out[i] != strconv.Itoa(i) {
				t.Fatalf("expected task%d, got %s", i, out[i])
			}
		}
	})

	t.Run("ManyQueus", func(t *testing.T) {
		q := gorder.NewWithOptions[string](context.Background(), gorder.Options{UnusedThreshold: time.Millisecond})

		var out []string
		var mu sync.Mutex

		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push("0", "task"+ind, func(ctx context.Context) error {
				time.Sleep(time.Duration(rand.Int63n(1)) * time.Millisecond)
				mu.Lock()
				out = append(out, ind)
				mu.Unlock()
				return nil
			})
		}

		var out2 []string
		var mu2 sync.Mutex
		for i := 100; i < 200; i++ {
			ind := strconv.Itoa(i)
			q.Push("1", "task"+ind, func(ctx context.Context) error {
				time.Sleep(time.Duration(rand.Int63n(1)) * time.Millisecond)
				mu2.Lock()
				out2 = append(out2, ind)
				mu2.Unlock()
				return nil
			})
		}

		var out3 []string
		var mu3 sync.Mutex
		for i := 200; i < 300; i++ {
			ind := strconv.Itoa(i)
			q.Push("2", "task"+ind, func(ctx context.Context) error {
				time.Sleep(time.Duration(rand.Int63n(1)) * time.Millisecond)
				mu3.Lock()
				out3 = append(out3, ind)
				mu3.Unlock()
				return nil
			})
		}

		stat := q.Stat()
		if stat["0"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["0"].Length)
		}
		if stat["1"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["1"].Length)
		}
		if stat["2"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["2"].Length)
		}

		time.Sleep(100 * time.Millisecond)

		err := q.Shutdown(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		defer mu.Unlock()

		if len(out) != 100 {
			t.Fatalf("expected 100, got %d", len(out))
		}
		for i := 0; i < 100; i++ {
			if out[i] != strconv.Itoa(i) {
				t.Fatalf("expected task%d, got %s", i, out[i])
			}
		}

		mu2.Lock()
		defer mu2.Unlock()

		if len(out2) != 100 {
			t.Fatalf("expected 100, got %d", len(out2))
		}
		for i := 0; i < 100; i++ {
			if out2[i] != strconv.Itoa(i+100) {
				t.Fatalf("expected task%d, got %s", i, out2[i])
			}
		}

		mu3.Lock()
		defer mu3.Unlock()

		if len(out3) != 100 {
			t.Fatalf("expected 100, got %d", len(out3))
		}
		for i := 0; i < 100; i++ {
			if out3[i] != strconv.Itoa(i+200) {
				t.Fatalf("expected task%d, got %s", i, out3[i])
			}
		}
	})

	t.Run("SingleQueueError", func(t *testing.T) {
		q := gorder.NewWithOptions[string](context.Background(), gorder.Options{
			RetryBackoffMinTimeout: time.Millisecond,
			RetryBackoffMaxTimeout: 10 * time.Millisecond,
			Retries:                5,
		})

		var out []string
		var mu sync.Mutex

		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push("0", "task"+ind, func(ctx context.Context) error {
				if ind == "50" {
					return errors.New("50")
				}
				mu.Lock()
				out = append(out, ind)
				mu.Unlock()
				return nil
			})
		}

		stat := q.Stat()
		if stat["0"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["0"].Length)
		}

		time.Sleep(100 * time.Millisecond)

		err := q.Shutdown(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		defer mu.Unlock()

		if len(out) != 99 {
			t.Fatalf("expected 99, got %d", len(out))
		}
		for i := 0; i < 99; i++ {
			if i < 50 {
				if out[i] != strconv.Itoa(i) {
					t.Fatalf("expected task%d, got %s", i, out[i])
				}
			} else {
				if out[i] != strconv.Itoa(i+1) {
					t.Fatalf("expected task%d, got %s", i, out[i])
				}
			}
		}
	})

	t.Run("SingleQueueErrorThrowShutdown", func(t *testing.T) {
		q := gorder.NewWithOptions[string](context.Background(), gorder.Options{
			RetryBackoffMinTimeout: time.Millisecond,
			RetryBackoffMaxTimeout: 10 * time.Millisecond,
			ThrowOnShutdown:        true,
			FlushInterval:          time.Millisecond,
		})

		var out []string
		var mu sync.Mutex

		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push("0", "task"+ind, func(ctx context.Context) error {
				if ind == "50" {
					return errors.New("50")
				}
				mu.Lock()
				out = append(out, ind)
				mu.Unlock()
				return nil
			})
		}

		time.Sleep(100 * time.Millisecond)

		stat := q.Stat()
		if stat["0"].Length == 0 {
			t.Fatalf("expected not zero, got %d", stat["0"].Length)
		}

		broken := q.BrokenQueues()
		if broken["0"] == 0 {
			t.Fatalf("expected not zero, got %d", broken["0"])
		}

		time.Sleep(100 * time.Millisecond)

		err := q.Shutdown(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		defer mu.Unlock()

		if len(out) != 99 {
			t.Fatalf("expected 99, got %d", len(out))
		}
		for i := 0; i < 99; i++ {
			if i < 50 {
				if out[i] != strconv.Itoa(i) {
					t.Fatalf("expected task%d, got %s", i, out[i])
				}
			} else {
				if out[i] != strconv.Itoa(i+1) {
					t.Fatalf("expected task%d, got %s", i, out[i])
				}
			}
		}
	})
}
