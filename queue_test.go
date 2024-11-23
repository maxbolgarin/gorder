package gorder_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxbolgarin/gorder"
)

func TestQueue(t *testing.T) {
	t.Run("PushDifferentQueues", func(t *testing.T) {
		q := gorder.NewQueue[string]()
		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push(ind, gorder.NewTask("task"+ind, func(ctx context.Context) error { return errors.New(ind) }))
		}

		for i := 0; i < 100; i++ {
			out, err := q.Next(strconv.Itoa(i))
			if err != nil {
				t.Fatal(err)
			}
			if out.Name != "task"+strconv.Itoa(i) {
				t.Fatalf("expected task%d, got %s", i, out.Name)
			}
			resErr := out.Foo(context.Background())
			if resErr == nil || !strings.Contains(resErr.Error(), strconv.Itoa(i)) {
				t.Fatalf("expected error")
			}
		}

		_, err := q.Next("dummy")
		if !errors.Is(err, gorder.ErrQueueNotFound) {
			t.Fatal("expected ErrQueueNotFound")
		}

		all := q.AllQueues()
		if len(all) != 100 {
			t.Fatalf("expected 100, got %d", len(all))
		}

		stat := q.Stat()
		if len(stat) != 100 {
			t.Fatalf("expected 100, got %d", len(stat))
		}

		for i := 0; i < 100; i++ {
			if stat[strconv.Itoa(i)].Length != 1 {
				t.Fatalf("expected 1, got %d", stat[strconv.Itoa(i)].Length)
			}	
			if !stat[strconv.Itoa(i)].IsWaitForAck {
				t.Fatalf("expected true, got %t", stat[strconv.Itoa(i)].IsWaitForAck)
			}
		}

		// all is working
		waiting := q.WaitingQueues()
		if len(waiting) != 0 {
			t.Fatalf("expected 0, got %d", len(waiting))
		}

		deleted, struggled := q.DeleteUnusedQueues(9999 * time.Hour)

		if len(deleted) != 0 {
			t.Fatalf("expected 0, got %d", len(deleted))
		}
		if len(struggled) != 0 {
			t.Fatalf("expected 0, got %d", len(struggled))
		}

		time.Sleep(10 * time.Millisecond)
		deleted, struggled = q.DeleteUnusedQueues(time.Millisecond)

		if len(deleted) != 0 {
			t.Fatalf("expected 0, got %d", len(deleted))
		}
		if len(struggled) != 100 {
			t.Fatalf("expected 100, got %d", len(struggled))
		}

		for i := 0; i < 100; i++ {
			err = q.Ack(strconv.Itoa(i))
			if err != nil {
				t.Fatal(err)
			}
		}

		time.Sleep(10 * time.Millisecond)
		deleted, struggled = q.DeleteUnusedQueues(time.Millisecond)

		if len(deleted) != 100 {
			t.Fatalf("expected 100, got %d", len(deleted))
		}
		if len(struggled) != 0 {
			t.Fatalf("expected 0, got %d", len(struggled))
		}
	})

	t.Run("PushSameQueue", func(t *testing.T) {
		q := gorder.NewQueue[string]()
		for i := 0; i < 100; i++ {
			ind := strconv.Itoa(i)
			q.Push("0", gorder.NewTask("task"+ind, func(ctx context.Context) error { return errors.New(ind) }))
		}

		out, err := q.Next("0")
		if err != nil {
			t.Fatal(err)
		}
		if out.Name != "task0" {
			t.Fatalf("expected task0, got %s", out.Name)
		}
		resErr := out.Foo(context.Background())
		if resErr == nil || !strings.Contains(resErr.Error(), "0") {
			t.Fatalf("expected error")
		}

		for i := 1; i < 100; i++ {
			_, err := q.Next("0")
			if !errors.Is(err, gorder.ErrWaitForAck) {
				t.Fatal("expected error")
			}
		}

		err = q.Ack("0")
		if err != nil {
			t.Fatal(err)
		}

		err = q.Ack("0")
		if !errors.Is(err, gorder.ErrNothingToAck) {
			t.Fatal("expected ErrNothingToAck")
		}

		waiting := q.WaitingQueues()
		if len(waiting) != 1 {
			t.Fatalf("expected 1, got %d", len(waiting))
		}

		for i := 1; i < 100; i++ {
			out, err := q.Next("0")
			if err != nil {
				t.Fatal(err)
			}
			if out.Name != "task"+strconv.Itoa(i) {
				t.Fatalf("expected task%d, got %s", i, out.Name)
			}
			resErr := out.Foo(context.Background())
			if resErr == nil || !strings.Contains(resErr.Error(), strconv.Itoa(i)) {
				t.Fatal("expected error")
			}
			err = q.Ack("0")
			if err != nil {
				t.Fatal(err)
			}
		}

		_, err = q.Next("0")
		if !errors.Is(err, gorder.ErrQueueIsEmpty) {
			t.Fatal("expected ErrQueueIsEmpty")
		}
		err = q.Ack("0")
		if !errors.Is(err, gorder.ErrNothingToAck) {
			t.Fatal("expected ErrNothingToAck")
		}

		all := q.AllQueues()
		if len(all) != 1 {
			t.Fatalf("expected 1, got %d", len(all))
		}

		if all[0] != "0" {
			t.Fatalf("expected 0, got %s", all[0])
		}

		if len(q.WaitingQueues()) > 0 {
			t.Fatalf("expected 0, got %d", len(q.WaitingQueues()))
		}
	})
}
