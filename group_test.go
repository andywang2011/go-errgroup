package errgroup

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGroup_ZeroValue(t *testing.T) {
	g := Group{}
	g.Go(func(context.Context) error { return nil })
	if err := g.Wait(); err != nil {
		t.Errorf("Wait() = %v", err)
	}
}

func TestGroup_WithContext(t *testing.T) {
	g := WithContext(context.Background())
	g.Go(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})
	if err := g.Wait(); err != nil {
		t.Errorf("Wait() = %v", err)
	}
}

func TestGroup_WithCancel(t *testing.T) {
	g := WithCancel(context.Background())
	g.Go(func(ctx context.Context) error { return nil })
	if err := g.Wait(); err != nil {
		t.Errorf("Wait() = %v", err)
	}
}

func TestGroup_FirstErrorWins(t *testing.T) {
	wantErr := errors.New("first")
	g := WithCancel(context.Background())
	var order []int
	var mu sync.Mutex
	g.Go(func(ctx context.Context) error {
		mu.Lock()
		order = append(order, 1)
		mu.Unlock()
		return wantErr
	})
	g.Go(func(ctx context.Context) error {
		<-ctx.Done()
		mu.Lock()
		order = append(order, 2)
		mu.Unlock()
		return nil
	})
	err := g.Wait()
	if err != wantErr {
		t.Errorf("Wait() = %v, want %v", err, wantErr)
	}
	if len(order) < 1 {
		t.Error("expected at least one task to run")
	}
}

func TestGroup_PanicRecovered(t *testing.T) {
	g := WithCancel(context.Background())
	g.Go(func(ctx context.Context) error {
		panic("test panic")
	})
	err := g.Wait()
	if err == nil {
		t.Fatal("Wait() expected error from panic")
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Wait() = %T %v, want *PanicError", err, err)
	}
	if pe.Error() != "errgroup: panic recovered: test panic" {
		t.Errorf("Error() = %q", pe.Error())
	}
	if strings.Contains(pe.Error(), "goroutine") {
		t.Errorf("Error() must not include stack: %q", pe.Error())
	}
	if len(pe.Stack()) == 0 || !strings.Contains(string(pe.Stack()), "goroutine") {
		t.Errorf("Stack() missing goroutine dump")
	}
}

func TestGroup_GOMAXPROCS_RejectsZero(t *testing.T) {
	g := Group{}
	if err := g.GOMAXPROCS(0); err == nil {
		t.Error("GOMAXPROCS(0) should return error")
	}
}

func TestGroup_GOMAXPROCS_TasksRun(t *testing.T) {
	g := Group{}
	_ = g.GOMAXPROCS(2)
	var n int
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		g.Go(func(ctx context.Context) error {
			mu.Lock()
			n++
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Errorf("Wait() = %v", err)
	}
	if n != 3 {
		t.Errorf("ran %d tasks, want 3", n)
	}
}

func TestGroup_CancelStopsWaitingTasks(t *testing.T) {
	g := WithCancel(context.Background())
	started := make(chan struct{})
	g.Go(func(ctx context.Context) error {
		close(started)
		return errors.New("fail fast")
	})
	g.Go(func(ctx context.Context) error {
		<-started
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return errors.New("task should have been cancelled")
		}
	})
	err := g.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGroup_GOMAXPROCS_LimitsConcurrentWorkers(t *testing.T) {
	g := Group{}
	_ = g.GOMAXPROCS(1)
	var active, maxActive int
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		g.Go(func(ctx context.Context) error {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if maxActive > 1 {
		t.Fatalf("max concurrent workers = %d, want 1", maxActive)
	}
}

func TestGroup_ConcurrentGo_NoRace(t *testing.T) {
	g := Group{}
	_ = g.GOMAXPROCS(2)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Go(func(ctx context.Context) error { return nil })
		}()
	}
	wg.Wait()
	if err := g.Wait(); err != nil {
		t.Errorf("Wait() = %v", err)
	}
}

func TestPanicError_UnwrapErrorValue(t *testing.T) {
	sentinel := errors.New("boom")
	g := WithCancel(context.Background())
	g.Go(func(ctx context.Context) error { panic(sentinel) })
	if err := g.Wait(); !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is = false for %v", err)
	}
}

func TestPanicError_UnwrapNonErrorValue(t *testing.T) {
	if (&PanicError{Value: "text"}).Unwrap() != nil {
		t.Error("Unwrap of non-error value should be nil")
	}
}

func TestPanicError_StackReturnsCopyAndNilSafe(t *testing.T) {
	pe := &PanicError{Value: "x", stack: []byte("goroutine 1")}
	s := pe.Stack()
	s[0] = 'X'
	if string(pe.Stack()) != "goroutine 1" {
		t.Error("Stack() must return a copy")
	}
	var nilPE *PanicError
	if nilPE.Stack() != nil {
		t.Error("nil receiver Stack() should be nil")
	}
}
