// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errgroup

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// PanicError is returned when a Go callback panics.
// Error() omits the stack; use Stack() for diagnostics.
type PanicError struct {
	Value any
	stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("errgroup: panic recovered: %v", e.Value)
}

// Stack returns the goroutine stack captured at recover time.
func (e *PanicError) Stack() []byte {
	if e == nil {
		return nil
	}
	out := make([]byte, len(e.stack))
	copy(out, e.stack)
	return out
}

// Unwrap returns the panic value when it is an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// Group is a collection of goroutines working on subtasks of a common task.
//
// A zero Group is valid and does not cancel on error.
// Do not call Go after Wait has returned; the group is single-use.
type Group struct {
	err     error
	wg      sync.WaitGroup
	errOnce sync.Once

	workerOnce sync.Once
	ch         chan func(ctx context.Context) error
	chs        []func(ctx context.Context) error
	mu         sync.Mutex

	ctx    context.Context
	cancel func()
}

// WithContext creates a Group whose Go callbacks receive ctx.
func WithContext(ctx context.Context) *Group {
	return &Group{ctx: ctx}
}

// WithCancel creates a Group with a derived context cancelled on first error or Wait.
func WithCancel(ctx context.Context) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{ctx: ctx, cancel: cancel}
}

func (g *Group) do(f func(ctx context.Context) error) {
	ctx := g.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			err = &PanicError{Value: r, stack: buf}
		}
		if err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel()
				}
			})
		}
		g.wg.Done()
	}()
	err = f(ctx)
}

// GOMAXPROCS limits concurrent workers when set before Go calls.
func (g *Group) GOMAXPROCS(n int) error {
	if n <= 0 {
		return errors.New("errgroup: GOMAXPROCS must be greater than 0")
	}
	g.workerOnce.Do(func() {
		g.ch = make(chan func(context.Context) error, n)
		for i := 0; i < n; i++ {
			go func() {
				for f := range g.ch {
					g.do(f)
				}
			}()
		}
	})
	return nil
}

// Go runs f in a new goroutine (or worker pool when GOMAXPROCS was set).
func (g *Group) Go(f func(ctx context.Context) error) {
	g.wg.Add(1)
	if g.ch != nil {
		g.mu.Lock()
		select {
		case g.ch <- f:
			g.mu.Unlock()
			return
		default:
			g.chs = append(g.chs, f)
			g.mu.Unlock()
			return
		}
	}
	go g.do(f)
}

// Wait blocks until all Go callbacks finish and returns the first error.
func (g *Group) Wait() error {
	if g.ch != nil {
		g.mu.Lock()
		pending := g.chs
		g.chs = nil
		g.mu.Unlock()
		for _, f := range pending {
			g.ch <- f
		}
	}
	g.wg.Wait()
	if g.ch != nil {
		close(g.ch)
	}
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
}
