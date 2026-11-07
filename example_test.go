package errgroup_test

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/andywang2011/go-errgroup"
)

func ExampleWithContext() {
	var n atomic.Int32
	g := errgroup.WithContext(context.Background())
	g.Go(func(ctx context.Context) error {
		n.Add(1)
		return nil
	})
	g.Go(func(ctx context.Context) error {
		n.Add(1)
		return nil
	})
	err := g.Wait()
	fmt.Println(err == nil, n.Load())
	// Output: true 2
}
