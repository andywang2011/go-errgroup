// Worker-pool example: cap concurrent tasks with GOMAXPROCS (override via WORKERS env).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/andywang2011/go-errgroup"
)

func main() {
	workers := 2
	if v := os.Getenv("WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}

	g := errgroup.Group{}
	if err := g.GOMAXPROCS(workers); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 4; i++ {
		i := i
		g.Go(func(ctx context.Context) error {
			fmt.Println("task", i)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}
