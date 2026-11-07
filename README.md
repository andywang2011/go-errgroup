# go-errgroup

Errgroup with panic recovery and an optional worker pool (`errgroup`).

## Install

```bash
go get github.com/andywang2011/go-errgroup@v0.2.0
```

## Quick start

```go
import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/andywang2011/go-errgroup"
)

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
```

## Examples

```bash
cd go-errgroup/examples && go run .
WORKERS=4 cd go-errgroup/examples && go run .
```

## License

MIT — see [LICENSE](LICENSE).
