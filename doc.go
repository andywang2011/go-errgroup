// Package errgroup extends golang.org/x/sync/errgroup with panic recovery and an optional worker pool.
//
// Design constraints:
//   - No viper, no global singletons, and no log.Fatal inside the library.
//   - ConfigDefault: N/A — utility concurrency helper with no configuration struct.
//   - Callers own lifecycle: create a Group, run Go callbacks, then Wait once (single-use per Group).
//   - Use WithCancel when sibling goroutines should stop after the first error.
//   - Panic stacks are not included in Error(); use PanicError.Stack() for diagnostics.
//   - The library does not log PII; redaction is the caller's responsibility.
package errgroup
