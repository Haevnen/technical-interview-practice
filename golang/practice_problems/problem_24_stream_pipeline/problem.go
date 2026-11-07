// =============================================================================
// INTERVIEW PROBLEM 24: Streaming Data Pipeline (Fan-Out / Fan-In)
// Difficulty: Senior Software Engineer | Estimated time: 45 min
// Concurrency patterns: generator, pipeline stages, fan-out, fan-in (merge),
// context cancellation, goroutine-leak avoidance, first-error-wins
// =============================================================================
//
// CONTEXT
// -------
// You're building the ingestion path of a log-analytics product for a
// developer-tools platform. Records flow through stages connected by channels:
// a source emits values, stages transform them (some in parallel), and a sink
// collects the output. Records are plain ints to keep the focus on concurrency.
//
// RULES (apply to every function in this file)
// --------------------------------------------
//   - A function that returns a channel owns it and must close it when it is done.
//   - Never close a channel you only receive from.
//   - Every goroutine you start must exit when ctx is cancelled, even if nobody
//     is reading its output or its input channel is never closed.
//   - Never block forever on a send or receive without also selecting on ctx.Done().
//
// Run your tests with the race detector:
//
//	./run_tests.sh -f golang/practice_problem_answers/cw_answer_24_stream_pipeline.go -c go test -race -v .
//
// EXAMPLE
// -------
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	src := Generate(ctx, []int{1, 2, 3, 4})
//	squared := ParallelMap(ctx, src, 3, func(v int) int { return v * v })
//	out := Collect(squared) // some permutation of [1 4 9 16]
// =============================================================================

package pipeline

import "context"

// ---------------------------------------------------------------------------
// PART 1 — Generator and sequential stages  (~15 min)
// ---------------------------------------------------------------------------

// Generate returns a channel that emits items in order, then closes.
// It stops early (and closes the channel) if ctx is cancelled.
func Generate(ctx context.Context, items []int) <-chan int {
	panic("not implemented")
}

// Map returns a channel of fn(v) for each v received from in, preserving order.
// It closes its output when in is closed or ctx is cancelled. It must also exit
// promptly on cancellation while it is waiting for input that never arrives.
func Map(ctx context.Context, in <-chan int, fn func(int) int) <-chan int {
	panic("not implemented")
}

// Collect reads in until it is closed and returns everything received, in
// arrival order. It returns an empty (possibly nil) slice for an empty stream.
func Collect(in <-chan int) []int {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// PART 2 — Fan-out and fan-in  (~15 min)
// ---------------------------------------------------------------------------

// ParallelMap applies fn to values from in using `workers` goroutines (fan-out)
// and emits results on one output channel. Output order is NOT guaranteed.
// At most `workers` calls to fn run at once; workers < 1 is treated as 1.
// The output is closed after in is closed and all workers are finished, or when
// ctx is cancelled (no leaked goroutines, even if nobody reads the output).
func ParallelMap(ctx context.Context, in <-chan int, workers int, fn func(int) int) <-chan int {
	panic("not implemented")
}

// Merge combines several channels into one (fan-in). Order across inputs is not
// guaranteed; order within one input is preserved. The output closes once every
// input is closed, or when ctx is cancelled. Merge with zero inputs returns an
// already-closed channel.
func Merge(ctx context.Context, chans ...<-chan int) <-chan int {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// PART 3 — Ordered processing with first-error-wins cancellation  (~15 min)
// ---------------------------------------------------------------------------

// ProcessAll runs fn over every item using at most `workers` goroutines
// (workers < 1 is treated as 1) and returns the results in INPUT order
// (results[i] corresponds to items[i]).
//
// If any call returns an error, ProcessAll cancels the context passed to fn,
// stops handing out new items, waits for all workers to exit, and returns
// (nil, firstErr) where firstErr is the first error produced by fn (not the
// context.Canceled caused by the cancellation itself).
//
// If the parent ctx is cancelled before all items finish, return (nil, ctx.Err()).
// ProcessAll must not leak goroutines on any path.
func ProcessAll(ctx context.Context, items []int, workers int, fn func(context.Context, int) (int, error)) ([]int, error) {
	panic("not implemented")
}
