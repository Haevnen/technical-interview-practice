// Tests for Problem 24: Streaming Data Pipeline (Fan-Out / Fan-In)
//
// Run the stub (expect all panics):
//
//	cd golang && go test ./practice_problems/problem_24_stream_pipeline/ -v
//
// Run against your answer via run_tests.sh (use -race!):
//
//	./run_tests.sh \
//	  -f golang/practice_problem_answers/cw_answer_24_stream_pipeline.go \
//	  -c go test -race -v .
package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func ints(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// collectTimeout drains ch and fails the test if it is not closed in time.
func collectTimeout(t *testing.T, ch <-chan int) []int {
	t.Helper()
	done := make(chan []int, 1)
	go func() {
		var out []int
		for v := range ch {
			out = append(out, v)
		}
		done <- out
	}()
	select {
	case out := <-done:
		return out
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for channel to close")
		return nil
	}
}

// assertNoLeak waits for the goroutine count to fall back to `before`.
func assertNoLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine leak: %d running, want <= %d", runtime.NumGoroutine(), before)
}

// expectClosed fails unless ch is closed (and empty) shortly.
func expectClosed(t *testing.T, ch <-chan int) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed, received a value")
		}
	case <-time.After(2 * time.Second):
		t.Error("channel was not closed after cancellation")
	}
}

func double(v int) int { return v * 2 }

// ---------------------------------------------------------------------------
// PART 1 — Generator and sequential stages
// ---------------------------------------------------------------------------

func TestGenerate(t *testing.T) {
	t.Run("emits_all_in_order", func(t *testing.T) {
		got := collectTimeout(t, Generate(context.Background(), ints(5)))
		if !equalInts(got, ints(5)) {
			t.Errorf("got %v, want %v", got, ints(5))
		}
	})

	t.Run("empty_input_closes_channel", func(t *testing.T) {
		if got := collectTimeout(t, Generate(context.Background(), nil)); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})

	t.Run("stops_on_cancel_without_reader", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		ch := Generate(ctx, ints(1000))
		time.Sleep(10 * time.Millisecond)
		cancel()
		assertNoLeak(t, before)
		_ = ch
	})
}

func TestMap(t *testing.T) {
	t.Run("transforms_in_order", func(t *testing.T) {
		ctx := context.Background()
		got := collectTimeout(t, Map(ctx, Generate(ctx, ints(5)), double))
		want := []int{2, 4, 6, 8, 10}
		if !equalInts(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("stages_compose", func(t *testing.T) {
		ctx := context.Background()
		inc := func(v int) int { return v + 1 }
		got := collectTimeout(t, Map(ctx, Map(ctx, Generate(ctx, ints(3)), double), inc))
		want := []int{3, 5, 7}
		if !equalInts(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("exits_on_cancel_while_waiting_for_input", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan int) // never sent to, never closed
		out := Map(ctx, in, double)
		time.Sleep(10 * time.Millisecond)
		cancel()
		expectClosed(t, out)
		assertNoLeak(t, before)
	})
}

func TestCollect(t *testing.T) {
	t.Run("collects_everything", func(t *testing.T) {
		got := Collect(Generate(context.Background(), ints(4)))
		if !equalInts(got, ints(4)) {
			t.Errorf("got %v, want %v", got, ints(4))
		}
	})

	t.Run("empty_stream", func(t *testing.T) {
		ch := make(chan int)
		close(ch)
		if got := Collect(ch); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
}

// ---------------------------------------------------------------------------
// PART 2 — Fan-out and fan-in
// ---------------------------------------------------------------------------

func TestParallelMap(t *testing.T) {
	t.Run("all_results_present", func(t *testing.T) {
		ctx := context.Background()
		got := collectTimeout(t, ParallelMap(ctx, Generate(ctx, ints(50)), 4, double))
		sort.Ints(got)
		if len(got) != 50 {
			t.Fatalf("got %d results, want 50", len(got))
		}
		for i, v := range got {
			if v != (i+1)*2 {
				t.Fatalf("sorted[%d] = %d, want %d", i, v, (i+1)*2)
			}
		}
	})

	t.Run("concurrency_bounded_by_workers", func(t *testing.T) {
		var cur, max int32
		fn := func(v int) int {
			n := atomic.AddInt32(&cur, 1)
			for {
				m := atomic.LoadInt32(&max)
				if n <= m || atomic.CompareAndSwapInt32(&max, m, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			return v
		}
		ctx := context.Background()
		collectTimeout(t, ParallelMap(ctx, Generate(ctx, ints(16)), 4, fn))
		if got := atomic.LoadInt32(&max); got > 4 {
			t.Errorf("max concurrent fn calls = %d, want <= 4", got)
		} else if got < 2 {
			t.Errorf("max concurrent fn calls = %d, workers are not running in parallel", got)
		}
	})

	t.Run("zero_workers_treated_as_one", func(t *testing.T) {
		ctx := context.Background()
		got := collectTimeout(t, ParallelMap(ctx, Generate(ctx, ints(5)), 0, double))
		if len(got) != 5 {
			t.Errorf("got %d results, want 5", len(got))
		}
	})

	t.Run("cancel_without_reader_does_not_leak", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		out := ParallelMap(ctx, Generate(ctx, ints(1000)), 4, double)
		time.Sleep(20 * time.Millisecond)
		cancel()
		assertNoLeak(t, before)
		_ = out
	})

	t.Run("exits_on_cancel_while_waiting_for_input", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		out := ParallelMap(ctx, make(chan int), 3, double)
		time.Sleep(10 * time.Millisecond)
		cancel()
		expectClosed(t, out)
		assertNoLeak(t, before)
	})
}

func TestMerge(t *testing.T) {
	t.Run("merges_all_inputs", func(t *testing.T) {
		ctx := context.Background()
		got := collectTimeout(t, Merge(ctx,
			Generate(ctx, []int{1, 2, 3}),
			Generate(ctx, []int{10, 20}),
			Generate(ctx, []int{100}),
		))
		sort.Ints(got)
		want := []int{1, 2, 3, 10, 20, 100}
		if !equalInts(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("preserves_order_within_one_input", func(t *testing.T) {
		ctx := context.Background()
		got := collectTimeout(t, Merge(ctx, Generate(ctx, ints(10))))
		if !equalInts(got, ints(10)) {
			t.Errorf("got %v, want %v", got, ints(10))
		}
	})

	t.Run("no_inputs_returns_closed_channel", func(t *testing.T) {
		if got := collectTimeout(t, Merge(context.Background())); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})

	t.Run("cancel_with_idle_inputs_does_not_leak", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		a, b := make(chan int), make(chan int) // never closed
		out := Merge(ctx, a, b)
		time.Sleep(10 * time.Millisecond)
		cancel()
		expectClosed(t, out)
		assertNoLeak(t, before)
	})

	t.Run("cancel_without_reader_does_not_leak", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		out := Merge(ctx, Generate(ctx, ints(100)), Generate(ctx, ints(100)))
		time.Sleep(20 * time.Millisecond)
		cancel()
		assertNoLeak(t, before)
		_ = out
	})
}

// ---------------------------------------------------------------------------
// PART 3 — Ordered processing with first-error-wins cancellation
// ---------------------------------------------------------------------------

func TestProcessAll(t *testing.T) {
	t.Run("preserves_input_order", func(t *testing.T) {
		fn := func(ctx context.Context, v int) (int, error) {
			time.Sleep(time.Duration(10-v) * time.Millisecond) // later items finish first
			return v * 10, nil
		}
		got, err := ProcessAll(context.Background(), ints(9), 4, fn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range got {
			if v != (i+1)*10 {
				t.Fatalf("results[%d] = %d, want %d (full: %v)", i, v, (i+1)*10, got)
			}
		}
		if len(got) != 9 {
			t.Errorf("got %d results, want 9", len(got))
		}
	})

	t.Run("empty_input", func(t *testing.T) {
		fn := func(ctx context.Context, v int) (int, error) { return v, nil }
		got, err := ProcessAll(context.Background(), nil, 3, fn)
		if err != nil || len(got) != 0 {
			t.Errorf("got (%v, %v), want (empty, nil)", got, err)
		}
	})

	t.Run("concurrency_bounded_by_workers", func(t *testing.T) {
		var cur, max int32
		fn := func(ctx context.Context, v int) (int, error) {
			n := atomic.AddInt32(&cur, 1)
			for {
				m := atomic.LoadInt32(&max)
				if n <= m || atomic.CompareAndSwapInt32(&max, m, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			return v, nil
		}
		if _, err := ProcessAll(context.Background(), ints(12), 3, fn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := atomic.LoadInt32(&max); got > 3 {
			t.Errorf("max concurrent fn calls = %d, want <= 3", got)
		} else if got < 2 {
			t.Errorf("max concurrent fn calls = %d, workers are not running in parallel", got)
		}
	})

	t.Run("first_error_cancels_the_rest", func(t *testing.T) {
		before := runtime.NumGoroutine()
		fn := func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errBoom
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(5 * time.Second):
				return v, nil
			}
		}
		start := time.Now()
		got, err := ProcessAll(context.Background(), ints(4), 4, fn)
		if !errors.Is(err, errBoom) {
			t.Errorf("expected errBoom, got %v", err)
		}
		if got != nil {
			t.Errorf("expected nil results on error, got %v", got)
		}
		if time.Since(start) > time.Second {
			t.Error("ProcessAll did not cancel in-flight work promptly")
		}
		assertNoLeak(t, before)
	})

	t.Run("error_stops_handing_out_new_items", func(t *testing.T) {
		var calls int32
		fn := func(ctx context.Context, v int) (int, error) {
			atomic.AddInt32(&calls, 1)
			if v == 1 {
				return 0, errBoom
			}
			<-ctx.Done()
			return 0, ctx.Err()
		}
		_, err := ProcessAll(context.Background(), ints(100), 2, fn)
		if !errors.Is(err, errBoom) {
			t.Errorf("expected errBoom, got %v", err)
		}
		if got := atomic.LoadInt32(&calls); got > 10 {
			t.Errorf("fn called %d times after an early error, expected far fewer than 100", got)
		}
	})

	t.Run("parent_cancel_returns_ctx_error", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		fn := func(ctx context.Context, v int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		got, err := ProcessAll(ctx, ints(10), 3, fn)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		if got != nil {
			t.Errorf("expected nil results, got %v", got)
		}
		assertNoLeak(t, before)
	})
}
