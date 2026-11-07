// Tests for Problem 27: Context Cancellation and Deadlines (Best Practices)
//
// Run the stub (expect all panics):
//
//	cd golang && go test ./practice_problems/problem_27_context_deadlines/ -v
//
// Run against your answer via run_tests.sh (use -race!):
//
//	./run_tests.sh \
//	  -f golang/practice_problem_answers/cw_answer_27_context_deadlines.go \
//	  -c go test -race -v .
package deadlines

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var (
	errA     = errors.New("a failed")
	errCause = errors.New("shutdown requested")
)

type timeoutErr struct{}

func (timeoutErr) Error() string { return "i/o timeout" }
func (timeoutErr) Timeout() bool { return true }

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

// remaining returns the time left until ctx's deadline, or -1 if it has none.
func remaining(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return time.Until(dl)
}

func cancelAfter(d time.Duration, cancel context.CancelFunc) {
	go func() {
		time.Sleep(d)
		cancel()
	}()
}

// ---------------------------------------------------------------------------
// PART 1 — Cancellation discipline
// ---------------------------------------------------------------------------

func TestSleepCtx(t *testing.T) {
	t.Run("waits_the_full_duration", func(t *testing.T) {
		start := time.Now()
		if err := SleepCtx(context.Background(), 30*time.Millisecond); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Since(start) < 25*time.Millisecond {
			t.Error("returned before the duration elapsed")
		}
	})

	t.Run("cancel_wakes_it_up", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelAfter(20*time.Millisecond, cancel)
		start := time.Now()
		err := SleepCtx(ctx, 10*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
		if time.Since(start) > time.Second {
			t.Error("SleepCtx did not wake up promptly on cancel")
		}
	})

	t.Run("deadline_returns_deadline_exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := SleepCtx(ctx, 10*time.Second); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
	})

	t.Run("already_done_returns_error_even_for_zero_duration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := SleepCtx(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
	})
}

func TestPoll(t *testing.T) {
	t.Run("stops_when_done", func(t *testing.T) {
		var calls int32
		err := Poll(context.Background(), 10*time.Millisecond, func(ctx context.Context) (bool, error) {
			return atomic.AddInt32(&calls, 1) == 3, nil
		})
		if err != nil || atomic.LoadInt32(&calls) != 3 {
			t.Errorf("err=%v calls=%d, want nil and 3", err, atomic.LoadInt32(&calls))
		}
	})

	t.Run("first_call_is_immediate", func(t *testing.T) {
		start := time.Now()
		err := Poll(context.Background(), 10*time.Second, func(ctx context.Context) (bool, error) {
			return true, nil
		})
		if err != nil || time.Since(start) > time.Second {
			t.Errorf("err=%v elapsed=%v, want nil and an immediate first call", err, time.Since(start))
		}
	})

	t.Run("returns_fn_error", func(t *testing.T) {
		err := Poll(context.Background(), 5*time.Millisecond, func(ctx context.Context) (bool, error) {
			return false, errA
		})
		if !errors.Is(err, errA) {
			t.Errorf("got %v, want errA", err)
		}
	})

	t.Run("cancel_stops_loop_and_no_calls_after_return", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls int32
		cancelAfter(55*time.Millisecond, cancel)
		err := Poll(ctx, 10*time.Millisecond, func(ctx context.Context) (bool, error) {
			atomic.AddInt32(&calls, 1)
			return false, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
		n := atomic.LoadInt32(&calls)
		if n < 3 || n > 8 {
			t.Errorf("fn called %d times, want roughly one per interval (3-8)", n)
		}
		time.Sleep(40 * time.Millisecond)
		if atomic.LoadInt32(&calls) != n {
			t.Error("fn was called after Poll returned")
		}
	})

	t.Run("already_cancelled_never_calls_fn", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls int32
		err := Poll(ctx, time.Millisecond, func(ctx context.Context) (bool, error) {
			atomic.AddInt32(&calls, 1)
			return true, nil
		})
		if !errors.Is(err, context.Canceled) || atomic.LoadInt32(&calls) != 0 {
			t.Errorf("err=%v calls=%d, want Canceled and 0", err, atomic.LoadInt32(&calls))
		}
	})
}

func TestCallWithTimeout(t *testing.T) {
	t.Run("returns_result", func(t *testing.T) {
		got, err := CallWithTimeout(context.Background(), time.Second, func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil || got != "ok" {
			t.Errorf("got (%q, %v), want (ok, nil)", got, err)
		}
	})

	t.Run("passes_fn_error_through", func(t *testing.T) {
		_, err := CallWithTimeout(context.Background(), time.Second, func(ctx context.Context) (string, error) {
			return "", errA
		})
		if !errors.Is(err, errA) {
			t.Errorf("got %v, want errA", err)
		}
	})

	t.Run("fn_sees_the_deadline", func(t *testing.T) {
		_, err := CallWithTimeout(context.Background(), 500*time.Millisecond, func(ctx context.Context) (string, error) {
			if r := remaining(ctx); r <= 0 || r > 500*time.Millisecond {
				t.Errorf("fn context deadline remaining = %v, want (0, 500ms]", r)
			}
			return "", nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("abandons_fn_that_ignores_ctx", func(t *testing.T) {
		before := runtime.NumGoroutine()
		fn := func(ctx context.Context) (string, error) {
			time.Sleep(300 * time.Millisecond) // ignores ctx on purpose
			return "late", nil
		}
		start := time.Now()
		got, err := CallWithTimeout(context.Background(), 30*time.Millisecond, fn)
		if !errors.Is(err, context.DeadlineExceeded) || got != "" {
			t.Errorf("got (%q, %v), want (\"\", DeadlineExceeded)", got, err)
		}
		if time.Since(start) > 200*time.Millisecond {
			t.Error("did not return at the deadline while fn was still running")
		}
		assertNoLeak(t, before) // the abandoned goroutine must be able to exit
	})

	t.Run("parent_cancel_returns_canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelAfter(20*time.Millisecond, cancel)
		_, err := CallWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
	})
}

// ---------------------------------------------------------------------------
// PART 2 — Deadline propagation and error handling
// ---------------------------------------------------------------------------

func TestRunSequential(t *testing.T) {
	t.Run("unused_time_rolls_over", func(t *testing.T) {
		var r0, r1, r2 time.Duration
		steps := []func(context.Context) error{
			func(ctx context.Context) error { r0 = remaining(ctx); return nil },
			func(ctx context.Context) error { r1 = remaining(ctx); return nil },
			func(ctx context.Context) error { r2 = remaining(ctx); return nil },
		}
		if err := RunSequential(context.Background(), 300*time.Millisecond, steps); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r0 <= 50*time.Millisecond || r0 > 100*time.Millisecond {
			t.Errorf("step 0 budget = %v, want (50ms, 100ms]", r0)
		}
		if r1 <= 100*time.Millisecond || r1 > 150*time.Millisecond {
			t.Errorf("step 1 budget = %v, want (100ms, 150ms]", r1)
		}
		if r2 <= 200*time.Millisecond || r2 > 300*time.Millisecond {
			t.Errorf("step 2 budget = %v, want (200ms, 300ms]", r2)
		}
	})

	t.Run("step_context_is_cancelled_when_the_step_returns", func(t *testing.T) {
		var first context.Context
		steps := []func(context.Context) error{
			func(ctx context.Context) error { first = ctx; return nil },
			func(ctx context.Context) error {
				if first.Err() == nil {
					t.Error("previous step's context was not cancelled before the next step started")
				}
				return nil
			},
		}
		if err := RunSequential(context.Background(), time.Second, steps); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("slow_step_is_cut_off_at_its_share", func(t *testing.T) {
		var r1 time.Duration
		steps := []func(context.Context) error{
			func(ctx context.Context) error { <-ctx.Done(); return nil },
			func(ctx context.Context) error { r1 = remaining(ctx); return nil },
			func(ctx context.Context) error { return nil },
		}
		start := time.Now()
		if err := RunSequential(context.Background(), 300*time.Millisecond, steps); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Since(start) > 250*time.Millisecond {
			t.Errorf("run took %v, the slow step was not limited to its share", time.Since(start))
		}
		if r1 <= 50*time.Millisecond || r1 > 100*time.Millisecond {
			t.Errorf("step 1 budget = %v, want (50ms, 100ms]", r1)
		}
	})

	t.Run("error_stops_the_run_and_is_wrapped", func(t *testing.T) {
		var ran bool
		steps := []func(context.Context) error{
			func(ctx context.Context) error { return errA },
			func(ctx context.Context) error { ran = true; return nil },
		}
		err := RunSequential(context.Background(), time.Second, steps)
		if !errors.Is(err, errA) || !strings.Contains(fmt.Sprint(err), "step 0") {
			t.Errorf("got %v, want errA wrapped with 'step 0'", err)
		}
		if ran {
			t.Error("step 1 ran after step 0 failed")
		}
	})

	t.Run("parent_deadline_wins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := RunSequential(ctx, 5*time.Second, []func(context.Context) error{
			func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Error("the budget extended the parent's deadline")
		}
	})

	t.Run("already_done_skips_steps", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var ran bool
		err := RunSequential(ctx, time.Second, []func(context.Context) error{
			func(ctx context.Context) error { ran = true; return nil },
		})
		if !errors.Is(err, context.Canceled) || ran {
			t.Errorf("err=%v ran=%v, want Canceled and no step run", err, ran)
		}
	})

	t.Run("no_steps", func(t *testing.T) {
		if err := RunSequential(context.Background(), time.Second, nil); err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})
}

func TestRetryWithDeadline(t *testing.T) {
	t.Run("gives_up_early_when_backoff_exceeds_time_left", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		var calls int32
		start := time.Now()
		err := RetryWithDeadline(ctx, 5, time.Second, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if !errors.Is(err, ErrNoTimeLeft) || !errors.Is(err, errA) {
			t.Errorf("got %v, want ErrNoTimeLeft wrapping errA", err)
		}
		if atomic.LoadInt32(&calls) != 1 {
			t.Errorf("fn called %d times, want 1", atomic.LoadInt32(&calls))
		}
		if time.Since(start) > 300*time.Millisecond {
			t.Error("slept even though the deadline would pass first")
		}
	})

	t.Run("retries_until_success", func(t *testing.T) {
		var calls int32
		err := RetryWithDeadline(context.Background(), 5, time.Millisecond, func(ctx context.Context) error {
			if atomic.AddInt32(&calls, 1) < 3 {
				return errA
			}
			return nil
		})
		if err != nil || atomic.LoadInt32(&calls) != 3 {
			t.Errorf("err=%v calls=%d, want nil and 3", err, atomic.LoadInt32(&calls))
		}
	})

	t.Run("returns_last_error_when_attempts_run_out", func(t *testing.T) {
		var calls int32
		err := RetryWithDeadline(context.Background(), 2, time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if !errors.Is(err, errA) || errors.Is(err, ErrNoTimeLeft) || atomic.LoadInt32(&calls) != 2 {
			t.Errorf("err=%v calls=%d, want plain errA and 2", err, atomic.LoadInt32(&calls))
		}
	})

	t.Run("cancel_during_backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelAfter(30*time.Millisecond, cancel)
		var calls int32
		start := time.Now()
		err := RetryWithDeadline(ctx, 10, 5*time.Second, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
		if time.Since(start) > time.Second || atomic.LoadInt32(&calls) != 1 {
			t.Errorf("elapsed=%v calls=%d, want a prompt return after 1 call", time.Since(start), atomic.LoadInt32(&calls))
		}
	})

	t.Run("already_cancelled_never_calls_fn", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls int32
		err := RetryWithDeadline(ctx, 3, time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
		if !errors.Is(err, context.Canceled) || atomic.LoadInt32(&calls) != 0 {
			t.Errorf("err=%v calls=%d, want Canceled and 0", err, atomic.LoadInt32(&calls))
		}
	})

	t.Run("fn_receives_the_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := RetryWithDeadline(ctx, 1, time.Millisecond, func(c context.Context) error {
			if remaining(c) <= 0 {
				t.Error("fn did not receive a context carrying the caller's deadline")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Outcome
	}{
		{"nil", nil, OutcomeOK},
		{"deadline", context.DeadlineExceeded, OutcomeTimeout},
		{"wrapped_deadline", fmt.Errorf("query users: %w", context.DeadlineExceeded), OutcomeTimeout},
		{"canceled", context.Canceled, OutcomeCanceled},
		{"wrapped_canceled", fmt.Errorf("handler: %w", context.Canceled), OutcomeCanceled},
		{"no_time_left", fmt.Errorf("%w: %w", ErrNoTimeLeft, errA), OutcomeTimeout},
		{"timeout_interface", timeoutErr{}, OutcomeTimeout},
		{"wrapped_timeout_interface", fmt.Errorf("dial: %w", timeoutErr{}), OutcomeTimeout},
		{"timeout_beats_canceled", errors.Join(context.Canceled, context.DeadlineExceeded), OutcomeTimeout},
		{"plain", errA, OutcomeError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PART 3 — Causes, AfterFunc and hedged requests
// ---------------------------------------------------------------------------

func TestWatchCancel(t *testing.T) {
	t.Run("reports_the_cancellation_cause", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		got := make(chan error, 1)
		WatchCancel(ctx, func(cause error) { got <- cause })
		cancel(errCause)
		select {
		case cause := <-got:
			if !errors.Is(cause, errCause) {
				t.Errorf("cause = %v, want errCause", cause)
			}
		case <-time.After(time.Second):
			t.Fatal("onCancel was never called")
		}
	})

	t.Run("reports_timeout_cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Millisecond, errCause)
		defer cancel()
		got := make(chan error, 1)
		WatchCancel(ctx, func(cause error) { got <- cause })
		select {
		case cause := <-got:
			if !errors.Is(cause, errCause) {
				t.Errorf("cause = %v, want errCause", cause)
			}
		case <-time.After(time.Second):
			t.Fatal("onCancel was never called")
		}
	})

	t.Run("stop_prevents_the_callback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var called int32
		stop := WatchCancel(ctx, func(error) { atomic.AddInt32(&called, 1) })
		if !stop() {
			t.Error("stop() = false, want true when the callback had not started")
		}
		cancel()
		time.Sleep(50 * time.Millisecond)
		if atomic.LoadInt32(&called) != 0 {
			t.Error("callback ran after stop()")
		}
	})

	t.Run("callback_does_not_block_the_canceller", func(t *testing.T) {
		release := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		WatchCancel(ctx, func(error) { <-release })
		start := time.Now()
		cancel()
		if time.Since(start) > 200*time.Millisecond {
			t.Error("cancel() was blocked by a slow onCancel")
		}
		close(release)
	})

	t.Run("already_cancelled_runs_once", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var called int32
		done := make(chan struct{})
		WatchCancel(ctx, func(error) {
			atomic.AddInt32(&called, 1)
			close(done)
		})
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("onCancel was never called for an already-cancelled context")
		}
		time.Sleep(20 * time.Millisecond)
		if n := atomic.LoadInt32(&called); n != 1 {
			t.Errorf("onCancel called %d times, want 1", n)
		}
	})

	t.Run("stopped_watchers_leave_no_goroutines", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var stops []func() bool
		for i := 0; i < 100; i++ {
			stops = append(stops, WatchCancel(ctx, func(error) {}))
		}
		for _, stop := range stops {
			if !stop() {
				t.Fatal("stop() = false for a watcher that never fired")
			}
		}
		assertNoLeak(t, before)
	})
}

func TestHedged(t *testing.T) {
	t.Run("fast_first_attempt_is_not_hedged", func(t *testing.T) {
		var calls int32
		got, err := Hedged(context.Background(), 50*time.Millisecond, 3, func(ctx context.Context, attempt int) (string, error) {
			atomic.AddInt32(&calls, 1)
			return "a0", nil
		})
		if err != nil || got != "a0" {
			t.Fatalf("got (%q, %v), want (a0, nil)", got, err)
		}
		time.Sleep(80 * time.Millisecond)
		if n := atomic.LoadInt32(&calls); n != 1 {
			t.Errorf("fn called %d times, want 1 (no hedge needed)", n)
		}
	})

	t.Run("slow_first_attempt_is_hedged_and_loser_is_cancelled", func(t *testing.T) {
		var sawCancel int32
		fn := func(ctx context.Context, attempt int) (string, error) {
			if attempt == 0 {
				<-ctx.Done()
				atomic.StoreInt32(&sawCancel, 1)
				return "", ctx.Err()
			}
			time.Sleep(5 * time.Millisecond)
			return "hedge", nil
		}
		start := time.Now()
		got, err := Hedged(context.Background(), 20*time.Millisecond, 2, fn)
		if err != nil || got != "hedge" {
			t.Fatalf("got (%q, %v), want (hedge, nil)", got, err)
		}
		if atomic.LoadInt32(&sawCancel) != 1 {
			t.Error("the losing attempt must be cancelled AND waited for before Hedged returns")
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Error("Hedged did not return as soon as the hedge succeeded")
		}
	})

	t.Run("failure_starts_next_attempt_immediately", func(t *testing.T) {
		fn := func(ctx context.Context, attempt int) (string, error) {
			if attempt == 0 {
				return "", errA
			}
			return "ok", nil
		}
		start := time.Now()
		got, err := Hedged(context.Background(), 5*time.Second, 2, fn)
		if err != nil || got != "ok" {
			t.Fatalf("got (%q, %v), want (ok, nil)", got, err)
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Error("waited for the hedge delay after a failure")
		}
	})

	t.Run("all_attempts_fail", func(t *testing.T) {
		var calls int32
		_, err := Hedged(context.Background(), 5*time.Millisecond, 3, func(ctx context.Context, attempt int) (string, error) {
			atomic.AddInt32(&calls, 1)
			return "", errA
		})
		if !errors.Is(err, ErrAllFailed) || !errors.Is(err, errA) {
			t.Errorf("got %v, want ErrAllFailed wrapping errA", err)
		}
		if n := atomic.LoadInt32(&calls); n != 3 {
			t.Errorf("fn called %d times, want 3", n)
		}
	})

	t.Run("bounded_by_max_attempts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		var calls int32
		_, err := Hedged(ctx, 5*time.Millisecond, 2, func(ctx context.Context, attempt int) (string, error) {
			atomic.AddInt32(&calls, 1)
			<-ctx.Done()
			return "", ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
		if n := atomic.LoadInt32(&calls); n != 2 {
			t.Errorf("fn called %d times, want exactly maxAttempts=2", n)
		}
	})

	t.Run("waits_for_attempts_when_ctx_is_done", func(t *testing.T) {
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		var active int32
		_, err := Hedged(ctx, 10*time.Millisecond, 3, func(ctx context.Context, attempt int) (string, error) {
			atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			return "", ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
		if n := atomic.LoadInt32(&active); n != 0 {
			t.Errorf("%d attempts still running when Hedged returned", n)
		}
		assertNoLeak(t, before)
	})
}
