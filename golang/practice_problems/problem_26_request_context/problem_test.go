// Tests for Problem 26: Request Context Propagation (Backend)
//
// Run the stub (expect all panics):
//
//	cd golang && go test ./practice_problems/problem_26_request_context/ -v
//
// Run against your answer via run_tests.sh (use -race!):
//
//	./run_tests.sh \
//	  -f golang/practice_problem_answers/cw_answer_26_request_context.go \
//	  -c go test -race -v .
package reqctx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var (
	errA = errors.New("a failed")
	errB = errors.New("b failed")
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func okHandler(ctx context.Context, req Request) (Response, error) {
	return Response{Status: 200, Body: req.Path}, nil
}

func blockingHandler(ctx context.Context, req Request) (Response, error) {
	<-ctx.Done()
	return Response{}, ctx.Err()
}

func expectDone(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("context was not done in time")
	}
}

// ---------------------------------------------------------------------------
// PART 1 — Request-scoped values and middleware
// ---------------------------------------------------------------------------

func TestRequestID(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		ctx := WithRequestID(context.Background(), "req-1")
		if id, ok := RequestID(ctx); !ok || id != "req-1" {
			t.Errorf("got (%q, %v), want (req-1, true)", id, ok)
		}
	})

	t.Run("missing", func(t *testing.T) {
		if id, ok := RequestID(context.Background()); ok {
			t.Errorf("got (%q, true), want not found", id)
		}
	})

	t.Run("plain_string_key_does_not_collide", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "request_id", "evil")
		if id, ok := RequestID(ctx); ok {
			t.Errorf("plain string key leaked into RequestID: %q", id)
		}
		ctx = WithRequestID(context.Background(), "real")
		ctx = context.WithValue(ctx, "request_id", "evil")
		if id, _ := RequestID(ctx); id != "real" {
			t.Errorf("got %q, want real", id)
		}
	})

	t.Run("inner_value_overrides_and_children_inherit", func(t *testing.T) {
		outer := WithRequestID(context.Background(), "outer")
		inner := WithRequestID(outer, "inner")
		child, cancel := context.WithCancel(inner)
		defer cancel()
		if id, _ := RequestID(child); id != "inner" {
			t.Errorf("child sees %q, want inner", id)
		}
		if id, _ := RequestID(outer); id != "outer" {
			t.Errorf("outer changed to %q", id)
		}
	})
}

func TestUser(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		ctx := WithUser(context.Background(), "alice")
		if u, ok := User(ctx); !ok || u != "alice" {
			t.Errorf("got (%q, %v), want (alice, true)", u, ok)
		}
		if _, ok := User(context.Background()); ok {
			t.Error("empty context should have no user")
		}
	})

	t.Run("independent_of_request_id", func(t *testing.T) {
		ctx := WithUser(WithRequestID(context.Background(), "r1"), "bob")
		if id, _ := RequestID(ctx); id != "r1" {
			t.Errorf("request ID = %q, want r1", id)
		}
		if u, _ := User(ctx); u != "bob" {
			t.Errorf("user = %q, want bob", u)
		}
	})
}

func TestRequestIDMiddleware(t *testing.T) {
	var seen string
	capture := func(ctx context.Context, req Request) (Response, error) {
		seen, _ = RequestID(ctx)
		return Response{Status: 200}, nil
	}

	t.Run("generates_when_absent", func(t *testing.T) {
		var gens int
		h := RequestIDMiddleware(func() string { gens++; return "gen-1" }, capture)
		if _, err := h(context.Background(), Request{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if seen != "gen-1" || gens != 1 {
			t.Errorf("seen=%q gens=%d, want gen-1 and 1", seen, gens)
		}
	})

	t.Run("keeps_existing_and_skips_generator", func(t *testing.T) {
		var gens int
		h := RequestIDMiddleware(func() string { gens++; return "gen-2" }, capture)
		h(WithRequestID(context.Background(), "upstream"), Request{})
		if seen != "upstream" || gens != 0 {
			t.Errorf("seen=%q gens=%d, want upstream and 0", seen, gens)
		}
	})
}

func TestRequireUser(t *testing.T) {
	t.Run("rejects_anonymous", func(t *testing.T) {
		var called bool
		h := RequireUser(func(ctx context.Context, req Request) (Response, error) {
			called = true
			return Response{Status: 200}, nil
		})
		_, err := h(context.Background(), Request{})
		if !errors.Is(err, ErrMissingUser) {
			t.Errorf("expected ErrMissingUser, got %v", err)
		}
		if called {
			t.Error("next must not be called without a user")
		}
	})

	t.Run("passes_authenticated", func(t *testing.T) {
		h := RequireUser(okHandler)
		resp, err := h(WithUser(context.Background(), "alice"), Request{Path: "/x"})
		if err != nil || resp.Status != 200 || resp.Body != "/x" {
			t.Errorf("got (%+v, %v)", resp, err)
		}
	})
}

// ---------------------------------------------------------------------------
// PART 2 — Deadlines, budgets and retries
// ---------------------------------------------------------------------------

func TestWithBudget(t *testing.T) {
	t.Run("sets_deadline_and_cause", func(t *testing.T) {
		ctx, cancel := WithBudget(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected a deadline")
		}
		expectDone(t, ctx)
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Errorf("Err = %v, want DeadlineExceeded", ctx.Err())
		}
		if !errors.Is(context.Cause(ctx), ErrBudgetExhausted) {
			t.Errorf("Cause = %v, want ErrBudgetExhausted", context.Cause(ctx))
		}
	})

	t.Run("never_extends_parent_deadline", func(t *testing.T) {
		parent, pcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer pcancel()
		pdl, _ := parent.Deadline()
		ctx, cancel := WithBudget(parent, 5*time.Second)
		defer cancel()
		if dl, ok := ctx.Deadline(); !ok || dl.After(pdl) {
			t.Errorf("deadline %v extends parent deadline %v", dl, pdl)
		}
	})

	t.Run("cancel_func_and_values", func(t *testing.T) {
		parent := WithRequestID(context.Background(), "r9")
		ctx, cancel := WithBudget(parent, time.Minute)
		if id, _ := RequestID(ctx); id != "r9" {
			t.Errorf("request ID not inherited, got %q", id)
		}
		cancel()
		expectDone(t, ctx)
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Errorf("Err = %v, want Canceled", ctx.Err())
		}
	})
}

func TestRemaining(t *testing.T) {
	t.Run("no_deadline", func(t *testing.T) {
		if d, ok := Remaining(context.Background()); ok || d != 0 {
			t.Errorf("got (%v, %v), want (0, false)", d, ok)
		}
	})

	t.Run("with_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		d, ok := Remaining(ctx)
		if !ok || d <= 0 || d > 200*time.Millisecond {
			t.Errorf("got (%v, %v), want (0, 200ms], true", d, ok)
		}
	})

	t.Run("expired_is_zero_not_negative", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if d, ok := Remaining(ctx); !ok || d != 0 {
			t.Errorf("got (%v, %v), want (0, true)", d, ok)
		}
	})
}

func TestTimeoutMiddleware(t *testing.T) {
	t.Run("handler_sees_deadline_and_success_passes_through", func(t *testing.T) {
		h := TimeoutMiddleware(time.Second, func(ctx context.Context, req Request) (Response, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("handler context has no deadline")
			}
			return Response{Status: 200, Body: "ok"}, nil
		})
		resp, err := h(context.Background(), Request{})
		if err != nil || resp.Body != "ok" {
			t.Errorf("got (%+v, %v)", resp, err)
		}
	})

	t.Run("budget_exhausted_wraps_both_errors", func(t *testing.T) {
		h := TimeoutMiddleware(30*time.Millisecond, blockingHandler)
		start := time.Now()
		_, err := h(context.Background(), Request{})
		if !errors.Is(err, ErrBudgetExhausted) {
			t.Errorf("expected ErrBudgetExhausted, got %v", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded to stay in the chain, got %v", err)
		}
		if time.Since(start) > time.Second {
			t.Error("middleware did not enforce the budget promptly")
		}
	})

	t.Run("parent_cancel_is_not_a_budget_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		h := TimeoutMiddleware(5*time.Second, blockingHandler)
		_, err := h(ctx, Request{})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected Canceled, got %v", err)
		}
		if errors.Is(err, ErrBudgetExhausted) {
			t.Error("a client cancel must not be reported as budget exhaustion")
		}
	})

	t.Run("unrelated_error_untouched", func(t *testing.T) {
		h := TimeoutMiddleware(time.Second, func(ctx context.Context, req Request) (Response, error) {
			return Response{}, errA
		})
		_, err := h(context.Background(), Request{})
		if !errors.Is(err, errA) || errors.Is(err, ErrBudgetExhausted) {
			t.Errorf("got %v, want plain errA", err)
		}
	})
}

func TestRetry(t *testing.T) {
	t.Run("succeeds_after_failures", func(t *testing.T) {
		var calls int32
		err := Retry(context.Background(), 5, time.Millisecond, func(ctx context.Context) error {
			if atomic.AddInt32(&calls, 1) < 3 {
				return errA
			}
			return nil
		})
		if err != nil || atomic.LoadInt32(&calls) != 3 {
			t.Errorf("err=%v calls=%d, want nil and 3", err, calls)
		}
	})

	t.Run("returns_last_error_when_exhausted", func(t *testing.T) {
		var calls int32
		err := Retry(context.Background(), 3, time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if !errors.Is(err, errA) || atomic.LoadInt32(&calls) != 3 {
			t.Errorf("err=%v calls=%d, want errA and 3", err, calls)
		}
	})

	t.Run("attempts_below_one_means_one", func(t *testing.T) {
		var calls int32
		Retry(context.Background(), 0, time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if atomic.LoadInt32(&calls) != 1 {
			t.Errorf("calls = %d, want 1", calls)
		}
	})

	t.Run("no_sleep_after_last_attempt", func(t *testing.T) {
		start := time.Now()
		err := Retry(context.Background(), 1, 5*time.Second, func(ctx context.Context) error { return errA })
		if !errors.Is(err, errA) {
			t.Errorf("got %v, want errA", err)
		}
		if time.Since(start) > time.Second {
			t.Error("slept after the final attempt")
		}
	})

	t.Run("already_cancelled_never_calls_fn", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls int32
		err := Retry(ctx, 3, time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
		if !errors.Is(err, context.Canceled) || atomic.LoadInt32(&calls) != 0 {
			t.Errorf("err=%v calls=%d, want Canceled and 0", err, calls)
		}
	})

	t.Run("cancel_interrupts_backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls int32
		go func() { time.Sleep(30 * time.Millisecond); cancel() }()
		start := time.Now()
		err := Retry(ctx, 10, 5*time.Second, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errA
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
		if time.Since(start) > time.Second || atomic.LoadInt32(&calls) != 1 {
			t.Errorf("elapsed=%v calls=%d, want prompt return after 1 call", time.Since(start), calls)
		}
	})
}

// ---------------------------------------------------------------------------
// PART 3 — First-success fan-out and detached cleanup
// ---------------------------------------------------------------------------

func TestFirstSuccess(t *testing.T) {
	t.Run("returns_winner_and_cancels_losers", func(t *testing.T) {
		var sawCancel int32
		slow := func(ctx context.Context) (string, error) {
			<-ctx.Done()
			atomic.StoreInt32(&sawCancel, 1)
			return "", ctx.Err()
		}
		fast := func(ctx context.Context) (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "fast", nil
		}
		got, err := FirstSuccess(context.Background(), []func(context.Context) (string, error){slow, fast, slow})
		if err != nil || got != "fast" {
			t.Fatalf("got (%q, %v), want (fast, nil)", got, err)
		}
		if atomic.LoadInt32(&sawCancel) != 1 {
			t.Error("losing calls must be cancelled AND waited for before FirstSuccess returns")
		}
	})

	t.Run("failure_does_not_cancel_others", func(t *testing.T) {
		fail := func(ctx context.Context) (string, error) { return "", errA }
		later := func(ctx context.Context) (string, error) {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(20 * time.Millisecond):
				return "ok", nil
			}
		}
		got, err := FirstSuccess(context.Background(), []func(context.Context) (string, error){fail, later})
		if err != nil || got != "ok" {
			t.Errorf("got (%q, %v), want (ok, nil)", got, err)
		}
	})

	t.Run("all_fail", func(t *testing.T) {
		calls := []func(context.Context) (string, error){
			func(ctx context.Context) (string, error) { return "", errA },
			func(ctx context.Context) (string, error) { return "", errB },
		}
		_, err := FirstSuccess(context.Background(), calls)
		if !errors.Is(err, ErrAllFailed) || !errors.Is(err, errA) || !errors.Is(err, errB) {
			t.Errorf("got %v, want ErrAllFailed wrapping errA and errB", err)
		}
	})

	t.Run("no_calls", func(t *testing.T) {
		if _, err := FirstSuccess(context.Background(), nil); !errors.Is(err, ErrAllFailed) {
			t.Errorf("got %v, want ErrAllFailed", err)
		}
	})

	t.Run("parent_cancel_returns_ctx_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		block := func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		}
		start := time.Now()
		_, err := FirstSuccess(ctx, []func(context.Context) (string, error){block, block})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want Canceled", err)
		}
		if time.Since(start) > time.Second {
			t.Error("FirstSuccess did not return promptly after cancel")
		}
	})

	t.Run("calls_inherit_context_values", func(t *testing.T) {
		ctx := WithRequestID(context.Background(), "r-77")
		call := func(ctx context.Context) (string, error) {
			id, _ := RequestID(ctx)
			return id, nil
		}
		if got, _ := FirstSuccess(ctx, []func(context.Context) (string, error){call}); got != "r-77" {
			t.Errorf("got %q, want r-77", got)
		}
	})
}

func TestRunAudit(t *testing.T) {
	t.Run("runs_after_parent_cancelled_and_keeps_values", func(t *testing.T) {
		parent, cancel := context.WithCancel(WithRequestID(context.Background(), "r-1"))
		cancel()
		var gotID string
		var ctxErr error
		var remaining time.Duration
		var hasDeadline bool
		err := RunAudit(parent, 500*time.Millisecond, func(ctx context.Context) error {
			gotID, _ = RequestID(ctx)
			ctxErr = ctx.Err()
			remaining, hasDeadline = Remaining(ctx)
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ctxErr != nil {
			t.Errorf("audit ctx must not inherit the parent's cancellation, Err = %v", ctxErr)
		}
		if gotID != "r-1" {
			t.Errorf("audit ctx lost request ID, got %q", gotID)
		}
		if !hasDeadline || remaining <= 0 || remaining > 500*time.Millisecond {
			t.Errorf("audit ctx needs its own timeout, got (%v, %v)", remaining, hasDeadline)
		}
	})

	t.Run("audit_is_bounded_by_its_own_timeout", func(t *testing.T) {
		err := RunAudit(context.Background(), 30*time.Millisecond, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
	})

	t.Run("propagates_audit_error", func(t *testing.T) {
		err := RunAudit(context.Background(), time.Second, func(ctx context.Context) error { return errA })
		if !errors.Is(err, errA) {
			t.Errorf("got %v, want errA", err)
		}
	})
}
