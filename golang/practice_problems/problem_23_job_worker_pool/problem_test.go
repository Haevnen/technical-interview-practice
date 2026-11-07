// Tests for Problem 23: Concurrent Job Worker Pool
//
// Run the stub (expect all panics):
//
//	cd golang && go test ./practice_problems/problem_23_job_worker_pool/ -v
//
// Run against your answer via run_tests.sh (use -race!):
//
//	./run_tests.sh \
//	  -f golang/practice_problem_answers/cw_answer_23_job_worker_pool.go \
//	  -c go test -race -v .
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errFlaky = errors.New("flaky")

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func echo(ctx context.Context, j Job) (int, error) { return j.Payload * 2, nil }

// drain collects every Result until the Results channel is closed.
func drain(p *Pool) <-chan []Result {
	done := make(chan []Result, 1)
	go func() {
		var out []Result
		for r := range p.Results() {
			out = append(out, r)
		}
		done <- out
	}()
	return done
}

func waitResults(t *testing.T, done <-chan []Result) []Result {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Results channel to close")
		return nil
	}
}

// fullPool returns a 1-worker, queue-2 pool whose handler blocks until release()
// is called, with the worker busy and the queue completely full.
func fullPool(t *testing.T) (*Pool, func()) {
	t.Helper()
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	h := func(ctx context.Context, j Job) (int, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
		return 0, nil
	}
	p := NewPool(Config{Workers: 1, QueueSize: 2}, h)
	if err := p.Submit(Job{ID: "running"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never started the first job")
	}
	for _, id := range []string{"q1", "q2"} {
		if err := p.TrySubmit(Job{ID: id}); err != nil {
			t.Fatalf("TrySubmit(%s): %v", id, err)
		}
	}
	var once sync.Once
	return p, func() { once.Do(func() { close(gate) }) }
}

// ---------------------------------------------------------------------------
// PART 1 — Worker pool basics
// ---------------------------------------------------------------------------

func TestPoolBasics(t *testing.T) {
	t.Run("processes_all_jobs", func(t *testing.T) {
		p := NewPool(Config{Workers: 3, QueueSize: 5}, echo)
		done := drain(p)
		for i := 0; i < 20; i++ {
			if err := p.Submit(Job{ID: fmt.Sprintf("j%d", i), Payload: i}); err != nil {
				t.Fatalf("Submit: %v", err)
			}
		}
		p.Close()
		res := waitResults(t, done)
		if len(res) != 20 {
			t.Fatalf("got %d results, want 20", len(res))
		}
		seen := map[string]int{}
		for _, r := range res {
			if r.Err != nil {
				t.Errorf("job %s: unexpected error %v", r.JobID, r.Err)
			}
			if r.Attempts != 1 {
				t.Errorf("job %s: Attempts = %d, want 1", r.JobID, r.Attempts)
			}
			seen[r.JobID] = r.Value
		}
		for i := 0; i < 20; i++ {
			if seen[fmt.Sprintf("j%d", i)] != i*2 {
				t.Errorf("job j%d: value %d, want %d", i, seen[fmt.Sprintf("j%d", i)], i*2)
			}
		}
	})

	t.Run("concurrency_is_bounded_by_workers", func(t *testing.T) {
		var cur, max int32
		h := func(ctx context.Context, j Job) (int, error) {
			n := atomic.AddInt32(&cur, 1)
			for {
				m := atomic.LoadInt32(&max)
				if n <= m || atomic.CompareAndSwapInt32(&max, m, n) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			return 0, nil
		}
		p := NewPool(Config{Workers: 3, QueueSize: 20}, h)
		done := drain(p)
		for i := 0; i < 12; i++ {
			p.Submit(Job{ID: fmt.Sprintf("j%d", i)})
		}
		p.Close()
		waitResults(t, done)
		if got := atomic.LoadInt32(&max); got > 3 {
			t.Errorf("max concurrent handlers = %d, want <= 3", got)
		} else if got < 2 {
			t.Errorf("max concurrent handlers = %d, workers are not running in parallel", got)
		}
	})

	t.Run("submit_after_close_returns_error", func(t *testing.T) {
		p := NewPool(Config{Workers: 1, QueueSize: 1}, echo)
		done := drain(p)
		p.Close()
		waitResults(t, done)
		if err := p.Submit(Job{ID: "late"}); !errors.Is(err, ErrPoolClosed) {
			t.Errorf("expected ErrPoolClosed, got %v", err)
		}
	})

	t.Run("close_is_idempotent_and_concurrent_safe", func(t *testing.T) {
		p := NewPool(Config{Workers: 2, QueueSize: 2}, echo)
		done := drain(p)
		p.Submit(Job{ID: "a", Payload: 1})
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); p.Close() }()
		}
		wg.Wait()
		p.Close()
		if res := waitResults(t, done); len(res) != 1 {
			t.Errorf("got %d results, want 1", len(res))
		}
	})

	t.Run("concurrent_submitters", func(t *testing.T) {
		p := NewPool(Config{Workers: 4, QueueSize: 3}, echo)
		done := drain(p)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					p.Submit(Job{ID: fmt.Sprintf("g%d-%d", g, i), Payload: i})
				}
			}(g)
		}
		wg.Wait()
		p.Close()
		if res := waitResults(t, done); len(res) != 200 {
			t.Errorf("got %d results, want 200", len(res))
		}
	})
}

// ---------------------------------------------------------------------------
// PART 2 — Backpressure
// ---------------------------------------------------------------------------

func TestTrySubmit(t *testing.T) {
	t.Run("returns_queue_full", func(t *testing.T) {
		p, release := fullPool(t)
		defer release()
		if err := p.TrySubmit(Job{ID: "overflow"}); !errors.Is(err, ErrQueueFull) {
			t.Errorf("expected ErrQueueFull, got %v", err)
		}
		done := drain(p)
		release()
		p.Close()
		if res := waitResults(t, done); len(res) != 3 {
			t.Errorf("got %d results, want 3 (overflow job must not run)", len(res))
		}
	})

	t.Run("returns_pool_closed", func(t *testing.T) {
		p := NewPool(Config{Workers: 1, QueueSize: 1}, echo)
		done := drain(p)
		p.Close()
		waitResults(t, done)
		if err := p.TrySubmit(Job{ID: "late"}); !errors.Is(err, ErrPoolClosed) {
			t.Errorf("expected ErrPoolClosed, got %v", err)
		}
	})
}

func TestSubmitCtx(t *testing.T) {
	t.Run("respects_context_deadline", func(t *testing.T) {
		p, release := fullPool(t)
		defer release()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := p.SubmitCtx(ctx, Job{ID: "blocked"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		if time.Since(start) > time.Second {
			t.Error("SubmitCtx did not return promptly after ctx expired")
		}
		done := drain(p)
		release()
		p.Close()
		waitResults(t, done)
	})

	t.Run("unblocks_when_space_frees", func(t *testing.T) {
		p, release := fullPool(t)
		errCh := make(chan error, 1)
		go func() { errCh <- p.SubmitCtx(context.Background(), Job{ID: "waiting"}) }()
		select {
		case err := <-errCh:
			t.Fatalf("SubmitCtx returned %v while the queue was full", err)
		case <-time.After(50 * time.Millisecond):
		}
		done := drain(p)
		release()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("SubmitCtx: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("SubmitCtx never unblocked")
		}
		p.Close()
		if res := waitResults(t, done); len(res) != 4 {
			t.Errorf("got %d results, want 4", len(res))
		}
	})

	t.Run("blocked_submit_returns_pool_closed_on_close", func(t *testing.T) {
		p, release := fullPool(t)
		errCh := make(chan error, 1)
		go func() { errCh <- p.SubmitCtx(context.Background(), Job{ID: "waiting"}) }()
		time.Sleep(30 * time.Millisecond)
		done := drain(p)
		go p.Close()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, ErrPoolClosed) {
				t.Errorf("expected nil or ErrPoolClosed, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("blocked SubmitCtx never returned after Close")
		}
		release()
		waitResults(t, done)
	})
}

// ---------------------------------------------------------------------------
// PART 3 — Retries, panics, and graceful shutdown
// ---------------------------------------------------------------------------

func TestRetries(t *testing.T) {
	t.Run("retries_until_success", func(t *testing.T) {
		var mu sync.Mutex
		calls := map[string]int{}
		h := func(ctx context.Context, j Job) (int, error) {
			mu.Lock()
			calls[j.ID]++
			n := calls[j.ID]
			mu.Unlock()
			if n < 3 {
				return 0, errFlaky
			}
			return 42, nil
		}
		p := NewPool(Config{Workers: 2, QueueSize: 4, MaxAttempts: 3}, h)
		done := drain(p)
		p.Submit(Job{ID: "a"})
		p.Close()
		res := waitResults(t, done)
		if len(res) != 1 || res[0].Err != nil || res[0].Value != 42 || res[0].Attempts != 3 {
			t.Errorf("unexpected result: %+v", res)
		}
	})

	t.Run("gives_up_after_max_attempts", func(t *testing.T) {
		var calls int32
		h := func(ctx context.Context, j Job) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 0, errFlaky
		}
		p := NewPool(Config{Workers: 1, QueueSize: 1, MaxAttempts: 3}, h)
		done := drain(p)
		p.Submit(Job{ID: "a"})
		p.Close()
		res := waitResults(t, done)
		if len(res) != 1 || !errors.Is(res[0].Err, errFlaky) || res[0].Attempts != 3 {
			t.Errorf("unexpected result: %+v", res)
		}
		if got := atomic.LoadInt32(&calls); got != 3 {
			t.Errorf("handler called %d times, want 3", got)
		}
	})

	t.Run("no_retry_by_default", func(t *testing.T) {
		var calls int32
		h := func(ctx context.Context, j Job) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 0, errFlaky
		}
		p := NewPool(Config{Workers: 1, QueueSize: 1}, h)
		done := drain(p)
		p.Submit(Job{ID: "a"})
		p.Close()
		res := waitResults(t, done)
		if len(res) != 1 || res[0].Attempts != 1 || atomic.LoadInt32(&calls) != 1 {
			t.Errorf("unexpected result: %+v (calls=%d)", res, atomic.LoadInt32(&calls))
		}
	})
}

func TestPanicRecovery(t *testing.T) {
	h := func(ctx context.Context, j Job) (int, error) {
		if j.ID == "boom" {
			panic("kaboom")
		}
		return j.Payload, nil
	}
	p := NewPool(Config{Workers: 1, QueueSize: 4, MaxAttempts: 3}, h)
	done := drain(p)
	p.Submit(Job{ID: "boom"})
	p.Submit(Job{ID: "ok", Payload: 7})
	p.Close()
	res := waitResults(t, done)
	if len(res) != 2 {
		t.Fatalf("got %d results, want 2", len(res))
	}
	for _, r := range res {
		switch r.JobID {
		case "boom":
			if !errors.Is(r.Err, ErrPanic) || r.Attempts != 1 {
				t.Errorf("panic job: %+v, want ErrPanic and Attempts=1", r)
			}
		case "ok":
			if r.Err != nil || r.Value != 7 {
				t.Errorf("ok job: %+v", r)
			}
		}
	}
}

func TestShutdown(t *testing.T) {
	t.Run("graceful_drain_returns_nil", func(t *testing.T) {
		p := NewPool(Config{Workers: 2, QueueSize: 8}, echo)
		done := drain(p)
		for i := 0; i < 8; i++ {
			p.Submit(Job{ID: fmt.Sprintf("j%d", i), Payload: i})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		if res := waitResults(t, done); len(res) != 8 {
			t.Errorf("got %d results, want 8", len(res))
		}
		if err := p.Submit(Job{ID: "late"}); !errors.Is(err, ErrPoolClosed) {
			t.Errorf("expected ErrPoolClosed after Shutdown, got %v", err)
		}
	})

	t.Run("deadline_cancels_inflight_handlers", func(t *testing.T) {
		started := make(chan struct{})
		h := func(ctx context.Context, j Job) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		p := NewPool(Config{Workers: 1, QueueSize: 1}, h)
		done := drain(p)
		p.Submit(Job{ID: "stuck"})
		<-started
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := p.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		res := waitResults(t, done)
		if len(res) != 1 || res[0].Err == nil {
			t.Errorf("expected one failed result, got %+v", res)
		}
	})

	t.Run("close_does_not_cancel_handlers", func(t *testing.T) {
		h := func(ctx context.Context, j Job) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(60 * time.Millisecond):
				return 1, nil
			}
		}
		p := NewPool(Config{Workers: 1, QueueSize: 1}, h)
		done := drain(p)
		p.Submit(Job{ID: "slow"})
		p.Close()
		res := waitResults(t, done)
		if len(res) != 1 || res[0].Err != nil || res[0].Value != 1 {
			t.Errorf("Close must let in-flight jobs finish, got %+v", res)
		}
	})
}
