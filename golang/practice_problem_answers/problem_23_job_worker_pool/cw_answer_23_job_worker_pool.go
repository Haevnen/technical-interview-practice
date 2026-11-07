// =============================================================================
// INTERVIEW PROBLEM 23: Concurrent Job Worker Pool
// Difficulty: Senior Software Engineer | Estimated time: 45 min
// Concurrency patterns: worker pool, bounded queue (backpressure), context
// cancellation, panic recovery, graceful shutdown
// =============================================================================
//
// CONTEXT
// -------
// You're building the background-job runner for a developer-tools platform
// (think: CI runners, webhook delivery, report generation). Callers submit
// jobs; a fixed number of worker goroutines execute them with a user-supplied
// Handler, and the outcome of every job is published on a results channel.
//
// The pool must be safe for concurrent use by many goroutines and must not
// leak goroutines. Run your tests with the race detector:
//
//	./run_tests.sh -f golang/practice_problem_answers/cw_answer_23_job_worker_pool.go -c go test -race -v .
//
// DATA MODEL
// ----------
// Job     — ID string, Payload int
// Result  — JobID string, Value int, Err error, Attempts int
// Handler — func(ctx context.Context, job Job) (int, error)
// Config  — Workers int, QueueSize int, MaxAttempts int
//
// EXAMPLE
// -------
//	p := NewPool(Config{Workers: 4, QueueSize: 16}, handler)
//	go func() {
//	    for r := range p.Results() { fmt.Println(r.JobID, r.Value, r.Err) }
//	}()
//	_ = p.Submit(Job{ID: "a", Payload: 1})
//	p.Close() // waits for queued + running jobs, then closes Results()
// =============================================================================

package workerpool

import (
	"context"
	"errors"
)

// ErrPoolClosed is returned when submitting to a pool that is closed or shutting down.
var ErrPoolClosed = errors.New("pool closed")

// ErrQueueFull is returned by TrySubmit when the queue has no free slot.
var ErrQueueFull = errors.New("queue full")

// ErrPanic is wrapped into Result.Err when a Handler panics.
var ErrPanic = errors.New("handler panicked")

// Job is a unit of work.
type Job struct {
	ID      string
	Payload int
}

// Result is the outcome of one job. Exactly one Result is published per job.
type Result struct {
	JobID    string
	Value    int   // Handler's return value when Err == nil
	Err      error // final error after all attempts; nil on success
	Attempts int   // how many times the Handler was invoked for this job
}

// Handler executes a job. It should return promptly when ctx is cancelled.
type Handler func(ctx context.Context, job Job) (int, error)

// Config configures a Pool.
type Config struct {
	Workers     int // number of worker goroutines (>= 1)
	QueueSize   int // capacity of the pending-job queue (>= 0)
	MaxAttempts int // total attempts per job; values < 1 are treated as 1 (no retry)
}

// Pool runs jobs on a fixed set of worker goroutines.
type Pool struct {
	queue        []Job
	queueSize    int
	headIdx      int
	numOfWorkers int
	maxAttempts  int
	handler      Handler
	result       chan Result
}

// NewPool creates a pool and starts cfg.Workers worker goroutines immediately.
func NewPool(cfg Config, h Handler) *Pool {
	return &Pool{
		queue:        make([]Job, 0),
		queueSize:    cfg.QueueSize,
		headIdx:      -1,
		numOfWorkers: cfg.Workers,
		maxAttempts:  cfg.MaxAttempts,
		handler:      h,
		result:       make(chan Result, cfg.Workers),
	}
}

// ---------------------------------------------------------------------------
// PART 1 — Worker pool basics  (~15 min)
// ---------------------------------------------------------------------------

// Submit enqueues a job, blocking while the queue is full.
// Returns ErrPoolClosed if the pool has been closed.
func (p *Pool) Submit(job Job) error {
	if p.headIdx == -1 {
		p.queue = append(p.queue, job)
		p.headIdx = 0
		return nil
	}

	if (len(p.queue) - p.headIdx) < p.queueSize {
		p.queue = append(p.queue, job)
		return nil
	}

	return ErrPoolClosed
}

// Results returns the channel on which one Result per job is published.
// Workers block if nobody is reading it. The channel is closed once the pool
// has been closed AND every accepted job has been processed.
func (p *Pool) Results() <-chan Result {
	return p.result
}

// Close stops accepting new jobs, lets queued and running jobs finish, waits
// for all workers to exit, then closes the Results channel.
// It must be safe to call Close more than once and from multiple goroutines.
func (p *Pool) Close() {

}

// ---------------------------------------------------------------------------
// PART 2 — Backpressure  (~15 min)
// ---------------------------------------------------------------------------

// TrySubmit enqueues a job without blocking.
// Returns ErrQueueFull if the queue has no free slot, ErrPoolClosed if closed.
func (p *Pool) TrySubmit(job Job) error {
	return nil
}

// SubmitCtx enqueues a job, blocking until there is room, ctx is done, or the
// pool is closed. Returns ctx.Err() if ctx finishes first, ErrPoolClosed if
// the pool is closed. Submit(job) behaves like SubmitCtx(context.Background(), job).
func (p *Pool) SubmitCtx(ctx context.Context, job Job) error {
	return nil
}

// ---------------------------------------------------------------------------
// PART 3 — Retries, panics, and graceful shutdown  (~15 min)
// ---------------------------------------------------------------------------
//
// Retries: when Handler returns an error and attempts < cfg.MaxAttempts, run it
// again immediately on the same worker. Result.Attempts is the number of calls.
// Result.Err is the error from the last attempt.
//
// Panics: a panicking Handler must not crash the pool. Recover and publish a
// Result whose Err wraps ErrPanic (errors.Is(err, ErrPanic) is true). A panic is
// NOT retried (Attempts == 1 for that job). The worker keeps serving other jobs.

// Shutdown is like Close, but bounded by ctx. It stops accepting jobs and waits
// for queued and running jobs to finish. If ctx is done first it cancels the
// context passed to in-flight Handlers, waits for the workers to exit, closes
// Results, and returns ctx.Err(). Returns nil if everything drained in time.
// Handler contexts must be cancelled only by a Shutdown deadline, not by Close.
func (p *Pool) Shutdown(ctx context.Context) error {
	return nil
}
