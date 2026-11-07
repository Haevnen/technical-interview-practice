package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrPoolClosed = errors.New("pool closed")
var ErrQueueFull = errors.New("queue full")
var ErrPanic = errors.New("handler panicked")

type Job struct {
	ID      string
	Payload int
}

type Result struct {
	JobID    string
	Value    int
	Err      error
	Attempts int
}

type Handler func(ctx context.Context, job Job) (int, error)

type Config struct {
	Workers     int
	QueueSize   int
	MaxAttempts int
}

type Pool struct {
	handler     Handler
	maxAttempts int
	jobs        chan Job
	results     chan Result
	closing     chan struct{}
	done        chan struct{}
	spaceChange chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func NewPool(cfg Config, h Handler) *Pool {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		handler:     h,
		maxAttempts: cfg.MaxAttempts,
		jobs:        make(chan Job, cfg.QueueSize),
		results:     make(chan Result),
		closing:     make(chan struct{}),
		done:        make(chan struct{}),
		spaceChange: make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.worker()
	}
	go func() {
		p.wg.Wait()
		close(p.results)
		close(p.done)
	}()

	return p
}

func (p *Pool) Submit(job Job) error {
	return p.SubmitCtx(context.Background(), job)
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) Close() {
	p.beginClose()
	<-p.done
}

func (p *Pool) TrySubmit(job Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *Pool) SubmitCtx(ctx context.Context, job Job) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrPoolClosed
		}

		select {
		case p.jobs <- job:
			p.mu.Unlock()
			return nil
		default:
			spaceChange := p.spaceChange
			p.mu.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.closing:
				return ErrPoolClosed
			case <-spaceChange:
			}
		}
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	p.beginClose()

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		select {
		case <-p.done:
			return nil
		default:
		}
		p.cancel()
		<-p.done
		return ctx.Err()
	}
}

func (p *Pool) beginClose() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	close(p.closing)
	close(p.jobs)
}

func (p *Pool) worker() {
	defer p.wg.Done()

	for job := range p.jobs {
		p.mu.Lock()
		close(p.spaceChange)
		p.spaceChange = make(chan struct{})
		p.mu.Unlock()

		p.results <- p.process(job)
	}
}

func (p *Pool) process(job Job) Result {
	result := Result{JobID: job.ID}
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		result.Attempts = attempt
		value, err, panicked := p.invoke(job)
		if panicked {
			result.Err = err
			return result
		}
		if err == nil {
			result.Value = value
			result.Err = nil
			return result
		}
		result.Err = err
	}
	return result
}

func (p *Pool) invoke(job Job) (value int, err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = 0
			err = fmt.Errorf("%w: %v", ErrPanic, recovered)
			panicked = true
		}
	}()

	value, err = p.handler(p.ctx, job)
	return value, err, false
}
