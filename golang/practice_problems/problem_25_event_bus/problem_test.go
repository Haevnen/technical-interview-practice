// Tests for Problem 25: Concurrent Pub/Sub Event Bus
//
// Run the stub (expect all panics):
//
//	cd golang && go test ./practice_problems/problem_25_event_bus/ -v
//
// Run against your answer via run_tests.sh (use -race!):
//
//	./run_tests.sh \
//	  -f golang/practice_problem_answers/cw_answer_25_event_bus.go \
//	  -c go test -race -v .
package eventbus

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func mustSub(t *testing.T, b *Bus, topic string, buffer int) *Subscription {
	t.Helper()
	s, err := b.Subscribe(topic, buffer)
	if err != nil {
		t.Fatalf("Subscribe(%q): %v", topic, err)
	}
	return s
}

func mustSubPolicy(t *testing.T, b *Bus, topic string, buffer int, p Policy) *Subscription {
	t.Helper()
	s, err := b.SubscribeWithPolicy(topic, buffer, p)
	if err != nil {
		t.Fatalf("SubscribeWithPolicy(%q): %v", topic, err)
	}
	return s
}

func mustPublish(t *testing.T, b *Bus, topic string, payload int) int {
	t.Helper()
	n, err := b.Publish(topic, payload)
	if err != nil {
		t.Fatalf("Publish(%q, %d): %v", topic, payload, err)
	}
	return n
}

func recv(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

func expectNone(t *testing.T, ch <-chan Event) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("unexpected event %+v", ev)
		}
		t.Fatal("channel closed unexpectedly")
	case <-time.After(30 * time.Millisecond):
	}
}

// expectClosed drains ch until it is closed, failing if that takes too long.
func expectClosed(t *testing.T, ch <-chan Event) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel was not closed")
		}
	}
}

type pubResult struct {
	n   int
	err error
}

func publishAsync(b *Bus, topic string, payload int) <-chan pubResult {
	ch := make(chan pubResult, 1)
	go func() {
		n, err := b.Publish(topic, payload)
		ch <- pubResult{n, err}
	}()
	return ch
}

func waitPub(t *testing.T, ch <-chan pubResult) pubResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return (blocked or deadlocked)")
		return pubResult{}
	}
}

func expectPending(t *testing.T, ch <-chan pubResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("Publish returned early: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// PART 1 — Topics, publish, unsubscribe
// ---------------------------------------------------------------------------

func TestPublish(t *testing.T) {
	t.Run("fans_out_to_topic_subscribers_only", func(t *testing.T) {
		b := NewBus()
		a := mustSub(t, b, "x", 4)
		c := mustSub(t, b, "x", 4)
		other := mustSub(t, b, "y", 4)
		if n := mustPublish(t, b, "x", 1); n != 2 {
			t.Errorf("delivered = %d, want 2", n)
		}
		for _, s := range []*Subscription{a, c} {
			if ev := recv(t, s.C()); ev.Topic != "x" || ev.Payload != 1 {
				t.Errorf("got %+v", ev)
			}
		}
		expectNone(t, other.C())
	})

	t.Run("no_subscribers_returns_zero", func(t *testing.T) {
		b := NewBus()
		if n, err := b.Publish("nobody", 1); n != 0 || err != nil {
			t.Errorf("got (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("preserves_publish_order", func(t *testing.T) {
		b := NewBus()
		s := mustSub(t, b, "t", 10)
		for i := 1; i <= 5; i++ {
			mustPublish(t, b, "t", i)
		}
		for i := 1; i <= 5; i++ {
			if ev := recv(t, s.C()); ev.Payload != i {
				t.Fatalf("event %d: got payload %d", i, ev.Payload)
			}
		}
	})
}

func TestUnsubscribe(t *testing.T) {
	t.Run("closes_channel_and_stops_delivery", func(t *testing.T) {
		b := NewBus()
		s := mustSub(t, b, "t", 4)
		s.Unsubscribe()
		expectClosed(t, s.C())
		if n := mustPublish(t, b, "t", 1); n != 0 {
			t.Errorf("delivered = %d after Unsubscribe, want 0", n)
		}
	})

	t.Run("is_idempotent", func(t *testing.T) {
		b := NewBus()
		s := mustSub(t, b, "t", 1)
		s.Unsubscribe()
		s.Unsubscribe() // must not panic
	})

	t.Run("other_subscribers_unaffected", func(t *testing.T) {
		b := NewBus()
		gone := mustSub(t, b, "t", 2)
		stays := mustSub(t, b, "t", 2)
		gone.Unsubscribe()
		if n := mustPublish(t, b, "t", 9); n != 1 {
			t.Errorf("delivered = %d, want 1", n)
		}
		if ev := recv(t, stays.C()); ev.Payload != 9 {
			t.Errorf("got %+v", ev)
		}
	})
}

// ---------------------------------------------------------------------------
// PART 2 — Slow-consumer policies
// ---------------------------------------------------------------------------

func TestDropNewest(t *testing.T) {
	b := NewBus()
	s := mustSubPolicy(t, b, "t", 2, DropNewest)
	total := 0
	for i := 1; i <= 5; i++ {
		total += mustPublish(t, b, "t", i)
	}
	if total != 2 {
		t.Errorf("total delivered = %d, want 2", total)
	}
	if d := s.Dropped(); d != 3 {
		t.Errorf("Dropped = %d, want 3", d)
	}
	for _, want := range []int{1, 2} {
		if ev := recv(t, s.C()); ev.Payload != want {
			t.Errorf("got payload %d, want %d", ev.Payload, want)
		}
	}
	expectNone(t, s.C())
}

func TestDropOldest(t *testing.T) {
	b := NewBus()
	s := mustSubPolicy(t, b, "t", 2, DropOldest)
	for i := 1; i <= 5; i++ {
		if n := mustPublish(t, b, "t", i); n != 1 {
			t.Errorf("publish %d delivered = %d, want 1", i, n)
		}
	}
	if d := s.Dropped(); d != 3 {
		t.Errorf("Dropped = %d, want 3", d)
	}
	for _, want := range []int{4, 5} {
		if ev := recv(t, s.C()); ev.Payload != want {
			t.Errorf("got payload %d, want %d", ev.Payload, want)
		}
	}
	expectNone(t, s.C())
}

func TestBlockPolicy(t *testing.T) {
	t.Run("publisher_waits_for_room", func(t *testing.T) {
		b := NewBus()
		s := mustSub(t, b, "t", 1)
		mustPublish(t, b, "t", 1) // fills the buffer
		pend := publishAsync(b, "t", 2)
		expectPending(t, pend)
		if ev := recv(t, s.C()); ev.Payload != 1 {
			t.Errorf("got %+v, want payload 1", ev)
		}
		if r := waitPub(t, pend); r.err != nil || r.n != 1 {
			t.Errorf("blocked Publish returned %+v, want (1, nil)", r)
		}
		if ev := recv(t, s.C()); ev.Payload != 2 {
			t.Errorf("got %+v, want payload 2", ev)
		}
		if d := s.Dropped(); d != 0 {
			t.Errorf("Dropped = %d for Block policy, want 0", d)
		}
	})

	t.Run("slow_dropper_does_not_stall_fast_subscriber", func(t *testing.T) {
		b := NewBus()
		mustSubPolicy(t, b, "t", 1, DropNewest) // never read
		fast := mustSub(t, b, "t", 100)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 20; i++ {
				b.Publish("t", i)
			}
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("publisher stalled behind a DropNewest subscriber")
		}
		for i := 0; i < 20; i++ {
			if ev := recv(t, fast.C()); ev.Payload != i {
				t.Fatalf("fast subscriber event %d: got payload %d", i, ev.Payload)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// PART 3 — Safe shutdown and concurrent churn
// ---------------------------------------------------------------------------

func TestUnsubscribeWhilePublishing(t *testing.T) {
	b := NewBus()
	s := mustSub(t, b, "t", 1)
	mustPublish(t, b, "t", 1) // buffer full
	pend := publishAsync(b, "t", 2)
	expectPending(t, pend)

	s.Unsubscribe() // must release the blocked publisher, not deadlock or panic

	r := waitPub(t, pend)
	if r.err != nil {
		t.Errorf("Publish err = %v, want nil", r.err)
	}
	if r.n != 0 {
		t.Errorf("delivered = %d, want 0 (subscriber left before delivery)", r.n)
	}
	expectClosed(t, s.C())
}

func TestSubscribeNotBlockedBySlowPublish(t *testing.T) {
	b := NewBus()
	s := mustSub(t, b, "t", 1)
	mustPublish(t, b, "t", 1)
	pend := publishAsync(b, "t", 2) // blocked on s
	expectPending(t, pend)

	done := make(chan struct{})
	go func() {
		defer close(done)
		other, err := b.Subscribe("t", 1)
		if err == nil {
			other.Unsubscribe()
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe/Unsubscribe blocked while a Publish was stuck on a slow subscriber")
	}
	recv(t, s.C())
	waitPub(t, pend)
}

func TestClose(t *testing.T) {
	t.Run("closes_channels_and_rejects_new_work", func(t *testing.T) {
		b := NewBus()
		a := mustSub(t, b, "x", 2)
		c := mustSubPolicy(t, b, "y", 2, DropOldest)
		b.Close()
		expectClosed(t, a.C())
		expectClosed(t, c.C())
		if _, err := b.Publish("x", 1); !errors.Is(err, ErrBusClosed) {
			t.Errorf("Publish after Close: got %v, want ErrBusClosed", err)
		}
		if _, err := b.Subscribe("x", 1); !errors.Is(err, ErrBusClosed) {
			t.Errorf("Subscribe after Close: got %v, want ErrBusClosed", err)
		}
		a.Unsubscribe() // must still be safe after Close
	})

	t.Run("idempotent_and_concurrent", func(t *testing.T) {
		b := NewBus()
		mustSub(t, b, "x", 1)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); b.Close() }()
		}
		wg.Wait()
		b.Close()
	})

	t.Run("releases_blocked_publisher", func(t *testing.T) {
		b := NewBus()
		s := mustSub(t, b, "t", 1)
		mustPublish(t, b, "t", 1)
		pend := publishAsync(b, "t", 2)
		expectPending(t, pend)

		closed := make(chan struct{})
		go func() { b.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("Close deadlocked behind a blocked publisher")
		}
		r := waitPub(t, pend)
		if r.err != nil && !errors.Is(r.err, ErrBusClosed) {
			t.Errorf("Publish err = %v, want nil or ErrBusClosed", r.err)
		}
		expectClosed(t, s.C())
	})
}

func TestConcurrentChurn(t *testing.T) {
	b := NewBus()

	// A well-behaved Block subscriber that drains until the bus closes.
	blocker := mustSub(t, b, "t", 2)
	drained := make(chan struct{})
	go func() {
		for range blocker.C() {
		}
		close(drained)
	}()

	var pubs, subs sync.WaitGroup
	for p := 0; p < 4; p++ {
		pubs.Add(1)
		go func() {
			defer pubs.Done()
			for i := 0; i < 200; i++ {
				if _, err := b.Publish("t", i); err != nil {
					return
				}
			}
		}()
	}
	for s := 0; s < 4; s++ {
		subs.Add(1)
		go func() {
			defer subs.Done()
			for i := 0; i < 50; i++ {
				policy := DropNewest
				if i%2 == 0 {
					policy = DropOldest
				}
				sub, err := b.SubscribeWithPolicy("t", 4, policy)
				if err != nil {
					return
				}
				for j := 0; j < 3; j++ {
					select {
					case <-sub.C():
					case <-time.After(time.Millisecond):
					}
				}
				sub.Unsubscribe()
			}
		}()
	}

	done := make(chan struct{})
	go func() { pubs.Wait(); subs.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock under concurrent publish/subscribe/unsubscribe")
	}

	b.Close()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber channel not closed by Close")
	}
}
