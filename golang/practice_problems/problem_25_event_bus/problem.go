// =============================================================================
// INTERVIEW PROBLEM 25: Concurrent Pub/Sub Event Bus
// Difficulty: Senior Software Engineer | Estimated time: 45 min
// Concurrency patterns: RWMutex, per-subscriber buffered channels, slow-consumer
// policies (block / drop-newest / drop-oldest), safe channel closing,
// deadlock avoidance
// =============================================================================
//
// CONTEXT
// -------
// You're building the internal event bus for an IoT device-telemetry platform.
// Producers publish events to named topics; any number of consumers subscribe
// to a topic and read events from their own buffered channel. The bus is used
// from many goroutines at once, and consumers come and go at runtime.
//
// The classic traps this problem is built around:
//
//   - Holding a lock while sending on a channel: one slow consumer stalls every
//     publisher AND every Subscribe/Unsubscribe call (or deadlocks them).
//   - Closing a subscriber's channel while a publisher is still sending on it
//     ("send on closed channel" panic).
//   - A blocked publisher that can never be released when its target leaves.
//
// Run your tests with the race detector:
//
//	./run_tests.sh -f golang/practice_problem_answers/cw_answer_25_event_bus.go -c go test -race -v .
//
// DATA MODEL
// ----------
// Event        — Topic string, Payload int
// Policy       — what to do when a subscriber's buffer is full:
//                Block (publisher waits), DropNewest (discard the new event),
//                DropOldest (evict the oldest buffered event to make room)
// Bus          — topic -> set of subscriptions
// Subscription — C() <-chan Event, Unsubscribe(), Dropped() int
//
// SEMANTICS
// ---------
//   - Delivery order to a single subscriber matches the order of Publish calls
//     made by a single goroutine.
//   - Publish delivers to every current subscriber of the topic and returns the
//     number of events actually enqueued (dropped-newest and aborted deliveries
//     do NOT count; an evict-and-enqueue under DropOldest does).
//   - Subscribers are delivered to one after another; a Block subscriber with a
//     full buffer therefore delays later subscribers of the same Publish call.
//   - buffer < 1 is treated as 1.
//
// EXAMPLE
// -------
//	b := NewBus()
//	sub, _ := b.Subscribe("temp", 8)
//	n, _ := b.Publish("temp", 21) // n == 1
//	ev := <-sub.C()               // Event{Topic: "temp", Payload: 21}
//	sub.Unsubscribe()             // sub.C() is now closed
// =============================================================================

package eventbus

import "errors"

// ErrBusClosed is returned by Subscribe/Publish once the bus has been closed.
var ErrBusClosed = errors.New("bus closed")

// Event is a message published on a topic.
type Event struct {
	Topic   string
	Payload int
}

// Policy controls what happens when a subscriber's buffer is full.
type Policy int

const (
	// Block makes Publish wait until the subscriber has room (or leaves).
	Block Policy = iota
	// DropNewest discards the event being published; Publish never blocks.
	DropNewest
	// DropOldest evicts the oldest buffered event to make room; Publish never blocks.
	DropOldest
)

// Subscription is one consumer's view of a topic.
type Subscription struct {
	// TODO: add whatever fields you need.
}

// Bus routes events from publishers to subscribers.
type Bus struct {
	// TODO: add whatever fields you need.
}

// NewBus returns an empty, open bus.
func NewBus() *Bus {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// PART 1 — Topics, publish, unsubscribe  (~15 min)
// ---------------------------------------------------------------------------

// Subscribe registers a Block-policy subscriber on topic with the given buffer.
// Returns ErrBusClosed if the bus is closed.
func (b *Bus) Subscribe(topic string, buffer int) (*Subscription, error) {
	panic("not implemented")
}

// Publish sends an event to every current subscriber of topic and returns how
// many deliveries were enqueued. Publishing to a topic with no subscribers
// returns (0, nil). Returns (0, ErrBusClosed) if the bus is closed.
func (b *Bus) Publish(topic string, payload int) (int, error) {
	panic("not implemented")
}

// C returns the channel on which this subscription's events arrive.
func (s *Subscription) C() <-chan Event {
	panic("not implemented")
}

// Unsubscribe removes the subscription from the bus and closes C().
// It is idempotent and safe to call from any goroutine.
func (s *Subscription) Unsubscribe() {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// PART 2 — Slow-consumer policies  (~15 min)
// ---------------------------------------------------------------------------

// SubscribeWithPolicy is like Subscribe but with an explicit full-buffer policy.
func (b *Bus) SubscribeWithPolicy(topic string, buffer int, policy Policy) (*Subscription, error) {
	panic("not implemented")
}

// Dropped returns how many events this subscription has lost to its policy
// (DropNewest: discarded new events; DropOldest: evicted buffered events).
// Always 0 for Block subscribers.
func (s *Subscription) Dropped() int {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// PART 3 — Safe shutdown and concurrent churn  (~15 min)
// ---------------------------------------------------------------------------
//
// Your Part 1/2 code must now survive:
//   - Unsubscribe called while a Publish is blocked sending to that subscriber:
//     the Publish must be released (that delivery simply doesn't count) and
//     nothing may panic or deadlock.
//   - Many goroutines publishing, subscribing and unsubscribing at once.

// Close shuts the bus down: every subscription's channel is closed, blocked
// publishers are released, and later Subscribe/Publish calls return
// ErrBusClosed. It is idempotent and safe to call concurrently.
func (b *Bus) Close() {
	panic("not implemented")
}
