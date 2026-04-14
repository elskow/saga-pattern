package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"saga-pattern/common/dto"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

func TestSagaStateMachineHappyPath(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)
	fixture.publishOutbox(t)

	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("payment"), fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateInventoryPending)
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("inventory"), fixture.inventoryRepliesTopic(), commonreplies.NewInventoryReservedReply("RES-1", "ORDER-1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateShippingPending)
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("shipping"), fixture.shippingRepliesTopic(), commonreplies.NewShippingScheduledReply("SHIP-1", "ORDER-1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateCompleted)

	assertMetricNames(t, fixture.runtime, []string{
		"saga_framework_duration_seconds",
		"saga_framework_step_duration_seconds",
		"saga_framework_compensation_started_total",
		"saga_framework_compensation_completed_total",
	})
	assertStepStatuses(t, fixture.store, sagaID, []model.StepStatus{
		model.StepStatusSucceeded,
		model.StepStatusSucceeded,
		model.StepStatusSucceeded,
	})
}

func TestDuplicateReplyAndTimeoutRecovery(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)
	fixture.publishOutbox(t)

	paymentReplyID := fixture.replyID("payment")
	fixture.handleReply(t, sagaID, fixture.now, paymentReplyID, fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	fixture.handleReply(t, sagaID, fixture.now, paymentReplyID, fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	fixture.publishOutbox(t)

	fixture.advance(11 * time.Second)
	if err := fixture.runtime.timeoutLoop.RunOnce(context.Background(), fixture.now); err != nil {
		t.Fatalf("timeout recovery: %v", err)
	}
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("inventory-failed"), fixture.inventoryRepliesTopic(), commonreplies.NewInventoryFailedReply("RES-1", "ORDER-1", "out of stock"))
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("payment-refunded"), fixture.paymentRepliesTopic(), commonreplies.NewPaymentRefundedReply("PAY-1", "ORDER-1", true, ""))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateCancelled)

	history, err := fixture.store.ListStepHistory(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list step history: %v", err)
	}
	var timedOut, compensated bool
	for _, row := range history {
		if row.Step == "Inventory" && row.Status == model.StepStatusTimedOut {
			timedOut = true
		}
		if row.Step == "Payment" && row.Direction == model.StepDirectionCompensation && row.Status == model.StepStatusCompensated {
			compensated = true
		}
	}
	if !timedOut {
		t.Fatalf("expected inventory timeout to be recorded")
	}
	if !compensated {
		t.Fatalf("expected payment compensation to complete")
	}
	if len(fixture.publisher.Messages()) != 4 {
		t.Fatalf("published message count = %d, want 4", len(fixture.publisher.Messages()))
	}
}

func TestOutboxPublisherIsReplicaSafe(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)

	other := New(OrderDefinition(), Dependencies{
		Store:       fixture.store,
		Publisher:   fixture.publisher,
		Clock:       fixture.clock,
		IDGenerator: fixture.idGenerator,
		WorkerID:    "worker-b",
		Config:      fixture.runtime.config,
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = fixture.runtime.outboxLoop.RunOnce(context.Background(), fixture.now)
	}()
	go func() {
		defer wg.Done()
		_ = other.outboxLoop.RunOnce(context.Background(), fixture.now)
	}()
	wg.Wait()

	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox row count = %d, want 1", len(outbox))
	}
	if outbox[0].Status != model.OutboxStatusSent {
		t.Fatalf("outbox status = %s, want %s", outbox[0].Status, model.OutboxStatusSent)
	}
	if len(fixture.publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(fixture.publisher.Messages()))
	}
}

func TestExpiredSendingOutboxRowIsReclaimed(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)

	claimed, err := fixture.store.ClaimOutbox(context.Background(), "worker-a", fixture.now, fixture.runtime.config.OutboxBatchSize, fixture.runtime.config.OutboxRetryDelay)
	if err != nil {
		t.Fatalf("claim outbox: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed outbox rows = %d, want 1", len(claimed))
	}

	fixture.advance(fixture.runtime.config.OutboxRetryDelay + time.Second)
	fixture.publishOutbox(t)

	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox row count = %d, want 1", len(outbox))
	}
	if outbox[0].Status != model.OutboxStatusSent {
		t.Fatalf("outbox status = %s, want %s", outbox[0].Status, model.OutboxStatusSent)
	}
	if outbox[0].AttemptCount != 2 {
		t.Fatalf("outbox attempt_count = %d, want 2", outbox[0].AttemptCount)
	}
	if len(fixture.publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(fixture.publisher.Messages()))
	}
}

type fixture struct {
	t           *testing.T
	store       *store.MemoryStore
	publisher   *recordingPublisher
	runtime     *Runtime
	now         time.Time
	clock       func() time.Time
	ids         []string
	idx         int
	idGenerator func() string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		t:         t,
		store:     store.NewMemoryStore(),
		publisher: newRecordingPublisher(),
		now:       time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC),
		ids:       []string{"saga-1", "hist-1", "outbox-1", "reply-1", "hist-2", "outbox-2", "hist-3", "outbox-3", "hist-4", "outbox-4", "hist-5", "outbox-5", "hist-6", "outbox-6"},
	}
	f.clock = func() time.Time { return f.now }
	f.idGenerator = func() string {
		if f.idx >= len(f.ids) {
			return "generated-extra-id"
		}
		value := f.ids[f.idx]
		f.idx++
		return value
	}
	f.runtime = New(OrderDefinition(), Dependencies{
		Store:       f.store,
		Publisher:   f.publisher,
		Clock:       f.clock,
		IDGenerator: f.idGenerator,
		WorkerID:    "worker-a",
		Config:      DefaultConfig(),
	})
	return f
}

func (f *fixture) startSaga(t *testing.T) string {
	t.Helper()
	sagaID, err := f.runtime.StartSaga(context.Background(), StartSagaInput{Data: SagaData{
		OrderID:         "ORDER-1",
		CustomerID:      "CUST-1",
		PaymentID:       "PAY-1",
		ReservationID:   "RES-1",
		ShippingID:      "SHIP-1",
		CorrelationID:   "corr-1",
		ShippingAddress: "123 Main Street",
		TotalAmount:     json.Number("99.99"),
		Items:           []dto.OrderItemRequest{{ProductID: "PROD-1", ProductName: "Widget", Quantity: 1, Price: json.Number("99.99")}},
	}})
	if err != nil {
		t.Fatalf("start saga: %v", err)
	}
	return sagaID
}

func (f *fixture) publishOutbox(t *testing.T) {
	t.Helper()
	if err := f.runtime.outboxLoop.RunOnce(context.Background(), f.now); err != nil {
		t.Fatalf("publish outbox: %v", err)
	}
}

func (f *fixture) handleReply(t *testing.T, sagaID string, at time.Time, replyID string, topic string, reply commonreplies.SagaReply) {
	t.Helper()
	payload, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	if err := f.runtime.ConsumeReply(context.Background(), ReplyEnvelope{ReplyID: replyID, SagaID: sagaID, Topic: topic, ReceivedAt: at, Payload: payload}); err != nil {
		t.Fatalf("handle reply: %v", err)
	}
}

func (f *fixture) replyID(prefix string) string {
	return prefix + "-reply"
}

func (f *fixture) advance(delta time.Duration) {
	f.now = f.now.Add(delta)
}

func (f *fixture) paymentRepliesTopic() string   { return "orchestration.payment.replies" }
func (f *fixture) inventoryRepliesTopic() string { return "orchestration.inventory.replies" }
func (f *fixture) shippingRepliesTopic() string  { return "orchestration.shipping.replies" }

func assertSagaState(t *testing.T, s *store.MemoryStore, sagaID string, want model.SagaState) {
	t.Helper()
	row, ok, err := s.GetSaga(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("get saga: %v", err)
	}
	if !ok {
		t.Fatalf("saga %s not found", sagaID)
	}
	if row.State != want {
		t.Fatalf("saga state = %s, want %s", row.State, want)
	}
}

func assertMetricNames(t *testing.T, runtime *Runtime, names []string) {
	t.Helper()
	families, err := runtime.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	seen := make(map[string]struct{}, len(families))
	for _, family := range families {
		seen[family.GetName()] = struct{}{}
	}
	for _, name := range names {
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing metric %s", name)
		}
	}
}

func assertStepStatuses(t *testing.T, s *store.MemoryStore, sagaID string, want []model.StepStatus) {
	t.Helper()
	rows, err := s.ListStepHistory(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list step history: %v", err)
	}
	got := make([]model.StepStatus, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.Status)
	}
	if len(got) != len(want) {
		t.Fatalf("step history len = %d, want %d (%v)", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("step status[%d] = %s, want %s", idx, got[idx], want[idx])
		}
	}
}

type publishedMessage struct {
	Message Message
	Body    any
}

type recordingPublisher struct {
	mu       sync.Mutex
	messages []publishedMessage
	failures map[string]int
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{failures: make(map[string]int)}
}

func (p *recordingPublisher) Publish(_ context.Context, message Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if remaining := p.failures[message.MessageType]; remaining > 0 {
		p.failures[message.MessageType] = remaining - 1
		return fmt.Errorf("forced publish failure for %s", message.MessageType)
	}
	var body any
	if len(message.Payload) > 0 {
		_ = json.Unmarshal(message.Payload, &body)
	}
	p.messages = append(p.messages, publishedMessage{Message: message, Body: body})
	return nil
}

func (p *recordingPublisher) Messages() []publishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishedMessage(nil), p.messages...)
}
