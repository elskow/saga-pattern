package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	internalkafka "saga-pattern/orchestration-framework/internal/kafka"
	"saga-pattern/orchestration-framework/internal/loops"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/observability"
	"saga-pattern/orchestration-framework/internal/store"
)

type Runtime struct {
	definition  Definition
	store       store.Store
	publisher   Publisher
	metrics     *observability.Metrics
	clock       func() time.Time
	idGenerator func() string
	workerID    string
	config      Config
	outboxLoop  *loops.OutboxLoop
	timeoutLoop *loops.TimeoutLoop
	cleanupLoop *loops.CleanupLoop
}

type publisherAdapter struct{ publisher Publisher }

func (a publisherAdapter) Publish(ctx context.Context, message internalkafka.Message) error {
	return a.publisher.Publish(ctx, Message{
		Topic:       message.Topic,
		Key:         message.Key,
		MessageType: message.MessageType,
		Payload:     message.Payload,
		SagaID:      message.SagaID,
		Step:        message.Step,
		Direction:   string(message.Direction),
	})
}

func New(def Definition, deps Dependencies) *Runtime {
	if err := def.Validate(); err != nil {
		panic(err)
	}
	if deps.Store == nil {
		panic("store is required")
	}
	if deps.Publisher == nil {
		panic("publisher is required")
	}
	config := deps.Config
	if config == (Config{}) {
		config = DefaultConfig()
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := deps.IDGenerator
	if idGenerator == nil {
		idGenerator = func() string { return fmt.Sprintf("saga-%d", clock().UnixNano()) }
	}
	metrics, err := observability.NewMetrics(deps.MetricsRegistry)
	if err != nil {
		panic(err)
	}
	stepNames := make([]string, 0, len(def.Steps))
	for _, step := range def.Steps {
		stepNames = append(stepNames, step.Name)
	}
	metrics.InitCompensationMetrics(def.SagaType, stepNames)
	workerID := deps.WorkerID
	if workerID == "" {
		workerID = idGenerator()
	}
	r := &Runtime{
		definition:  def,
		store:       deps.Store,
		publisher:   deps.Publisher,
		metrics:     metrics,
		clock:       clock,
		idGenerator: idGenerator,
		workerID:    workerID,
		config:      config,
	}
	internalPublisher := publisherAdapter{publisher: deps.Publisher}
	r.outboxLoop = &loops.OutboxLoop{
		LeaseName:   "orchestration-framework-outbox",
		LeaseTTL:    config.LeaseTTL,
		RetryDelay:  config.OutboxRetryDelay,
		BatchSize:   config.OutboxBatchSize,
		SendTimeout: config.KafkaSendTimeout,
		Store:       r.store,
		Publisher:   internalPublisher,
		WorkerID:    workerID,
	}
	r.timeoutLoop = &loops.TimeoutLoop{Store: r.store, Handler: r, Limit: config.OutboxBatchSize}
	r.cleanupLoop = &loops.CleanupLoop{Store: r.store, ProcessedReplyRetention: config.ProcessedReplyRetention, OutboxRetention: config.OutboxRetention}
	return r
}

func NewInMemory(def Definition, deps InMemoryDependencies) *Runtime {
	return New(def, Dependencies{
		Store:           store.NewMemoryStore(),
		Publisher:       deps.Publisher,
		MetricsRegistry: deps.MetricsRegistry,
		Clock:           deps.Clock,
		IDGenerator:     deps.IDGenerator,
		WorkerID:        deps.WorkerID,
		Config:          deps.Config,
	})
}

func NewPostgres(def Definition, db *sql.DB, deps PostgresDependencies) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	postgresStore := store.NewPostgresStore(db)
	if err := postgresStore.InitSchema(context.Background()); err != nil {
		return nil, err
	}
	return New(def, Dependencies{
		Store:           postgresStore,
		Publisher:       deps.Publisher,
		MetricsRegistry: deps.MetricsRegistry,
		Clock:           deps.Clock,
		IDGenerator:     deps.IDGenerator,
		WorkerID:        deps.WorkerID,
		Config:          deps.Config,
	}), nil
}

func (r *Runtime) Registry() *prometheus.Registry {
	return r.metrics.Registry()
}

func (r *Runtime) StartSaga(ctx context.Context, input StartSagaInput) (string, error) {
	data := input.Data.withDefaults()
	if err := data.Validate(); err != nil {
		return "", err
	}
	now := r.clock().UTC()
	sagaID := input.SagaID
	if sagaID == "" {
		sagaID = r.idGenerator()
	}
	if data.PaymentID == "" {
		data.PaymentID = r.idGenerator()
	}
	if data.ReservationID == "" {
		data.ReservationID = r.idGenerator()
	}
	if data.ShippingID == "" {
		data.ShippingID = r.idGenerator()
	}
	step := r.definition.Steps[0]
	sagaRow := model.SagaInstanceRow{
		ID:                 sagaID,
		SagaType:           r.definition.SagaType,
		State:              step.PendingState,
		CurrentStep:        step.Name,
		PendingDirection:   model.StepDirectionForward,
		PendingCommandType: step.Forward.CommandType,
		PendingReplyType:   step.Success.ReplyType,
		StartedAt:          now,
		UpdatedAt:          now,
		DeadlineAt:         now.Add(r.config.SagaTimeout),
		StepDeadlineAt:     now.Add(r.config.PendingCommandRetryDelay),
		RetryCount:         0,
		MaxRetryCount:      r.config.MaxStepRetries,
		Data:               toModelData(data),
	}
	if err := r.store.WithinTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertSaga(ctx, sagaRow); err != nil {
			return err
		}
		if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
			ID:          r.idGenerator(),
			SagaID:      sagaID,
			Step:        step.Name,
			Direction:   model.StepDirectionForward,
			Status:      model.StepStatusPending,
			CommandType: step.Forward.CommandType,
			Attempt:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			return err
		}
		return r.enqueueCommandTx(ctx, tx, sagaRow, step, model.StepDirectionForward, now)
	}); err != nil {
		return "", err
	}
	return sagaID, nil
}

func (r *Runtime) HandleReply(ctx context.Context, envelope internalkafka.ReplyEnvelope) error {
	reply, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload))
	if err != nil {
		return err
	}
	now := r.clock().UTC()
	return r.store.WithinTx(ctx, func(tx store.Tx) error {
		inserted, err := tx.InsertProcessedReply(ctx, model.ProcessedReplyRow{
			ReplyID:    envelope.ReplyID,
			SagaID:     envelope.SagaID,
			ReplyType:  reply.ReplyType(),
			RecordedAt: now,
			ExpiresAt:  now.Add(r.config.ReplyRetention),
		})
		if err != nil || !inserted {
			return err
		}
		sagaRow, ok, err := tx.LockSaga(ctx, envelope.SagaID)
		if err != nil || !ok {
			return err
		}
		if r.isTerminal(sagaRow.State) || sagaRow.PendingCommandType == "" {
			return nil
		}
		currentStep, spec := r.commandForPending(sagaRow)
		if err != nil {
			return err
		}
		if !r.replyTopicMatches(envelope.Topic, reply.ReplyType()) || !r.replyMatches(spec, reply.ReplyType(), sagaRow.PendingDirection) {
			return nil
		}
		history, err := tx.ListStepHistory(ctx, sagaRow.ID)
		if err != nil {
			return err
		}
		pending, ok := latestPending(history, currentStep.Name, sagaRow.PendingDirection)
		if !ok {
			return nil
		}
		completedAt := now
		pending.UpdatedAt = now
		pending.CompletedAt = &completedAt
		result := "success"
		if sagaRow.PendingDirection == model.StepDirectionForward {
			switch reply.ReplyType() {
			case currentStep.Success.ReplyType:
				pending.Status = model.StepStatusSucceeded
				sagaRow.LastError = ""
			case currentStep.Failure.ReplyType:
				pending.Status = model.StepStatusFailed
				result = "failure"
				sagaRow.LastError = extractReason(reply)
			default:
				return nil
			}
		} else {
			if !isCompensationSuccess(reply) {
				return nil
			}
			pending.Status = model.StepStatusCompensated
			result = "compensated"
		}
		if err := tx.UpdateStepHistory(ctx, pending); err != nil {
			return err
		}
		r.metrics.RecordStepDuration(sagaRow.SagaType, currentStep.Name, string(sagaRow.PendingDirection), result, now.Sub(pending.CreatedAt))
		return r.advanceSagaTx(ctx, tx, sagaRow, currentStep, reply, now, history)
	})
}

func (r *Runtime) ConsumeReply(ctx context.Context, envelope ReplyEnvelope) error {
	return r.HandleReply(ctx, internalkafka.ReplyEnvelope{
		ReplyID:    envelope.ReplyID,
		SagaID:     envelope.SagaID,
		Topic:      envelope.Topic,
		ReceivedAt: envelope.ReceivedAt,
		Payload:    envelope.Payload,
	})
}

func (r *Runtime) RecoverExpiredSaga(ctx context.Context, sagaID string, now time.Time) error {
	return r.store.WithinTx(ctx, func(tx store.Tx) error {
		sagaRow, ok, err := tx.LockSaga(ctx, sagaID)
		if err != nil || !ok {
			return err
		}
		if sagaRow.PendingCommandType == "" || sagaRow.StepDeadlineAt.After(now) || r.isTerminal(sagaRow.State) {
			return nil
		}
		history, err := tx.ListStepHistory(ctx, sagaID)
		if err != nil {
			return err
		}
		pending, ok := latestPending(history, sagaRow.CurrentStep, sagaRow.PendingDirection)
		if ok {
			completedAt := now
			pending.Status = model.StepStatusTimedOut
			pending.UpdatedAt = now
			pending.CompletedAt = &completedAt
			pending.Error = "step timeout"
			if err := tx.UpdateStepHistory(ctx, pending); err != nil {
				return err
			}
			r.metrics.RecordStepDuration(sagaRow.SagaType, pending.Step, string(pending.Direction), "timeout", now.Sub(pending.CreatedAt))
		}
		if sagaRow.PendingDirection == model.StepDirectionForward && sagaRow.RetryCount < sagaRow.MaxRetryCount {
			step := r.mustStepByName(sagaRow.CurrentStep)
			sagaRow.RetryCount++
			sagaRow.UpdatedAt = now
			sagaRow.StepDeadlineAt = now.Add(r.config.PendingCommandRetryDelay)
			if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
				return err
			}
			if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
				ID:          r.idGenerator(),
				SagaID:      sagaRow.ID,
				Step:        step.Name,
				Direction:   model.StepDirectionForward,
				Status:      model.StepStatusPending,
				CommandType: step.Forward.CommandType,
				Attempt:     sagaRow.RetryCount + 1,
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return err
			}
			return r.enqueueCommandTx(ctx, tx, sagaRow, step, model.StepDirectionForward, now)
		}
		if sagaRow.PendingDirection == model.StepDirectionForward {
			step := r.mustStepByName(sagaRow.CurrentStep)
			return r.beginCompensationTx(ctx, tx, sagaRow, step, now, history, "timeout exhausted")
		}
		return nil
	})
}

func (r *Runtime) RunWorkers(ctx context.Context) {
	go r.runTickerLoop(ctx, r.config.OutboxPublishInterval, "orchestration outbox worker failed", func(now time.Time) error {
		return r.outboxLoop.RunOnce(ctx, now)
	})
	go r.runTickerLoop(ctx, r.config.TimeoutCheckInterval, "orchestration timeout worker failed", func(now time.Time) error {
		return r.timeoutLoop.RunOnce(ctx, now)
	})
	go r.runTickerLoop(ctx, r.config.CleanupInterval, "orchestration cleanup worker failed", func(now time.Time) error {
		return r.cleanupLoop.RunOnce(ctx, now)
	})
	<-ctx.Done()
}

func (r *Runtime) runTickerLoop(ctx context.Context, interval time.Duration, logMessage string, run func(time.Time) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := run(r.clock().UTC()); err != nil {
				slog.Default().Error(logMessage, "error", err)
			}
		}
	}
}

func (r *Runtime) PublishPending(ctx context.Context) error {
	return r.outboxLoop.RunOnce(ctx, r.clock().UTC())
}

func (r *Runtime) RecoverTimeouts(ctx context.Context) error {
	return r.timeoutLoop.RunOnce(ctx, r.clock().UTC())
}

func (r *Runtime) Cleanup(ctx context.Context) error {
	return r.cleanupLoop.RunOnce(ctx, r.clock().UTC())
}

func (r *Runtime) Snapshot(ctx context.Context, sagaID string) (Snapshot, bool, error) {
	row, ok, err := r.store.GetSaga(ctx, sagaID)
	if err != nil || !ok {
		return Snapshot{}, ok, err
	}
	return toPublicSnapshot(row), true, nil
}

func (r *Runtime) enqueueCommandTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, step Step, direction model.StepDirection, now time.Time) error {
	var command CommandSpec
	if direction == model.StepDirectionForward {
		command = step.Forward
	} else {
		command = step.Compensation
	}
	payload, err := command.BuildPayload(fromModelData(sagaRow.Data))
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return tx.InsertOutbox(ctx, model.OutboxRow{
		ID:          r.idGenerator(),
		SagaID:      sagaRow.ID,
		SagaType:    sagaRow.SagaType,
		Step:        step.Name,
		Direction:   direction,
		Topic:       command.Topic,
		Key:         sagaRow.ID,
		MessageType: command.CommandType,
		Payload:     encoded,
		Status:      model.OutboxStatusPending,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
		MaxAttempts: r.config.OutboxMaxAttempts,
	})
}

func (r *Runtime) advanceSagaTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, step Step, reply commonreplies.SagaReply, now time.Time, history []model.StepHistoryRow) error {
	if sagaRow.PendingDirection == model.StepDirectionCompensation {
		r.metrics.RecordCompensationCompleted(sagaRow.SagaType, step.Name)
		next, ok := nextCompensationStep(history, step.Name)
		if ok {
			nextStep := r.mustStepByName(next)
			sagaRow.State = r.definition.CompensatingState
			sagaRow.CurrentStep = nextStep.Name
			sagaRow.PendingDirection = model.StepDirectionCompensation
			sagaRow.PendingCommandType = nextStep.Compensation.CommandType
			sagaRow.PendingReplyType = nextStep.Compensation.ReplyType
			sagaRow.UpdatedAt = now
			sagaRow.StepDeadlineAt = now.Add(r.config.PendingCommandRetryDelay)
			if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
				return err
			}
			if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
				ID:          r.idGenerator(),
				SagaID:      sagaRow.ID,
				Step:        nextStep.Name,
				Direction:   model.StepDirectionCompensation,
				Status:      model.StepStatusPending,
				CommandType: nextStep.Compensation.CommandType,
				ReplyType:   nextStep.Compensation.ReplyType,
				Attempt:     1,
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return err
			}
			r.metrics.RecordCompensationStarted(sagaRow.SagaType, nextStep.Name)
			return r.enqueueCommandTx(ctx, tx, sagaRow, nextStep, model.StepDirectionCompensation, now)
		}
		sagaRow.State = model.SagaStateCancelled
		sagaRow.CurrentStep = step.Name
		sagaRow.PendingDirection = ""
		sagaRow.PendingCommandType = ""
		sagaRow.PendingReplyType = ""
		sagaRow.StepDeadlineAt = time.Time{}
		sagaRow.UpdatedAt = now
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		return nil
	}

	if reply.ReplyType() == step.Success.ReplyType {
		nextState := step.Success.NextState
		nextStep, ok := r.stepForState(nextState)
		sagaRow.State = nextState
		sagaRow.UpdatedAt = now
		sagaRow.RetryCount = 0
		if ok {
			sagaRow.CurrentStep = nextStep.Name
			sagaRow.PendingDirection = model.StepDirectionForward
			sagaRow.PendingCommandType = nextStep.Forward.CommandType
			sagaRow.PendingReplyType = nextStep.Success.ReplyType
			sagaRow.StepDeadlineAt = now.Add(r.config.PendingCommandRetryDelay)
			if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
				return err
			}
			if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
				ID:          r.idGenerator(),
				SagaID:      sagaRow.ID,
				Step:        nextStep.Name,
				Direction:   model.StepDirectionForward,
				Status:      model.StepStatusPending,
				CommandType: nextStep.Forward.CommandType,
				ReplyType:   nextStep.Success.ReplyType,
				Attempt:     1,
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return err
			}
			return r.enqueueCommandTx(ctx, tx, sagaRow, nextStep, model.StepDirectionForward, now)
		}
		sagaRow.PendingDirection = ""
		sagaRow.PendingCommandType = ""
		sagaRow.PendingReplyType = ""
		sagaRow.StepDeadlineAt = time.Time{}
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "completed", now.Sub(sagaRow.StartedAt))
		return nil
	}
	return r.beginCompensationTx(ctx, tx, sagaRow, step, now, history, extractReason(reply))
}

func (r *Runtime) beginCompensationTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, failedStep Step, now time.Time, history []model.StepHistoryRow, reason string) error {
	sagaRow.LastError = reason
	if failedStep.Failure.NextState == model.SagaStateCancelled {
		sagaRow.State = model.SagaStateCancelled
		sagaRow.PendingDirection = ""
		sagaRow.PendingCommandType = ""
		sagaRow.PendingReplyType = ""
		sagaRow.StepDeadlineAt = time.Time{}
		sagaRow.UpdatedAt = now
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		return nil
	}
	stepName, ok := lastSuccessfulForwardStep(history)
	if !ok {
		sagaRow.State = model.SagaStateCancelled
		sagaRow.PendingDirection = ""
		sagaRow.PendingCommandType = ""
		sagaRow.PendingReplyType = ""
		sagaRow.StepDeadlineAt = time.Time{}
		sagaRow.UpdatedAt = now
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		return nil
	}
	compStep := r.mustStepByName(stepName)
	sagaRow.State = r.definition.CompensatingState
	sagaRow.CurrentStep = compStep.Name
	sagaRow.PendingDirection = model.StepDirectionCompensation
	sagaRow.PendingCommandType = compStep.Compensation.CommandType
	sagaRow.PendingReplyType = compStep.Compensation.ReplyType
	sagaRow.StepDeadlineAt = now.Add(r.config.PendingCommandRetryDelay)
	sagaRow.UpdatedAt = now
	if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
		return err
	}
	if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
		ID:          r.idGenerator(),
		SagaID:      sagaRow.ID,
		Step:        compStep.Name,
		Direction:   model.StepDirectionCompensation,
		Status:      model.StepStatusPending,
		CommandType: compStep.Compensation.CommandType,
		ReplyType:   compStep.Compensation.ReplyType,
		Attempt:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		return err
	}
	r.metrics.RecordCompensationStarted(sagaRow.SagaType, compStep.Name)
	return r.enqueueCommandTx(ctx, tx, sagaRow, compStep, model.StepDirectionCompensation, now)
}

func (r *Runtime) stepForState(state model.SagaState) (Step, bool) {
	for _, step := range r.definition.Steps {
		if step.PendingState == state {
			return step, true
		}
	}
	return Step{}, false
}

func (r *Runtime) mustStepByName(name string) Step {
	for _, step := range r.definition.Steps {
		if step.Name == name {
			return step
		}
	}
	panic("unknown step " + name)
}

func (r *Runtime) commandForPending(sagaRow model.SagaInstanceRow) (Step, CommandSpec) {
	step := r.mustStepByName(sagaRow.CurrentStep)
	if sagaRow.PendingDirection == model.StepDirectionCompensation {
		return step, step.Compensation
	}
	return step, step.Forward
}

func (r *Runtime) replyTopicMatches(topic string, replyType string) bool {
	switch replyType {
	case commonreplies.TypePaymentCompleted, commonreplies.TypePaymentFailed, commonreplies.TypePaymentRefunded:
		return topic == commonkafka.DefaultPaymentRepliesTopic
	case commonreplies.TypeInventoryReserved, commonreplies.TypeInventoryFailed, commonreplies.TypeInventoryReleased:
		return topic == commonkafka.DefaultInventoryRepliesTopic
	case commonreplies.TypeShippingScheduled, commonreplies.TypeShippingFailed, commonreplies.TypeShippingCancelled:
		return topic == commonkafka.DefaultShippingRepliesTopic
	default:
		return false
	}
}

func (r *Runtime) replyMatches(command CommandSpec, replyType string, direction model.StepDirection) bool {
	if direction == model.StepDirectionCompensation {
		return command.ReplyType == replyType
	}
	return true
}

func (r *Runtime) isTerminal(state model.SagaState) bool {
	for _, terminal := range r.definition.TerminalStates {
		if terminal == state {
			return true
		}
	}
	return false
}

func latestPending(rows []model.StepHistoryRow, step string, direction model.StepDirection) (model.StepHistoryRow, bool) {
	for idx := len(rows) - 1; idx >= 0; idx-- {
		if rows[idx].Step == step && rows[idx].Direction == direction && rows[idx].Status == model.StepStatusPending {
			return rows[idx], true
		}
	}
	return model.StepHistoryRow{}, false
}

func lastSuccessfulForwardStep(rows []model.StepHistoryRow) (string, bool) {
	compensated := make(map[string]struct{})
	for _, row := range rows {
		if row.Direction == model.StepDirectionCompensation && row.Status == model.StepStatusCompensated {
			compensated[row.Step] = struct{}{}
		}
	}
	for idx := len(rows) - 1; idx >= 0; idx-- {
		row := rows[idx]
		if row.Direction != model.StepDirectionForward || row.Status != model.StepStatusSucceeded {
			continue
		}
		if _, done := compensated[row.Step]; done {
			continue
		}
		return row.Step, true
	}
	return "", false
}

func nextCompensationStep(rows []model.StepHistoryRow, current string) (string, bool) {
	compensated := make(map[string]struct{})
	for _, row := range rows {
		if row.Direction == model.StepDirectionCompensation && row.Status == model.StepStatusCompensated {
			compensated[row.Step] = struct{}{}
		}
	}
	seenCurrent := false
	for idx := len(rows) - 1; idx >= 0; idx-- {
		row := rows[idx]
		if row.Direction != model.StepDirectionForward || row.Status != model.StepStatusSucceeded {
			continue
		}
		if row.Step == current {
			seenCurrent = true
			continue
		}
		if !seenCurrent {
			continue
		}
		if _, done := compensated[row.Step]; done {
			continue
		}
		return row.Step, true
	}
	return "", false
}

func extractReason(reply commonreplies.SagaReply) string {
	switch typed := reply.(type) {
	case commonreplies.PaymentFailedReply:
		return typed.Reason
	case commonreplies.InventoryFailedReply:
		return typed.Reason
	case commonreplies.ShippingFailedReply:
		return typed.Reason
	case commonreplies.PaymentRefundedReply:
		return typed.Reason
	case commonreplies.InventoryReleasedReply:
		return typed.Reason
	case commonreplies.ShippingCancelledReply:
		return typed.Reason
	default:
		return ""
	}
}

func isCompensationSuccess(reply commonreplies.SagaReply) bool {
	switch typed := reply.(type) {
	case commonreplies.PaymentRefundedReply:
		return typed.Success
	case commonreplies.InventoryReleasedReply:
		return typed.Success
	case commonreplies.ShippingCancelledReply:
		return typed.Success
	default:
		return false
	}
}
