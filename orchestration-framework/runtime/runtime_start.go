package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

func (r *Runtime[D]) StartSaga(ctx context.Context, input StartSagaInput[D]) (sagaID string, err error) {
	step := r.definition.Steps[0]
	ctx, span := r.startSpan(ctx, "orchestration.runtime.start_saga",
		attribute.String("saga.type", r.definition.SagaType),
		attribute.String("saga.id", input.SagaID),
		attribute.String("saga.step", step.Name),
		attribute.String("saga.pending.direction", string(model.StepDirectionForward)),
		attribute.String("saga.command.type", step.Forward.CommandType),
		attribute.String("saga.reply.types", pendingReplyTypes(step.ForwardReplies)),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	data := input.Data
	if r.definition.ValidateData != nil {
		if err := r.definition.ValidateData(data); err != nil {
			return "", err
		}
	}
	encodedData, err := r.definition.codec().Marshal(data)
	if err != nil {
		return "", err
	}
	now := r.clock().UTC()
	sagaID = input.SagaID
	if sagaID == "" {
		sagaID = r.idGenerator()
	}
	requestContext, _ := commoncontext.From(ctx)
	span.SetAttributes(
		attribute.String("saga.id", sagaID),
		attribute.String("saga.pattern", "orchestration"),
		attribute.String("saga.role", "orchestrator"),
		attribute.String("saga.phase", "start"),
		attribute.String("saga.outcome", "starting"),
	)
	sagaRow := model.SagaInstanceRow{
		ID:                 sagaID,
		SagaType:           r.definition.SagaType,
		State:              model.SagaState(step.PendingState),
		CurrentStep:        step.Name,
		PendingDirection:   model.StepDirectionForward,
		PendingCommandType: step.Forward.CommandType,
		PendingReplyType:   pendingReplyTypes(step.ForwardReplies),
		StartedAt:          now,
		UpdatedAt:          now,
		DeadlineAt:         now.Add(r.config.SagaTimeout),
		StepDeadlineAt:     now.Add(r.config.PendingCommandRetryDelay),
		RetryCount:         0,
		MaxRetryCount:      r.config.MaxStepRetries,
		Data:               encodedData,
		RequestID:          requestContext.RequestID,
		CorrelationID:      requestContext.CorrelationID,
		BenchmarkRun:       requestContext.BenchmarkRun,
		BenchmarkScene:     requestContext.BenchmarkScene,
		BenchmarkPhase:     requestContext.BenchmarkPhase,
	}
	if err = r.store.WithinTx(ctx, func(tx store.Tx) error {
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
			ReplyType:   pendingReplyTypes(step.ForwardReplies),
			Attempt:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			return err
		}
		return r.enqueueCommandTx(ctx, tx, sagaRow, step, model.StepDirectionForward, now)
	}); err != nil {
		if errors.Is(err, store.ErrSagaAlreadyExists) {
			return "", fmt.Errorf("%w: %s", ErrSagaAlreadyExists, sagaID)
		}
		return "", err
	}
	r.triggerOutboxPublish(ctx, span)
	span.SetAttributes(
		attribute.String("saga.state", string(sagaRow.State)),
		attribute.String("saga.outcome", "started"),
		attribute.Int("saga.retry_count", sagaRow.RetryCount),
		attribute.String("saga.deadline_at", sagaRow.DeadlineAt.Format(time.RFC3339Nano)),
	)
	span.AddEvent("saga.started",
		trace.WithAttributes(
			attribute.String("saga.step", step.Name),
			attribute.String("saga.phase", "forward"),
			attribute.String("saga.pending.direction", string(model.StepDirectionForward)),
			attribute.String("saga.command.type", step.Forward.CommandType),
			attribute.String("saga.reply.types", pendingReplyTypes(step.ForwardReplies)),
		),
	)
	return sagaID, nil
}
