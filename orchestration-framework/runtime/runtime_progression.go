package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

func (r *Runtime[D]) enqueueCommandTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, step Step[D], direction model.StepDirection, now time.Time) error {
	var command CommandSpec[D]
	if direction == model.StepDirectionForward {
		command = step.Forward
	} else {
		command = step.Compensation
	}
	data, err := r.definition.codec().Unmarshal(sagaRow.Data)
	if err != nil {
		return err
	}
	payload, err := command.BuildPayload(data)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	requestID := sagaRow.RequestID
	correlationID := sagaRow.CorrelationID
	benchmarkRun := sagaRow.BenchmarkRun
	benchmarkScene := sagaRow.BenchmarkScene
	benchmarkPhase := sagaRow.BenchmarkPhase
	if requestContext, ok := commoncontext.From(ctx); ok {
		if requestContext.RequestID != "" {
			requestID = requestContext.RequestID
		}
		if requestContext.CorrelationID != "" {
			correlationID = requestContext.CorrelationID
		}
		if requestContext.BenchmarkRun != "" {
			benchmarkRun = requestContext.BenchmarkRun
		}
		if requestContext.BenchmarkScene != "" {
			benchmarkScene = requestContext.BenchmarkScene
		}
		if requestContext.BenchmarkPhase != "" {
			benchmarkPhase = requestContext.BenchmarkPhase
		}
	}
	outboxRow := model.OutboxRow{
		ID:             r.idGenerator(),
		SagaID:         sagaRow.ID,
		SagaType:       sagaRow.SagaType,
		Step:           step.Name,
		Direction:      direction,
		Topic:          command.Topic,
		Key:            sagaRow.ID,
		MessageType:    command.CommandType,
		Payload:        encoded,
		Status:         model.OutboxStatusPending,
		AvailableAt:    now,
		CreatedAt:      now,
		UpdatedAt:      now,
		MaxAttempts:    r.config.OutboxMaxAttempts,
		RequestID:      requestID,
		CorrelationID:  correlationID,
		BenchmarkRun:   benchmarkRun,
		BenchmarkScene: benchmarkScene,
		BenchmarkPhase: benchmarkPhase,
		TraceHeaders:   traceHeadersFromContext(ctx),
	}
	if err := tx.InsertOutbox(ctx, outboxRow); err != nil {
		return err
	}
	span := trace.SpanFromContext(ctx)
	attrs := commandEnqueuedAttributes(outboxRow)
	span.SetAttributes(attrs...)
	span.AddEvent("saga.command.enqueued", trace.WithAttributes(attrs...))
	return nil
}

func traceHeadersFromContext(ctx context.Context) map[string]string {
	headers := commontracing.InjectHeaders(ctx, nil)
	traceHeaders := make(map[string]string, 2)
	for _, key := range []string{"traceparent", "tracestate"} {
		if value := headers[key]; value != "" {
			traceHeaders[key] = value
		}
	}
	if len(traceHeaders) == 0 {
		return nil
	}
	return traceHeaders
}

func commandEnqueuedAttributes(row model.OutboxRow) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("saga.command.event", "saga.command.enqueued"),
		attribute.Bool("saga.command.enqueued", true),
		attribute.String("saga.id", row.SagaID),
		attribute.String("saga.type", row.SagaType),
		attribute.String("saga.step", row.Step),
		attribute.String("saga.pending.direction", string(row.Direction)),
		attribute.String("saga.phase", phaseForDirection(row.Direction)),
		attribute.String("topic", row.Topic),
		attribute.String("saga.command.type", row.MessageType),
		attribute.Int("messaging.message.payload_size_bytes", len(row.Payload)),
	}
	if row.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", row.RequestID))
	}
	if row.CorrelationID != "" {
		attrs = append(attrs, attribute.String("correlation.id", row.CorrelationID))
	}
	if row.BenchmarkRun != "" {
		attrs = append(attrs, attribute.String("benchmark.run_label", row.BenchmarkRun))
	}
	if row.BenchmarkScene != "" {
		attrs = append(attrs, attribute.String("benchmark.scenario", row.BenchmarkScene))
	}
	if row.BenchmarkPhase != "" {
		attrs = append(attrs, attribute.String("benchmark.phase", row.BenchmarkPhase))
	}
	return attrs
}

func (r *Runtime[D]) advanceSagaTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, step Step[D], decision Decision, now time.Time, history []model.StepHistoryRow) (bool, error) {
	span := trace.SpanFromContext(ctx)
	if sagaRow.PendingDirection == model.StepDirectionCompensation {
		r.metrics.RecordCompensationCompleted(sagaRow.SagaType, step.Name)
		span.AddEvent("saga.compensation.completed",
			trace.WithAttributes(
				attribute.String("saga.step", step.Name),
				attribute.String("saga.pending.direction", string(model.StepDirectionCompensation)),
			),
		)
		next, ok := nextCompensationStep(history, step.Name)
		if ok {
			nextStep, err := r.stepByName(next)
			if err != nil {
				return false, err
			}
			sagaRow.State = model.SagaState(r.definition.CompensatingState)
			setPendingStep(&sagaRow, nextStep, model.StepDirectionCompensation, pendingReplyTypes(nextStep.CompensateReplies), now, r.config.PendingCommandRetryDelay)
			if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
				return false, err
			}
			if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
				ID:          r.idGenerator(),
				SagaID:      sagaRow.ID,
				Step:        nextStep.Name,
				Direction:   model.StepDirectionCompensation,
				Status:      model.StepStatusPending,
				CommandType: nextStep.Compensation.CommandType,
				ReplyType:   pendingReplyTypes(nextStep.CompensateReplies),
				Attempt:     1,
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return false, err
			}
			r.metrics.RecordCompensationStarted(sagaRow.SagaType, nextStep.Name)
			span.SetAttributes(
				attribute.String("saga.phase", "compensation"),
				attribute.Bool("saga.compensating", true),
				attribute.String("saga.outcome", "compensation_advanced"),
			)
			span.AddEvent("saga.compensation.started",
				trace.WithAttributes(
					attribute.String("saga.state.after", string(sagaRow.State)),
					attribute.String("saga.step", nextStep.Name),
					attribute.String("saga.pending.direction", string(sagaRow.PendingDirection)),
					attribute.String("saga.command.type", nextStep.Compensation.CommandType),
				),
			)
			if err := r.enqueueCommandTx(ctx, tx, sagaRow, nextStep, model.StepDirectionCompensation, now); err != nil {
				return false, err
			}
			return true, nil
		}
		cancelSaga(&sagaRow, now, sagaRow.LastError)
		sagaRow.CurrentStep = step.Name
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		span.SetAttributes(
			attribute.String("saga.phase", "terminal"),
			attribute.String("saga.outcome", "cancelled"),
			attribute.String("failure.type", failureTypeFromText(sagaRow.LastError)),
		)
		span.AddEvent("saga.cancelled",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("failure.type", failureTypeFromText(sagaRow.LastError)),
			),
		)
		return false, nil
	}

	switch decision.Kind {
	case DecisionAdvance:
		nextState := decision.NextState
		nextStep, err := r.stepByName(decision.NextStep)
		if err != nil {
			return false, err
		}
		sagaRow.State = model.SagaState(nextState)
		sagaRow.RetryCount = 0
		setPendingStep(&sagaRow, nextStep, model.StepDirectionForward, pendingReplyTypes(nextStep.ForwardReplies), now, r.config.PendingCommandRetryDelay)
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
			ID:          r.idGenerator(),
			SagaID:      sagaRow.ID,
			Step:        nextStep.Name,
			Direction:   model.StepDirectionForward,
			Status:      model.StepStatusPending,
			CommandType: nextStep.Forward.CommandType,
			ReplyType:   pendingReplyTypes(nextStep.ForwardReplies),
			Attempt:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			return false, err
		}
		span.SetAttributes(
			attribute.String("saga.phase", "forward"),
			attribute.Bool("saga.compensating", false),
			attribute.String("saga.outcome", "advanced"),
		)
		span.AddEvent("saga.step.advanced",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("saga.decision", string(decision.Kind)),
				attribute.String("saga.step", nextStep.Name),
				attribute.String("saga.pending.direction", string(sagaRow.PendingDirection)),
				attribute.String("saga.command.type", nextStep.Forward.CommandType),
			),
		)
		if err := r.enqueueCommandTx(ctx, tx, sagaRow, nextStep, model.StepDirectionForward, now); err != nil {
			return false, err
		}
		return true, nil
	case DecisionComplete:
		sagaRow.State = model.SagaState(decision.NextState)
		sagaRow.UpdatedAt = now
		sagaRow.RetryCount = 0
		clearPendingState(&sagaRow)
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "completed", now.Sub(sagaRow.StartedAt))
		span.SetAttributes(
			attribute.String("saga.phase", "terminal"),
			attribute.String("saga.outcome", "completed"),
		)
		span.AddEvent("saga.completed",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
			),
		)
		return false, nil
	case DecisionBeginCompensation:
		return r.beginCompensationTx(ctx, tx, sagaRow, step, now, history, decision.Reason)
	case DecisionCancel:
		cancelSaga(&sagaRow, now, decision.Reason)
		sagaRow.State = model.SagaState(decision.NextState)
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		span.SetAttributes(
			attribute.String("saga.phase", "terminal"),
			attribute.String("saga.outcome", "cancelled"),
			attribute.String("failure.type", failureTypeFromText(sagaRow.LastError)),
		)
		span.AddEvent("saga.cancelled",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("failure.type", failureTypeFromText(sagaRow.LastError)),
			),
		)
		return false, nil
	default:
		return false, fmt.Errorf("unsupported forward decision %q for step %s", decision.Kind, step.Name)
	}
}

func (r *Runtime[D]) beginCompensationTx(ctx context.Context, tx store.Tx, sagaRow model.SagaInstanceRow, failedStep Step[D], now time.Time, history []model.StepHistoryRow, reason string) (bool, error) {
	span := trace.SpanFromContext(ctx)
	sagaRow.LastError = reason
	_, hasSuccessfulForwardStep := lastSuccessfulForwardStep(history)
	if failedStep.Name == r.definition.Steps[0].Name && !hasSuccessfulForwardStep {
		cancelSaga(&sagaRow, now, reason)
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		span.SetAttributes(
			attribute.String("saga.phase", "terminal"),
			attribute.String("saga.outcome", "cancelled"),
			attribute.String("failure.type", failureTypeFromText(reason)),
		)
		span.AddEvent("saga.cancelled",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("failure.type", failureTypeFromText(reason)),
			),
		)
		return false, nil
	}
	stepName, ok := lastSuccessfulForwardStep(history)
	if !ok {
		cancelSaga(&sagaRow, now, reason)
		if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
			return false, err
		}
		r.metrics.RecordSagaDuration(sagaRow.SagaType, "cancelled", now.Sub(sagaRow.StartedAt))
		span.SetAttributes(
			attribute.String("saga.phase", "terminal"),
			attribute.String("saga.outcome", "cancelled"),
			attribute.String("failure.type", failureTypeFromText(reason)),
		)
		span.AddEvent("saga.cancelled",
			trace.WithAttributes(
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("failure.type", failureTypeFromText(reason)),
			),
		)
		return false, nil
	}
	compStep, err := r.stepByName(stepName)
	if err != nil {
		return false, err
	}
	sagaRow.State = model.SagaState(r.definition.CompensatingState)
	setPendingStep(&sagaRow, compStep, model.StepDirectionCompensation, pendingReplyTypes(compStep.CompensateReplies), now, r.config.PendingCommandRetryDelay)
	if err := tx.UpdateSaga(ctx, sagaRow); err != nil {
		return false, err
	}
	if err := tx.InsertStepHistory(ctx, model.StepHistoryRow{
		ID:          r.idGenerator(),
		SagaID:      sagaRow.ID,
		Step:        compStep.Name,
		Direction:   model.StepDirectionCompensation,
		Status:      model.StepStatusPending,
		CommandType: compStep.Compensation.CommandType,
		ReplyType:   pendingReplyTypes(compStep.CompensateReplies),
		Attempt:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		return false, err
	}
	r.metrics.RecordCompensationStarted(sagaRow.SagaType, compStep.Name)
	span.SetAttributes(
		attribute.String("saga.phase", "compensation"),
		attribute.Bool("saga.compensating", true),
		attribute.String("saga.outcome", "compensation_started"),
		attribute.String("failure.type", failureTypeFromText(reason)),
	)
	span.AddEvent("saga.compensation.started",
		trace.WithAttributes(
			attribute.String("saga.state.after", string(sagaRow.State)),
			attribute.String("saga.step", compStep.Name),
			attribute.String("saga.pending.direction", string(sagaRow.PendingDirection)),
			attribute.String("saga.command.type", compStep.Compensation.CommandType),
			attribute.String("failure.type", failureTypeFromText(reason)),
		),
	)
	if err := r.enqueueCommandTx(ctx, tx, sagaRow, compStep, model.StepDirectionCompensation, now); err != nil {
		return false, err
	}
	return true, nil
}

func phaseForDirection(direction model.StepDirection) string {
	if direction == model.StepDirectionCompensation {
		return "compensation"
	}
	return "forward"
}

func failureTypeFromText(value string) string {
	normalized := strings.ToLower(value)
	switch {
	case strings.Contains(normalized, "payment"):
		return "payment"
	case strings.Contains(normalized, "inventory") || strings.Contains(normalized, "reservation") || strings.Contains(normalized, "stock"):
		return "inventory"
	case strings.Contains(normalized, "shipping") || strings.Contains(normalized, "shipment"):
		return "shipping"
	case strings.Contains(normalized, "timeout") || strings.Contains(normalized, "deadline"):
		return "timeout"
	case strings.TrimSpace(value) == "":
		return "unknown"
	default:
		return "domain_failure"
	}
}
