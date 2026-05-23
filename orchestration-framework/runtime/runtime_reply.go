package runtime

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	internalkafka "saga-pattern/orchestration-framework/internal/kafka"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

func (r *Runtime[D]) HandleReply(ctx context.Context, envelope internalkafka.ReplyEnvelope) (err error) {
	ctx, span := r.startSpan(ctx, "orchestration.runtime.handle_reply",
		attribute.String("saga.id", envelope.SagaID),
		attribute.String("saga.pattern", "orchestration"),
		attribute.String("saga.role", "orchestrator"),
		attribute.String("saga.phase", "reply"),
		attribute.String("reply.id", envelope.ReplyID),
		attribute.String("reply.topic", envelope.Topic),
		attribute.String("reply.received_at", envelope.ReceivedAt.UTC().Format(time.RFC3339Nano)),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	replyType, err := ReplyType(envelope.Payload)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.String("reply.type", replyType))
	span.AddEvent("saga.reply.received",
		trace.WithAttributes(
			attribute.String("reply.id", envelope.ReplyID),
			attribute.String("reply.type", replyType),
			attribute.String("reply.topic", envelope.Topic),
		),
	)
	now := r.clock().UTC()
	commandEnqueued := false
	err = r.store.WithinTx(ctx, func(tx store.Tx) error {
		inserted, err := tx.InsertProcessedReply(ctx, model.ProcessedReplyRow{
			ReplyID:    envelope.ReplyID,
			SagaID:     envelope.SagaID,
			ReplyType:  replyType,
			RecordedAt: now,
			ExpiresAt:  now.Add(r.config.ReplyRetention),
		})
		if err != nil {
			return err
		}
		if !inserted {
			span.SetAttributes(attribute.String("saga.outcome", "duplicate_reply"))
			span.AddEvent("saga.reply.duplicate",
				trace.WithAttributes(
					attribute.String("reply.id", envelope.ReplyID),
					attribute.String("reply.type", replyType),
				),
			)
			return nil
		}
		sagaRow, ok, err := tx.LockSaga(ctx, envelope.SagaID)
		if err != nil || !ok {
			return err
		}
		span.SetAttributes(
			attribute.String("saga.type", sagaRow.SagaType),
			attribute.String("saga.state.before", string(sagaRow.State)),
			attribute.String("saga.step", sagaRow.CurrentStep),
			attribute.String("saga.pending.direction", string(sagaRow.PendingDirection)),
			attribute.String("saga.command.type", sagaRow.PendingCommandType),
			attribute.String("saga.reply.types", sagaRow.PendingReplyType),
			attribute.Int("saga.retry_count", sagaRow.RetryCount),
		)
		if r.isTerminal(sagaRow.State) || sagaRow.PendingCommandType == "" {
			span.SetAttributes(attribute.String("saga.outcome", "reply_ignored"))
			span.AddEvent("saga.reply.ignored",
				trace.WithAttributes(attribute.String("ignore.type", "terminal_or_no_pending_command")),
			)
			return nil
		}
		currentStep, err := r.stepByName(sagaRow.CurrentStep)
		if err != nil {
			return err
		}
		matchedReply, ok := r.replyCaseFor(currentStep, sagaRow.PendingDirection, envelope.Topic, replyType)
		if !ok {
			span.SetAttributes(attribute.String("saga.outcome", "reply_ignored"))
			span.AddEvent("saga.reply.ignored",
				trace.WithAttributes(
					attribute.String("ignore.type", "no_matching_reply_case"),
					attribute.String("saga.step", currentStep.Name),
				),
			)
			return nil
		}
		data, err := r.definition.codec().Unmarshal(sagaRow.Data)
		if err != nil {
			return err
		}
		decision, err := matchedReply.Decide(data, envelope.Payload)
		if err != nil {
			return err
		}
		if decision.Kind == DecisionIgnore {
			span.SetAttributes(attribute.String("saga.outcome", "reply_ignored"))
			span.AddEvent("saga.reply.ignored",
				trace.WithAttributes(attribute.String("decision.kind", string(decision.Kind))),
			)
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
		result, err := applyDecisionToPending(&sagaRow, &pending, decision)
		if err != nil {
			return err
		}
		pending.ReplyType = replyType
		if err := tx.UpdateStepHistory(ctx, pending); err != nil {
			return err
		}
		span.SetAttributes(
			attribute.String("saga.decision", string(decision.Kind)),
			attribute.String("saga.outcome", result),
		)
		span.AddEvent("saga.step.completed",
			trace.WithAttributes(
				attribute.String("decision.kind", string(decision.Kind)),
				attribute.String("saga.state.after", string(sagaRow.State)),
				attribute.String("step.status", result),
				attribute.String("saga.step", currentStep.Name),
				attribute.String("saga.pending.direction", string(sagaRow.PendingDirection)),
			),
		)
		r.metrics.RecordStepDuration(sagaRow.SagaType, currentStep.Name, string(sagaRow.PendingDirection), result, now.Sub(pending.CreatedAt))
		commandEnqueued, err = r.advanceSagaTx(ctx, tx, sagaRow, currentStep, decision, now, history)
		return err
	})
	if err == nil && commandEnqueued {
		r.triggerOutboxPublish(ctx, span)
	}
	return
}

func (r *Runtime[D]) ConsumeReply(ctx context.Context, envelope ReplyEnvelope) error {
	return r.HandleReply(ctx, internalkafka.ReplyEnvelope{
		ReplyID:    envelope.ReplyID,
		SagaID:     envelope.SagaID,
		Topic:      envelope.Topic,
		ReceivedAt: envelope.ReceivedAt,
		Payload:    envelope.Payload,
	})
}

func (r *Runtime[D]) replyCaseFor(step Step[D], direction model.StepDirection, topic string, replyType string) (ReplyCase[D], bool) {
	var cases []ReplyCase[D]
	if direction == model.StepDirectionCompensation {
		cases = step.CompensateReplies
	} else {
		cases = step.ForwardReplies
	}
	for _, candidate := range cases {
		if candidate.Topic == topic && candidate.ReplyType == replyType {
			return candidate, true
		}
	}
	return ReplyCase[D]{}, false
}

func applyDecisionToPending(sagaRow *model.SagaInstanceRow, pending *model.StepHistoryRow, decision Decision) (string, error) {
	if sagaRow.PendingDirection == model.StepDirectionCompensation {
		if decision.Kind != DecisionCompensated {
			return "", fmt.Errorf("unsupported compensation decision %q", decision.Kind)
		}
		pending.Status = model.StepStatusCompensated
		return "compensated", nil
	}
	switch decision.Kind {
	case DecisionAdvance, DecisionComplete:
		pending.Status = model.StepStatusSucceeded
		sagaRow.LastError = ""
		return "success", nil
	case DecisionBeginCompensation, DecisionCancel:
		pending.Status = model.StepStatusFailed
		pending.Error = decision.Reason
		sagaRow.LastError = decision.Reason
		return "failure", nil
	default:
		return "", fmt.Errorf("unsupported forward decision %q", decision.Kind)
	}
}
