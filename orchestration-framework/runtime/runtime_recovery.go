package runtime

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

func (r *Runtime[D]) RecoverExpiredSaga(ctx context.Context, sagaID string, now time.Time) (err error) {
	ctx, span := r.startSpan(ctx, "orchestration.runtime.recover_expired_saga",
		attribute.String("saga.id", sagaID),
		attribute.String("recovery.at", now.UTC().Format(time.RFC3339Nano)),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	commandEnqueued := false
	err = r.store.WithinTx(ctx, func(tx store.Tx) error {
		sagaRow, ok, err := tx.LockSaga(ctx, sagaID)
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
			attribute.Int("saga.max_retry_count", sagaRow.MaxRetryCount),
		)
		if sagaRow.PendingCommandType == "" || sagaRow.StepDeadlineAt.After(now) || r.isTerminal(sagaRow.State) {
			span.AddEvent("recovery_skipped",
				trace.WithAttributes(attribute.String("reason", "not_expired_or_terminal")),
			)
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
			span.AddEvent("step_timed_out",
				trace.WithAttributes(
					attribute.String("saga.step", pending.Step),
					attribute.String("saga.pending.direction", string(pending.Direction)),
					attribute.String("timeout.reason", pending.Error),
				),
			)
			r.metrics.RecordStepDuration(sagaRow.SagaType, pending.Step, string(pending.Direction), "timeout", now.Sub(pending.CreatedAt))
		}
		if sagaRow.PendingDirection == model.StepDirectionForward && sagaRow.RetryCount < sagaRow.MaxRetryCount {
			step, err := r.stepByName(sagaRow.CurrentStep)
			if err != nil {
				return err
			}
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
			span.AddEvent("timeout_retry_scheduled",
				trace.WithAttributes(
					attribute.String("saga.step", step.Name),
					attribute.String("saga.command.type", step.Forward.CommandType),
					attribute.Int("saga.retry_count", sagaRow.RetryCount),
				),
			)
			if err := r.enqueueCommandTx(ctx, tx, sagaRow, step, model.StepDirectionForward, now); err != nil {
				return err
			}
			commandEnqueued = true
			return nil
		}
		if sagaRow.PendingDirection == model.StepDirectionForward {
			step, err := r.stepByName(sagaRow.CurrentStep)
			if err != nil {
				return err
			}
			span.AddEvent("timeout_begin_compensation",
				trace.WithAttributes(
					attribute.String("saga.step", step.Name),
					attribute.String("timeout.reason", "timeout exhausted"),
				),
			)
			var beginErr error
			commandEnqueued, beginErr = r.beginCompensationTx(ctx, tx, sagaRow, step, now, history, "timeout exhausted")
			return beginErr
		}
		return nil
	})
	if err == nil && commandEnqueued {
		r.triggerOutboxPublish(ctx, span)
	}
	return
}
