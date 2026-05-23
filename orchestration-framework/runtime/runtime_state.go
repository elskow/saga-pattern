package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"saga-pattern/orchestration-framework/internal/model"
)

func (r *Runtime[D]) Snapshot(ctx context.Context, sagaID string) (Snapshot[D], bool, error) {
	row, ok, err := r.store.GetSaga(ctx, sagaID)
	if err != nil || !ok {
		return Snapshot[D]{}, ok, err
	}
	return r.toPublicSnapshot(row)
}

func (r *Runtime[D]) View(ctx context.Context, sagaID string) (View[D], bool, error) {
	row, ok, err := r.store.GetSaga(ctx, sagaID)
	if err != nil || !ok {
		return View[D]{}, ok, err
	}
	return r.toPublicView(ctx, row)
}

func (r *Runtime[D]) stepByName(name string) (Step[D], error) {
	for _, step := range r.definition.Steps {
		if step.Name == name {
			return step, nil
		}
	}
	return Step[D]{}, fmt.Errorf("runtime definition missing step %q", name)
}

func (r *Runtime[D]) isTerminal(state model.SagaState) bool {
	for _, terminal := range r.definition.TerminalStates {
		if terminal == string(state) {
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

func (r *Runtime[D]) toPublicSnapshot(row model.SagaInstanceRow) (Snapshot[D], bool, error) {
	data, err := r.definition.codec().Unmarshal(row.Data)
	if err != nil {
		return Snapshot[D]{}, false, err
	}
	return Snapshot[D]{
		SagaID:             row.ID,
		SagaType:           row.SagaType,
		State:              string(row.State),
		CurrentStep:        row.CurrentStep,
		PendingDirection:   string(row.PendingDirection),
		PendingCommandType: row.PendingCommandType,
		PendingReplyType:   row.PendingReplyType,
		LastError:          row.LastError,
		StartedAt:          row.StartedAt,
		UpdatedAt:          row.UpdatedAt,
		DeadlineAt:         row.DeadlineAt,
		StepDeadlineAt:     row.StepDeadlineAt,
		RetryCount:         row.RetryCount,
		MaxRetryCount:      row.MaxRetryCount,
		Data:               data,
	}, true, nil
}

func (r *Runtime[D]) toPublicView(ctx context.Context, row model.SagaInstanceRow) (View[D], bool, error) {
	data, err := r.definition.codec().Unmarshal(row.Data)
	if err != nil {
		return View[D]{}, false, err
	}
	history, err := r.store.ListStepHistory(ctx, row.ID)
	if err != nil {
		return View[D]{}, false, err
	}
	return View[D]{
		SagaID:      row.ID,
		SagaType:    row.SagaType,
		State:       string(row.State),
		CurrentStep: row.CurrentStep,
		LastError:   row.LastError,
		StartedAt:   row.StartedAt,
		UpdatedAt:   row.UpdatedAt,
		Data:        data,
		StepHistory: publicStepHistory(history),
	}, true, nil
}

func publicStepHistory(rows []model.StepHistoryRow) []StepHistoryEntry {
	entries := make([]StepHistoryEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, StepHistoryEntry{
			Step:      row.Step,
			Direction: string(row.Direction),
			Status:    string(row.Status),
			Error:     row.Error,
		})
	}
	return entries
}

func pendingReplyTypes[D any](cases []ReplyCase[D]) string {
	types := make([]string, 0, len(cases))
	for _, reply := range cases {
		types = append(types, reply.ReplyType)
	}
	return strings.Join(types, ",")
}

func clearPendingState(row *model.SagaInstanceRow) {
	row.PendingDirection = ""
	row.PendingCommandType = ""
	row.PendingReplyType = ""
	row.StepDeadlineAt = time.Time{}
}

func setPendingStep[D any](row *model.SagaInstanceRow, step Step[D], direction model.StepDirection, replyTypes string, now time.Time, retryDelay time.Duration) {
	row.CurrentStep = step.Name
	row.PendingDirection = direction
	if direction == model.StepDirectionCompensation {
		row.PendingCommandType = step.Compensation.CommandType
	} else {
		row.PendingCommandType = step.Forward.CommandType
	}
	row.PendingReplyType = replyTypes
	row.StepDeadlineAt = now.Add(retryDelay)
	row.UpdatedAt = now
}

func cancelSaga(row *model.SagaInstanceRow, now time.Time, reason string) {
	row.State = model.SagaStateCancelled
	row.LastError = reason
	clearPendingState(row)
	row.UpdatedAt = now
}
