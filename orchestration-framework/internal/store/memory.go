package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"saga-pattern/orchestration-framework/internal/model"
)

type MemoryStore struct {
	mu               sync.Mutex
	sagas            map[string]model.SagaInstanceRow
	stepHistory      map[string][]model.StepHistoryRow
	outbox           map[string]model.OutboxRow
	processedReplies map[string]model.ProcessedReplyRow
	leases           map[string]model.SchedulerLeaseRow
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sagas:            make(map[string]model.SagaInstanceRow),
		stepHistory:      make(map[string][]model.StepHistoryRow),
		outbox:           make(map[string]model.OutboxRow),
		processedReplies: make(map[string]model.ProcessedReplyRow),
		leases:           make(map[string]model.SchedulerLeaseRow),
	}
}

func (s *MemoryStore) WithinTx(_ context.Context, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(memoryTx{store: s})
}

func (s *MemoryStore) AcquireLease(_ context.Context, name string, owner string, now time.Time, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[name]
	if ok && lease.Owner != owner && lease.LeasedUntil.After(now) {
		return false, nil
	}
	lease = model.SchedulerLeaseRow{Name: name, Owner: owner, LeasedUntil: now.Add(ttl), UpdatedAt: now}
	s.leases[name] = lease
	return true, nil
}

func (s *MemoryStore) ClaimOutbox(_ context.Context, owner string, now time.Time, batchSize int, retryDelay time.Duration) ([]model.OutboxRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]model.OutboxRow, 0, len(s.outbox))
	for _, row := range s.outbox {
		if row.Status != model.OutboxStatusPending && row.Status != model.OutboxStatusSending {
			continue
		}
		if row.AvailableAt.After(now) {
			continue
		}
		if row.Status == model.OutboxStatusPending {
			if row.ClaimedUntil != nil && row.ClaimedUntil.After(now) && row.ClaimedBy != owner {
				continue
			}
		} else {
			if row.ClaimedUntil == nil || row.ClaimedUntil.After(now) {
				continue
			}
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].AvailableAt.Equal(rows[j].AvailableAt) {
			return rows[i].CreatedAt.Before(rows[j].CreatedAt)
		}
		return rows[i].AvailableAt.Before(rows[j].AvailableAt)
	})
	if len(rows) > batchSize {
		rows = rows[:batchSize]
	}
	claimed := make([]model.OutboxRow, 0, len(rows))
	for _, row := range rows {
		row.Status = model.OutboxStatusSending
		row.AttemptCount++
		row.LastError = ""
		row.UpdatedAt = now
		row.ClaimedBy = owner
		claimedUntil := now.Add(retryDelay)
		row.ClaimedUntil = &claimedUntil
		s.outbox[row.ID] = row
		claimed = append(claimed, row)
	}
	return claimed, nil
}

func (s *MemoryStore) MarkOutboxSent(_ context.Context, outboxID string, sentAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.outbox[outboxID]
	if !ok {
		return fmt.Errorf("outbox %s not found", outboxID)
	}
	row.Status = model.OutboxStatusSent
	row.UpdatedAt = sentAt
	row.SentAt = &sentAt
	row.ClaimedBy = ""
	row.ClaimedUntil = nil
	s.outbox[outboxID] = row
	return nil
}

func (s *MemoryStore) MarkOutboxFailed(_ context.Context, outboxID string, nextAttemptAt time.Time, errMsg string, terminal bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.outbox[outboxID]
	if !ok {
		return fmt.Errorf("outbox %s not found", outboxID)
	}
	if terminal {
		row.Status = model.OutboxStatusFailed
	} else {
		row.Status = model.OutboxStatusPending
		row.AvailableAt = nextAttemptAt
	}
	row.LastError = errMsg
	row.UpdatedAt = nextAttemptAt
	row.ClaimedBy = ""
	row.ClaimedUntil = nil
	s.outbox[outboxID] = row
	return nil
}

func (s *MemoryStore) ListExpiredSagas(_ context.Context, now time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for _, row := range s.sagas {
		if row.PendingCommandType == "" || row.StepDeadlineAt.IsZero() || row.StepDeadlineAt.After(now) {
			continue
		}
		if row.State == model.SagaStateCompleted || row.State == model.SagaStateCancelled {
			continue
		}
		ids = append(ids, row.ID)
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (s *MemoryStore) CleanupProcessedReplies(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, row := range s.processedReplies {
		if row.ExpiresAt.Before(before) {
			delete(s.processedReplies, key)
			removed++
		}
	}
	return removed, nil
}

func (s *MemoryStore) CleanupOutbox(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, row := range s.outbox {
		if row.Status == model.OutboxStatusPending || row.Status == model.OutboxStatusSending {
			continue
		}
		if row.UpdatedAt.Before(before) {
			delete(s.outbox, key)
			removed++
		}
	}
	return removed, nil
}

func (s *MemoryStore) GetSaga(_ context.Context, sagaID string) (model.SagaInstanceRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.sagas[sagaID]
	return row, ok, nil
}

func (s *MemoryStore) ListStepHistory(_ context.Context, sagaID string) ([]model.StepHistoryRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := append([]model.StepHistoryRow(nil), s.stepHistory[sagaID]...)
	return rows, nil
}

func (s *MemoryStore) ListOutbox(_ context.Context, sagaID string) ([]model.OutboxRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]model.OutboxRow, 0)
	for _, row := range s.outbox {
		if sagaID == "" || row.SagaID == sagaID {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].CreatedAt.Before(rows[j].CreatedAt) })
	return rows, nil
}

type memoryTx struct{ store *MemoryStore }

func (tx memoryTx) InsertSaga(_ context.Context, row model.SagaInstanceRow) error {
	if _, exists := tx.store.sagas[row.ID]; exists {
		return fmt.Errorf("saga %s already exists", row.ID)
	}
	tx.store.sagas[row.ID] = row
	return nil
}

func (tx memoryTx) LockSaga(_ context.Context, sagaID string) (model.SagaInstanceRow, bool, error) {
	row, ok := tx.store.sagas[sagaID]
	return row, ok, nil
}

func (tx memoryTx) UpdateSaga(_ context.Context, row model.SagaInstanceRow) error {
	if _, exists := tx.store.sagas[row.ID]; !exists {
		return fmt.Errorf("saga %s not found", row.ID)
	}
	tx.store.sagas[row.ID] = row
	return nil
}

func (tx memoryTx) InsertStepHistory(_ context.Context, row model.StepHistoryRow) error {
	tx.store.stepHistory[row.SagaID] = append(tx.store.stepHistory[row.SagaID], row)
	return nil
}

func (tx memoryTx) UpdateStepHistory(_ context.Context, row model.StepHistoryRow) error {
	rows := tx.store.stepHistory[row.SagaID]
	for idx := range rows {
		if rows[idx].ID == row.ID {
			rows[idx] = row
			tx.store.stepHistory[row.SagaID] = rows
			return nil
		}
	}
	return fmt.Errorf("step history %s not found", row.ID)
}

func (tx memoryTx) ListStepHistory(_ context.Context, sagaID string) ([]model.StepHistoryRow, error) {
	return append([]model.StepHistoryRow(nil), tx.store.stepHistory[sagaID]...), nil
}

func (tx memoryTx) InsertOutbox(_ context.Context, row model.OutboxRow) error {
	if _, exists := tx.store.outbox[row.ID]; exists {
		return fmt.Errorf("outbox %s already exists", row.ID)
	}
	tx.store.outbox[row.ID] = row
	return nil
}

func (tx memoryTx) InsertProcessedReply(_ context.Context, row model.ProcessedReplyRow) (bool, error) {
	if _, exists := tx.store.processedReplies[row.ReplyID]; exists {
		return false, nil
	}
	tx.store.processedReplies[row.ReplyID] = row
	return true, nil
}
