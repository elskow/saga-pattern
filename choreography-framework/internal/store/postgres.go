package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"saga-pattern/choreography-framework/internal/model"
)

// Uses FOR UPDATE SKIP LOCKED, lease-based claiming, terminal failure status, and backoff.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) WithinTx(ctx context.Context, fn func(Tx) error) error {
	if s.db == nil {
		return fmt.Errorf("postgres db is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	wrapped := postgresTx{tx: tx}
	if err := fn(wrapped); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) AcquireLease(ctx context.Context, name string, owner string, now time.Time, ttl time.Duration) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("postgres db is required")
	}
	query := `
	INSERT INTO scheduler_leases (name, owner, leased_until, updated_at)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (name) DO UPDATE
	SET owner = EXCLUDED.owner,
	    leased_until = EXCLUDED.leased_until,
	    updated_at = EXCLUDED.updated_at
	WHERE scheduler_leases.leased_until <= $4 OR scheduler_leases.owner = $2`
	result, err := s.db.ExecContext(ctx, query, name, owner, now.Add(ttl), now)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *PostgresStore) ClaimOutbox(ctx context.Context, owner string, now time.Time, batchSize int, retryDelay time.Duration) ([]model.OutboxRow, error) {
	if s.db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
	SELECT id, topic, message_key, event_type, payload_json, status,
	       available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at,
	       trace_headers_json
	FROM outbox_messages
	WHERE (
	      (status = 'pending' AND (claimed_until IS NULL OR claimed_until <= $1 OR claimed_by = $2))
	   OR (status = 'sending' AND claimed_until IS NOT NULL AND claimed_until <= $1)
	)
	  AND available_at <= $1
	ORDER BY available_at, created_at
	FOR UPDATE SKIP LOCKED
	LIMIT $3`, now, owner, batchSize)
	if err != nil {
		return nil, err
	}
	claimed := make([]model.OutboxRow, 0)
	for rows.Next() {
		row, err := scanOutbox(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		claimed = append(claimed, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range claimed {
		row := claimed[i]
		row.Status = model.OutboxStatusSending
		row.AttemptCount++
		row.UpdatedAt = now
		claimedUntil := now.Add(retryDelay)
		row.ClaimedBy = owner
		row.ClaimedUntil = &claimedUntil
		if _, err := tx.ExecContext(ctx, `
		UPDATE outbox_messages
		SET status = $2, attempt_count = $3, updated_at = $4, claimed_by = $5, claimed_until = $6
		WHERE id = $1`, row.ID, row.Status, row.AttemptCount, row.UpdatedAt, row.ClaimedBy, claimedUntil); err != nil {
			return nil, err
		}
		claimed[i] = row
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *PostgresStore) MarkOutboxSent(ctx context.Context, outboxID string, sentAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
	UPDATE outbox_messages
	SET status = 'sent', sent_at = $2, updated_at = $2, claimed_by = NULL, claimed_until = NULL
	WHERE id = $1`, outboxID, sentAt)
	return err
}

func (s *PostgresStore) MarkOutboxFailed(ctx context.Context, outboxID string, nextAttemptAt time.Time, errMsg string, terminal bool) error {
	status := model.OutboxStatusPending
	if terminal {
		status = model.OutboxStatusFailed
	}
	_, err := s.db.ExecContext(ctx, `
	UPDATE outbox_messages
	SET status = $2, available_at = $3, last_error = $4, updated_at = $3, claimed_by = NULL, claimed_until = NULL
	WHERE id = $1`, outboxID, status, nextAttemptAt, errMsg)
	return err
}

func (s *PostgresStore) CleanupProcessedEvents(ctx context.Context, before time.Time) (int, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM processed_events WHERE processed_at < $1`, before)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (s *PostgresStore) CleanupOutbox(ctx context.Context, before time.Time) (int, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM outbox_messages WHERE status IN ('sent', 'failed') AND updated_at < $1`, before)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (s *PostgresStore) ListOutbox(ctx context.Context) ([]model.OutboxRow, error) {
	rows, err := s.db.QueryContext(ctx, `
	SELECT id, topic, message_key, event_type, payload_json, status,
	       available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at,
	       trace_headers_json
	FROM outbox_messages
	ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.OutboxRow, 0)
	for rows.Next() {
		item, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// Required for the transactional outbox pattern to be atomic.
func WrapSQLTx(tx *sql.Tx) Tx {
	return postgresTx{tx: tx}
}

type postgresTx struct{ tx *sql.Tx }

func (tx postgresTx) InsertOutbox(ctx context.Context, row model.OutboxRow) error {
	_, err := tx.tx.ExecContext(ctx, `
	INSERT INTO outbox_messages (id, topic, message_key, event_type, payload_json,
	status, available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at, claimed_by, claimed_until,
	trace_headers_json)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		row.ID, row.Topic, row.Key, row.EventType, string(row.Payload),
		row.Status, row.AvailableAt, row.CreatedAt, row.UpdatedAt, row.AttemptCount, row.MaxAttempts, row.LastError, row.SentAt, row.ClaimedBy, row.ClaimedUntil,
		encodeTraceHeaders(row.TraceHeaders))
	return err
}

func (tx postgresTx) TryMarkProcessedEvent(ctx context.Context, key string) (bool, error) {
	result, err := tx.tx.ExecContext(ctx, `
	INSERT INTO processed_events (event_key, processed_at)
	VALUES ($1, NOW())
	ON CONFLICT (event_key) DO NOTHING`, key)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (tx postgresTx) DeleteProcessedEvent(ctx context.Context, key string) error {
	_, err := tx.tx.ExecContext(ctx, `DELETE FROM processed_events WHERE event_key = $1`, key)
	return err
}

type outboxScanner interface{ Scan(dest ...any) error }

func scanOutbox(scanner outboxScanner) (model.OutboxRow, error) {
	var row model.OutboxRow
	var payload string
	var traceHeadersJSON string
	err := scanner.Scan(&row.ID, &row.Topic, &row.Key, &row.EventType,
		&payload, &row.Status, &row.AvailableAt, &row.CreatedAt, &row.UpdatedAt, &row.AttemptCount, &row.MaxAttempts, &row.LastError, &row.SentAt,
		&traceHeadersJSON)
	if err != nil {
		return model.OutboxRow{}, err
	}
	row.Payload = []byte(payload)
	row.TraceHeaders = decodeTraceHeaders(traceHeadersJSON)
	return row, nil
}

func encodeTraceHeaders(headers map[string]string) string {
	if len(headers) == 0 {
		return "{}"
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func decodeTraceHeaders(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	var headers map[string]string
	if err := json.Unmarshal([]byte(raw), &headers); err != nil || len(headers) == 0 {
		return nil
	}
	return headers
}
