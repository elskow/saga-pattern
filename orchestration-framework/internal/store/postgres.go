package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"saga-pattern/orchestration-framework/internal/model"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) InitSchema(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("postgres db is required")
	}
	_, err := s.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS scheduler_leases (
	    name VARCHAR(255) PRIMARY KEY,
	    owner VARCHAR(255) NOT NULL,
	    leased_until TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE IF NOT EXISTS saga_instances (
	    id VARCHAR(255) PRIMARY KEY,
	    saga_type VARCHAR(255) NOT NULL,
	    state VARCHAR(255) NOT NULL,
	    current_step VARCHAR(255) NOT NULL,
	    pending_direction VARCHAR(32) NOT NULL,
	    pending_command_type VARCHAR(255) NOT NULL,
	    pending_reply_type VARCHAR(255) NOT NULL,
	    started_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL,
	    deadline_at TIMESTAMP NOT NULL,
	    step_deadline_at TIMESTAMP NOT NULL,
	    retry_count INT NOT NULL,
	    max_retry_count INT NOT NULL,
	    last_error TEXT NOT NULL DEFAULT '',
	    data_json TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_saga_instances_type_state ON saga_instances(saga_type, state);
	CREATE INDEX IF NOT EXISTS idx_saga_instances_step_deadline ON saga_instances(step_deadline_at);
	CREATE TABLE IF NOT EXISTS saga_step_history (
	    id VARCHAR(255) PRIMARY KEY,
	    saga_id VARCHAR(255) NOT NULL,
	    step VARCHAR(255) NOT NULL,
	    direction VARCHAR(32) NOT NULL,
	    status VARCHAR(32) NOT NULL,
	    command_type VARCHAR(255) NOT NULL,
	    reply_type VARCHAR(255) NOT NULL,
	    attempt INT NOT NULL,
	    created_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL,
	    completed_at TIMESTAMP,
	    error_message TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_saga_step_history_saga ON saga_step_history(saga_id, created_at, attempt);
	CREATE TABLE IF NOT EXISTS outbox_messages (
	    id VARCHAR(255) PRIMARY KEY,
	    saga_id VARCHAR(255) NOT NULL,
	    saga_type VARCHAR(255) NOT NULL,
	    step VARCHAR(255) NOT NULL,
	    direction VARCHAR(32) NOT NULL,
	    topic VARCHAR(255) NOT NULL,
	    message_key VARCHAR(255) NOT NULL,
	    message_type VARCHAR(255) NOT NULL,
	    payload_json TEXT NOT NULL,
	    status VARCHAR(32) NOT NULL,
	    available_at TIMESTAMP NOT NULL,
	    created_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL,
	    attempt_count INT NOT NULL,
	    max_attempts INT NOT NULL,
	    last_error TEXT NOT NULL DEFAULT '',
	    sent_at TIMESTAMP,
	    claimed_by VARCHAR(255),
	    claimed_until TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_outbox_messages_pending ON outbox_messages(status, available_at, created_at);
	CREATE INDEX IF NOT EXISTS idx_outbox_messages_saga ON outbox_messages(saga_id, created_at);
	CREATE TABLE IF NOT EXISTS processed_replies (
	    reply_id VARCHAR(255) PRIMARY KEY,
	    saga_id VARCHAR(255) NOT NULL,
	    reply_type VARCHAR(255) NOT NULL,
	    recorded_at TIMESTAMP NOT NULL,
	    expires_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_processed_replies_expiry ON processed_replies(expires_at);`)
	if err != nil {
		return fmt.Errorf("init runtime schema: %w", err)
	}
	return nil
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
	SELECT id, saga_id, saga_type, step, direction, topic, message_key, message_type, payload_json, status,
	       available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at
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

func (s *PostgresStore) ListExpiredSagas(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
	SELECT id
	FROM saga_instances
	WHERE pending_command_type <> ''
	  AND step_deadline_at <= $1
	  AND state NOT IN ('COMPLETED', 'CANCELLED')
	ORDER BY step_deadline_at
	LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PostgresStore) CleanupProcessedReplies(ctx context.Context, before time.Time) (int, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM processed_replies WHERE expires_at < $1`, before)
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

func (s *PostgresStore) GetSaga(ctx context.Context, sagaID string) (model.SagaInstanceRow, bool, error) {
	row := s.db.QueryRowContext(ctx, `
	SELECT id, saga_type, state, current_step, pending_direction, pending_command_type, pending_reply_type,
	       started_at, updated_at, deadline_at, step_deadline_at, retry_count, max_retry_count, last_error, data_json
	FROM saga_instances WHERE id = $1`, sagaID)
	return scanSagaRow(row)
}

func (s *PostgresStore) ListStepHistory(ctx context.Context, sagaID string) ([]model.StepHistoryRow, error) {
	rows, err := s.db.QueryContext(ctx, `
	SELECT id, saga_id, step, direction, status, command_type, reply_type, attempt, created_at, updated_at, completed_at, error_message
	FROM saga_step_history WHERE saga_id = $1 ORDER BY created_at, attempt`, sagaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.StepHistoryRow, 0)
	for rows.Next() {
		item, err := scanStepHistory(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *PostgresStore) ListOutbox(ctx context.Context, sagaID string) ([]model.OutboxRow, error) {
	rows, err := s.db.QueryContext(ctx, `
	SELECT id, saga_id, saga_type, step, direction, topic, message_key, message_type, payload_json, status,
	       available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at
	FROM outbox_messages WHERE ($1 = '' OR saga_id = $1)
	ORDER BY created_at`, sagaID)
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

type postgresTx struct{ tx *sql.Tx }

func (tx postgresTx) InsertSaga(ctx context.Context, row model.SagaInstanceRow) error {
	data, err := json.Marshal(row.Data)
	if err != nil {
		return err
	}
	_, err = tx.tx.ExecContext(ctx, `
	INSERT INTO saga_instances (id, saga_type, state, current_step, pending_direction, pending_command_type, pending_reply_type,
	started_at, updated_at, deadline_at, step_deadline_at, retry_count, max_retry_count, last_error, data_json)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		row.ID, row.SagaType, row.State, row.CurrentStep, row.PendingDirection, row.PendingCommandType, row.PendingReplyType,
		row.StartedAt, row.UpdatedAt, row.DeadlineAt, row.StepDeadlineAt, row.RetryCount, row.MaxRetryCount, row.LastError, data)
	return err
}

func (tx postgresTx) LockSaga(ctx context.Context, sagaID string) (model.SagaInstanceRow, bool, error) {
	row := tx.tx.QueryRowContext(ctx, `
	SELECT id, saga_type, state, current_step, pending_direction, pending_command_type, pending_reply_type,
	       started_at, updated_at, deadline_at, step_deadline_at, retry_count, max_retry_count, last_error, data_json
	FROM saga_instances WHERE id = $1 FOR UPDATE`, sagaID)
	return scanSagaRow(row)
}

func (tx postgresTx) UpdateSaga(ctx context.Context, row model.SagaInstanceRow) error {
	data, err := json.Marshal(row.Data)
	if err != nil {
		return err
	}
	_, err = tx.tx.ExecContext(ctx, `
	UPDATE saga_instances
	SET state=$2, current_step=$3, pending_direction=$4, pending_command_type=$5, pending_reply_type=$6,
	    updated_at=$7, deadline_at=$8, step_deadline_at=$9, retry_count=$10, max_retry_count=$11, last_error=$12, data_json=$13
	WHERE id=$1`, row.ID, row.State, row.CurrentStep, row.PendingDirection, row.PendingCommandType, row.PendingReplyType,
		row.UpdatedAt, row.DeadlineAt, row.StepDeadlineAt, row.RetryCount, row.MaxRetryCount, row.LastError, data)
	return err
}

func (tx postgresTx) InsertStepHistory(ctx context.Context, row model.StepHistoryRow) error {
	_, err := tx.tx.ExecContext(ctx, `
	INSERT INTO saga_step_history (id, saga_id, step, direction, status, command_type, reply_type, attempt, created_at, updated_at, completed_at, error_message)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		row.ID, row.SagaID, row.Step, row.Direction, row.Status, row.CommandType, row.ReplyType, row.Attempt,
		row.CreatedAt, row.UpdatedAt, row.CompletedAt, row.Error)
	return err
}

func (tx postgresTx) UpdateStepHistory(ctx context.Context, row model.StepHistoryRow) error {
	_, err := tx.tx.ExecContext(ctx, `
	UPDATE saga_step_history
	SET status=$2, updated_at=$3, completed_at=$4, error_message=$5, reply_type=$6
	WHERE id=$1`, row.ID, row.Status, row.UpdatedAt, row.CompletedAt, row.Error, row.ReplyType)
	return err
}

func (tx postgresTx) ListStepHistory(ctx context.Context, sagaID string) ([]model.StepHistoryRow, error) {
	rows, err := tx.tx.QueryContext(ctx, `
	SELECT id, saga_id, step, direction, status, command_type, reply_type, attempt, created_at, updated_at, completed_at, error_message
	FROM saga_step_history WHERE saga_id=$1 ORDER BY created_at, attempt`, sagaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.StepHistoryRow, 0)
	for rows.Next() {
		item, err := scanStepHistory(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (tx postgresTx) InsertOutbox(ctx context.Context, row model.OutboxRow) error {
	_, err := tx.tx.ExecContext(ctx, `
	INSERT INTO outbox_messages (id, saga_id, saga_type, step, direction, topic, message_key, message_type, payload_json,
	status, available_at, created_at, updated_at, attempt_count, max_attempts, last_error, sent_at, claimed_by, claimed_until)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
		row.ID, row.SagaID, row.SagaType, row.Step, row.Direction, row.Topic, row.Key, row.MessageType, string(row.Payload),
		row.Status, row.AvailableAt, row.CreatedAt, row.UpdatedAt, row.AttemptCount, row.MaxAttempts, row.LastError, row.SentAt, row.ClaimedBy, row.ClaimedUntil)
	return err
}

func (tx postgresTx) InsertProcessedReply(ctx context.Context, row model.ProcessedReplyRow) (bool, error) {
	result, err := tx.tx.ExecContext(ctx, `
	INSERT INTO processed_replies (reply_id, saga_id, reply_type, recorded_at, expires_at)
	VALUES ($1,$2,$3,$4,$5)
	ON CONFLICT (reply_id) DO NOTHING`, row.ReplyID, row.SagaID, row.ReplyType, row.RecordedAt, row.ExpiresAt)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

type sagaScanner interface{ Scan(dest ...any) error }

func scanSagaRow(scanner sagaScanner) (model.SagaInstanceRow, bool, error) {
	var row model.SagaInstanceRow
	var raw []byte
	err := scanner.Scan(&row.ID, &row.SagaType, &row.State, &row.CurrentStep, &row.PendingDirection, &row.PendingCommandType,
		&row.PendingReplyType, &row.StartedAt, &row.UpdatedAt, &row.DeadlineAt, &row.StepDeadlineAt, &row.RetryCount,
		&row.MaxRetryCount, &row.LastError, &raw)
	if err == sql.ErrNoRows {
		return model.SagaInstanceRow{}, false, nil
	}
	if err != nil {
		return model.SagaInstanceRow{}, false, err
	}
	if err := json.Unmarshal(raw, &row.Data); err != nil {
		return model.SagaInstanceRow{}, false, err
	}
	return row, true, nil
}

type stepScanner interface{ Scan(dest ...any) error }

func scanStepHistory(scanner stepScanner) (model.StepHistoryRow, error) {
	var row model.StepHistoryRow
	err := scanner.Scan(&row.ID, &row.SagaID, &row.Step, &row.Direction, &row.Status, &row.CommandType, &row.ReplyType,
		&row.Attempt, &row.CreatedAt, &row.UpdatedAt, &row.CompletedAt, &row.Error)
	return row, err
}

type outboxScanner interface{ Scan(dest ...any) error }

func scanOutbox(scanner outboxScanner) (model.OutboxRow, error) {
	var row model.OutboxRow
	var payload string
	err := scanner.Scan(&row.ID, &row.SagaID, &row.SagaType, &row.Step, &row.Direction, &row.Topic, &row.Key, &row.MessageType,
		&payload, &row.Status, &row.AvailableAt, &row.CreatedAt, &row.UpdatedAt, &row.AttemptCount, &row.MaxAttempts, &row.LastError, &row.SentAt)
	if err != nil {
		return model.OutboxRow{}, err
	}
	row.Payload = []byte(payload)
	return row, nil
}
